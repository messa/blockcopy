#!/usr/bin/env python3

r'''
Copy large files (or block devices) efficiently over network.

The idea is to split the file into blocks, calculate hash of each block and send
the hashes to the source side. The source side will compare the hashes with the
hashes of the blocks it has and sends only the blocks that are different to the
destination side.

Communication over network is not implemented here. This script uses stdin/stdout
for communication. You have to run this scripts multiple times and connect their
stdout/stdin via pipe. You can use ssh or netcat to send data over network.

Usage - run on the destination side:

    blockcopy.py checksum /dev/destination | \
    ssh srchost blockcopy.py retrieve /dev/source | \
    blockcopy.py save /dev/destination

Or run on the source side:

    ssh dsthost blockcopy.py checksum /dev/destination | \
    blockcopy.py retrieve /dev/source | \
    ssh dsthost blockcopy.py save /dev/destination

You can plug in compression:

    ssh dsthost blockcopy.py checksum /dev/destination | \
    blockcopy.py retrieve /dev/source | pzstd | \
    ssh dsthost 'zstdcat | blockcopy.py save /dev/destination'

See also readme: https://github.com/messa/blockcopy
'''

from argparse import ArgumentParser
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from grp import getgrgid, getgrnam
from hashlib import sha3_512
from logging import getLogger
from os import chmod, chown, cpu_count, environ, getpid, kill, readlink, SEEK_END, symlink, utime, walk
from os import remove as os_remove
from pathlib import Path
from pwd import getpwuid, getpwnam
from queue import Queue
from shutil import rmtree
from stat import S_ISREG
from sys import exit, stderr, stdin, stdout
from threading import Event, Lock
from time import monotonic


__version__ = '0.0.3'

logger = getLogger(__name__)

block_size = 128 * 1024
hash_factory = sha3_512
hash_digest_size = hash_factory().digest_size
assert hash_digest_size == 512 / 8
worker_count = min(cpu_count(), 8)


class IncompleteReadError(Exception):
    '''
    Exception raised when reading from a stream is incomplete.
    '''
    pass


class CollectedExceptions(Exception):
    '''
    Exception that contains multiple exceptions.
    '''

    def __init__(self, exceptions):
        self.exceptions = exceptions
        super().__init__('Collected exceptions: ' + ', '.join(repr(e) for e in exceptions))

    def __repr__(self):
        return f'{self.__class__.__name__}({self.exceptions!r})'


class ExceptionCollector:
    '''
    Collects exceptions from worker threads and allows the main thread to check
    if any exception occurred and re-raise it.
    '''
    def __init__(self):
        self._lock = Lock()
        self._exceptions = []

    def collect_exception(self, exc):
        '''Store exception from worker thread'''
        with self._lock:
            self._exceptions.append(exc)

    def check_and_raise(self):
        '''Check if any exception was collected and re-raise it'''
        with self._lock:
            if self._exceptions:
                raise CollectedExceptions(self._exceptions)

    def has_exception(self):
        '''Check if any exception was collected'''
        with self._lock:
            return bool(self._exceptions)


def get_user_name_by_uid(uid):
    '''Get username by UID, returns empty string if not found.'''
    try:
        return getpwuid(uid).pw_name
    except KeyError:
        return ''


def get_group_name_by_gid(gid):
    '''Get group name by GID, returns empty string if not found.'''
    try:
        return getgrgid(gid).gr_name
    except KeyError:
        return ''


# Entry types for recursive mode
ENTRY_FILE = 0
ENTRY_DIR = 1
ENTRY_SYMLINK = 2


@dataclass
class FileEntry:
    '''Represents a file in directory listing.'''
    path: str  # relative path
    size: int
    mtime_ns: int
    mode: int
    uid: int
    gid: int
    owner_name: str
    group_name: str


@dataclass
class DirEntry:
    '''Represents a directory in directory listing.'''
    path: str  # relative path
    mtime_ns: int
    mode: int
    uid: int
    gid: int
    owner_name: str
    group_name: str


@dataclass
class SymlinkEntry:
    '''Represents a symlink in directory listing.'''
    path: str  # relative path
    target: str
    mtime_ns: int
    uid: int
    gid: int


def scan_directory(base_path):
    '''
    Scan directory recursively and return list of entries.
    Returns tuple of (files, dirs, symlinks).
    '''
    base_path = Path(base_path).resolve()
    files = []
    dirs = []
    symlinks = []

    for root, dirnames, filenames in walk(base_path, followlinks=False):
        root_path = Path(root)
        rel_root = root_path.relative_to(base_path)

        # Process directories
        for dirname in dirnames:
            dir_path = root_path / dirname
            rel_path = str(rel_root / dirname) if str(rel_root) != '.' else dirname

            if dir_path.is_symlink():
                # It's a symlink to a directory
                try:
                    stat_result = dir_path.lstat()
                    target = readlink(dir_path)
                    symlinks.append(SymlinkEntry(
                        path=rel_path,
                        target=target,
                        mtime_ns=stat_result.st_mtime_ns,
                        uid=stat_result.st_uid,
                        gid=stat_result.st_gid,
                    ))
                except OSError as e:
                    logger.warning('Failed to read symlink %s: %s', dir_path, e)
            else:
                try:
                    stat_result = dir_path.stat()
                    dirs.append(DirEntry(
                        path=rel_path,
                        mtime_ns=stat_result.st_mtime_ns,
                        mode=stat_result.st_mode,
                        uid=stat_result.st_uid,
                        gid=stat_result.st_gid,
                        owner_name=get_user_name_by_uid(stat_result.st_uid),
                        group_name=get_group_name_by_gid(stat_result.st_gid),
                    ))
                except OSError as e:
                    logger.warning('Failed to stat directory %s: %s', dir_path, e)

        # Process files
        for filename in filenames:
            file_path = root_path / filename
            rel_path = str(rel_root / filename) if str(rel_root) != '.' else filename

            if file_path.is_symlink():
                try:
                    stat_result = file_path.lstat()
                    target = readlink(file_path)
                    symlinks.append(SymlinkEntry(
                        path=rel_path,
                        target=target,
                        mtime_ns=stat_result.st_mtime_ns,
                        uid=stat_result.st_uid,
                        gid=stat_result.st_gid,
                    ))
                except OSError as e:
                    logger.warning('Failed to read symlink %s: %s', file_path, e)
            else:
                try:
                    stat_result = file_path.stat()
                    if S_ISREG(stat_result.st_mode):
                        files.append(FileEntry(
                            path=rel_path,
                            size=stat_result.st_size,
                            mtime_ns=stat_result.st_mtime_ns,
                            mode=stat_result.st_mode,
                            uid=stat_result.st_uid,
                            gid=stat_result.st_gid,
                            owner_name=get_user_name_by_uid(stat_result.st_uid),
                            group_name=get_group_name_by_gid(stat_result.st_gid),
                        ))
                    else:
                        logger.warning('Skipping non-regular file: %s', file_path)
                except OSError as e:
                    logger.warning('Failed to stat file %s: %s', file_path, e)

    return files, dirs, symlinks


def write_file_entry(stream, entry):
    '''Write FileEntry to binary stream.'''
    path_bytes = entry.path.encode('utf-8')
    owner_bytes = entry.owner_name.encode('utf-8')
    group_bytes = entry.group_name.encode('utf-8')
    stream.write(b'file')
    stream.write(len(path_bytes).to_bytes(2, 'big'))
    stream.write(path_bytes)
    stream.write(entry.size.to_bytes(8, 'big'))
    stream.write(entry.mtime_ns.to_bytes(8, 'big', signed=True))
    stream.write(entry.mode.to_bytes(4, 'big'))
    stream.write(entry.uid.to_bytes(4, 'big'))
    stream.write(entry.gid.to_bytes(4, 'big'))
    stream.write(len(owner_bytes).to_bytes(2, 'big'))
    stream.write(owner_bytes)
    stream.write(len(group_bytes).to_bytes(2, 'big'))
    stream.write(group_bytes)


def write_dir_entry(stream, entry):
    '''Write DirEntry to binary stream.'''
    path_bytes = entry.path.encode('utf-8')
    owner_bytes = entry.owner_name.encode('utf-8')
    group_bytes = entry.group_name.encode('utf-8')
    stream.write(b'dirs')
    stream.write(len(path_bytes).to_bytes(2, 'big'))
    stream.write(path_bytes)
    stream.write(entry.mtime_ns.to_bytes(8, 'big', signed=True))
    stream.write(entry.mode.to_bytes(4, 'big'))
    stream.write(entry.uid.to_bytes(4, 'big'))
    stream.write(entry.gid.to_bytes(4, 'big'))
    stream.write(len(owner_bytes).to_bytes(2, 'big'))
    stream.write(owner_bytes)
    stream.write(len(group_bytes).to_bytes(2, 'big'))
    stream.write(group_bytes)


def write_symlink_entry(stream, entry):
    '''Write SymlinkEntry to binary stream.'''
    path_bytes = entry.path.encode('utf-8')
    target_bytes = entry.target.encode('utf-8')
    stream.write(b'link')
    stream.write(len(path_bytes).to_bytes(2, 'big'))
    stream.write(path_bytes)
    stream.write(len(target_bytes).to_bytes(2, 'big'))
    stream.write(target_bytes)
    stream.write(entry.mtime_ns.to_bytes(8, 'big', signed=True))
    stream.write(entry.uid.to_bytes(4, 'big'))
    stream.write(entry.gid.to_bytes(4, 'big'))


def read_file_entry(stream):
    '''Read FileEntry from binary stream.'''
    path_len = int.from_bytes(stream.read(2), 'big')
    path = stream.read(path_len).decode('utf-8')
    size = int.from_bytes(stream.read(8), 'big')
    mtime_ns = int.from_bytes(stream.read(8), 'big', signed=True)
    mode = int.from_bytes(stream.read(4), 'big')
    uid = int.from_bytes(stream.read(4), 'big')
    gid = int.from_bytes(stream.read(4), 'big')
    owner_len = int.from_bytes(stream.read(2), 'big')
    owner_name = stream.read(owner_len).decode('utf-8')
    group_len = int.from_bytes(stream.read(2), 'big')
    group_name = stream.read(group_len).decode('utf-8')
    return FileEntry(path, size, mtime_ns, mode, uid, gid, owner_name, group_name)


def read_dir_entry(stream):
    '''Read DirEntry from binary stream.'''
    path_len = int.from_bytes(stream.read(2), 'big')
    path = stream.read(path_len).decode('utf-8')
    mtime_ns = int.from_bytes(stream.read(8), 'big', signed=True)
    mode = int.from_bytes(stream.read(4), 'big')
    uid = int.from_bytes(stream.read(4), 'big')
    gid = int.from_bytes(stream.read(4), 'big')
    owner_len = int.from_bytes(stream.read(2), 'big')
    owner_name = stream.read(owner_len).decode('utf-8')
    group_len = int.from_bytes(stream.read(2), 'big')
    group_name = stream.read(group_len).decode('utf-8')
    return DirEntry(path, mtime_ns, mode, uid, gid, owner_name, group_name)


def read_symlink_entry(stream):
    '''Read SymlinkEntry from binary stream.'''
    path_len = int.from_bytes(stream.read(2), 'big')
    path = stream.read(path_len).decode('utf-8')
    target_len = int.from_bytes(stream.read(2), 'big')
    target = stream.read(target_len).decode('utf-8')
    mtime_ns = int.from_bytes(stream.read(8), 'big', signed=True)
    uid = int.from_bytes(stream.read(4), 'big')
    gid = int.from_bytes(stream.read(4), 'big')
    return SymlinkEntry(path, target, mtime_ns, uid, gid)


def main():
    parser = ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--version', action='version', version=f'blockcopy {__version__}')

    subparsers = parser.add_subparsers(dest='command', required=True)
    p_checksum = subparsers.add_parser('checksum')
    p_retrieve = subparsers.add_parser('retrieve')
    p_save = subparsers.add_parser('save')

    p_checksum.add_argument('destination_file')
    p_checksum.add_argument('--progress', action='store_true', help='show progress info')
    p_checksum.add_argument('--start', type=int, metavar='OFFSET', default=0)
    p_checksum.add_argument('--end', type=int, metavar='OFFSET', default=None)
    p_checksum.add_argument('-r', '--recursive', action='store_true', help='recursively process directories')

    p_retrieve.add_argument('source_file')
    p_retrieve.add_argument('--lzma', action='store_true', help='use lzma compression')
    p_retrieve.add_argument('-r', '--recursive', action='store_true', help='recursively process directories')
    p_retrieve.add_argument('--delete', action='store_true', help='delete files on destination that do not exist on source')

    p_save.add_argument('destination_file')
    p_save.add_argument('--truncate', action='store_true', help='truncate the destination file to the size of the source file')
    p_save.add_argument('--times', '-t', action='store_true', help='preserve timestamps from source file')
    p_save.add_argument('--perms', '-p', action='store_true', help='preserve permissions from source file')
    p_save.add_argument('--owner', '-o', action='store_true', help='preserve owner from source file')
    p_save.add_argument('--group', '-g', action='store_true', help='preserve group from source file')
    p_save.add_argument('--numeric-ids', action='store_true', help='use numeric uid/gid instead of looking up user/group names')
    p_save.add_argument('-r', '--recursive', action='store_true', help='recursively process directories')

    args = parser.parse_args()

    setup_logging(args.verbose or environ.get('DEBUG'))
    logger.debug('Args: %r', args)

    ctrl_c_will_terminate_immediately()

    try:
        if args.command == 'checksum':
            if args.recursive:
                do_checksum_recursive(args.destination_file, stdin.buffer, stdout.buffer, show_progress=args.progress)
            else:
                do_checksum(args.destination_file, stdout.buffer, start_offset=args.start, end_offset=args.end, show_progress=args.progress)
        elif args.command == 'retrieve':
            if args.recursive:
                do_retrieve_recursive(args.source_file, stdin.buffer, stdout.buffer, use_lzma=args.lzma, delete=args.delete, verbose=args.verbose)
            else:
                do_retrieve(args.source_file, stdin.buffer, stdout.buffer, use_lzma=args.lzma, verbose=args.verbose)
        elif args.command == 'save':
            if args.recursive:
                do_save_recursive(args.destination_file, stdin.buffer,
                                  preserve_times=args.times, preserve_perms=args.perms,
                                  preserve_owner=args.owner, preserve_group=args.group,
                                  numeric_ids=args.numeric_ids)
            else:
                do_save(args.destination_file, stdin.buffer, truncate=args.truncate,
                        preserve_times=args.times, preserve_perms=args.perms,
                        preserve_owner=args.owner, preserve_group=args.group,
                        numeric_ids=args.numeric_ids)
        else:
            raise Exception(f'Not implemented: {args.command}')

        logger.debug('Done (command=%r)', args.command)
    except CollectedExceptions as exc:
        logger.error('Failed: %r', exc)
        exit(f'ERROR ({args.command}): {exc}')
    except Exception as exc:
        logger.exception('Failed: %r', exc)
        exit(f'ERROR ({args.command}): {exc}')


def setup_logging(verbose):
    from logging import basicConfig, DEBUG, INFO
    basicConfig(
        format='%(asctime)s [%(process)d %(threadName)s] %(name)s %(levelname)5s: %(message)s',
        level=DEBUG if verbose else INFO)


def ctrl_c_will_terminate_immediately():
    '''
    Make Ctrl+C terminate the process immediately.

    Doing graceful shutdown with threads, queues and ThreadPoolExecutor is too complicated.
    Since this program is only used for copying files, it's OK to terminate immediately
    and let the OS clear up resources - close open files and standard input/output.

    The only downside is that the shell may print message "Killed" instead of "Terminated".

    If you know about a better solution, please let me know :)
    '''
    from signal import signal, SIGTERM, SIGINT, SIGKILL
    signal(SIGINT, lambda *args: kill(getpid(), SIGKILL))
    signal(SIGTERM, lambda *args: kill(getpid(), SIGKILL))


def do_checksum(file_path, hash_output_stream, start_offset, end_offset, show_progress):
    '''
    Read the file in blocks, calculate hash of each block and write the hashes to the output stream.

    The output stream is a binary stream of the following format:

    - 4 bytes: command "hash"
    - 4 bytes: size of the block
    - 64 bytes: hash of the block
    - 4 bytes: command "hash"
    - 4 bytes: size of the block
    - 64 bytes: hash of the block
    - ...
    - 4 bytes: command "done"
    '''
    if hash_output_stream.isatty():
        exit('ERROR (checksum): hash_output_stream is a tty - will not write binary data to terminal')
    start_offset = start_offset or 0
    if end_offset is not None:
        assert start_offset < end_offset

    checksum_start_time = monotonic()

    with ThreadPoolExecutor(worker_count + 2, thread_name_prefix='checksum') as executor:
        hash_output_stream_lock = Lock()
        block_queue = Queue(worker_count * 3)
        send_queue = Queue(worker_count * 3)
        source_end_offset = None
        exception_collector = ExceptionCollector()

        def read_worker():
            # Only one will run
            nonlocal source_end_offset, show_progress
            show_progress_total_size = None
            show_progress_last_output_time = monotonic()
            show_progress_last_output_pct = 0
            try:
                with ExitStack() as stack:
                    if file_path == '-':
                        f = stdin.buffer
                    else:
                        f = stack.enter_context(open(file_path, 'rb'))

                    if show_progress:
                        if end_offset is None:
                            try:
                                assert f.tell() == 0
                                show_progress_total_size = f.seek(0, SEEK_END)
                                f.seek(0)
                            except OSError:
                                # Probably `[Errno 29] Illegal seek` when reading from pipe e.g. from pv command
                                show_progress = False
                        else:
                            show_progress_total_size = end_offset - start_offset

                    if start_offset:
                        block_pos = f.seek(start_offset)
                        assert block_pos == start_offset
                    else:
                        block_pos = 0

                    while True:
                        block_data_batch = []

                        for _ in range(16):
                            if exception_collector.has_exception():
                                break

                            try:
                                current_pos = f.tell()
                            except OSError:
                                # Probably `[Errno 29] Illegal seek` when reading from pipe e.g. from pv command
                                pass
                            else:
                                assert block_pos == current_pos
                                del current_pos

                            if end_offset is None:
                                block_data = f.read(block_size)
                            elif block_pos >= end_offset:
                                break
                            else:
                                block_data = f.read(min(block_size, end_offset - block_pos))

                            if not block_data:
                                break
                            block_data_batch.append((block_pos, block_data))

                            block_pos += len(block_data)

                        if not block_data_batch or exception_collector.has_exception():
                            break

                        hash_result_event = Event()
                        hash_result_container = []
                        block_queue.put((block_data_batch, hash_result_event, hash_result_container))
                        send_queue.put((hash_result_event, hash_result_container))

                        del block_data_batch

                        if show_progress:
                            show_progress_pct = 100 * (block_pos - start_offset) / show_progress_total_size
                            if monotonic() - show_progress_last_output_time >= 60 or show_progress_pct - show_progress_last_output_pct >= 5:
                                show_progress_last_output_time = monotonic()
                                show_progress_last_output_pct = show_progress_pct
                                print(
                                    f'Checksum progress: {block_pos/2**30:7.2f} GB / {show_progress_total_size/2**30:.2f} GB'
                                    f' ({show_progress_pct:5.2f}%)',
                                    file=stderr,
                                    flush=True)

                    if end_offset is None:
                        try:
                            # Mark where we have ended reading the destination file checksum.
                            # The retrieve side then has chance to read and send anything from the source beyond this offset.
                            source_end_offset = f.tell()
                        except OSError:
                            # Probably `[Errno 29] Illegal seek` when reading from pipe e.g. from pv command
                            source_end_offset = None
                    elif end_offset > block_pos:
                        raise Exception('You have specified an --end offset, but the destination file (which we are checksumming now) is smaller than that')
                    else:
                        assert end_offset == block_pos

            except Exception as exc:
                logger.exception('do_checksum read_worker failed: %r', exc)
                exception_collector.collect_exception(exc)
            except BaseException as exc:
                # not sure what the exception could be, but let's log it and re-raise it
                logger.exception('do_checksum read_worker failed (BaseException): %r', exc)
                exception_collector.collect_exception(exc)
                raise exc
            finally:
                for _ in range(worker_count):
                    block_queue.put(None)
                send_queue.put(None)

        def hash_worker():
            # Will run in multiple threads
            try:
                while True:

                    task = block_queue.get()
                    try:
                        if task is None:
                            break

                        if exception_collector.has_exception():
                            # just consume all tasks
                            continue

                        block_data_batch, hash_result_event, hash_result_container = task
                        hash_results = []
                        for block_pos, block_data in block_data_batch:
                            if exception_collector.has_exception():
                                break
                            hash_results.append((
                                block_pos,
                                len(block_data),
                                hash_factory(block_data).digest(),
                            ))
                        hash_result_container.append(hash_results)
                        hash_result_event.set()
                    finally:
                        block_queue.task_done()
            except Exception as exc:
                exception_collector.collect_exception(exc)

        def send_worker():
            # Only one will run
            try:
                while True:
                    task = send_queue.get()
                    try:
                        if task is None:
                            break

                        if exception_collector.has_exception():
                            # just consume all tasks
                            continue

                        hash_result_event, hash_result_container = task
                        hash_result_event.wait()
                        hash_results, = hash_result_container
                        with hash_output_stream_lock:
                            for block_pos, block_data_length, block_hash in hash_results:
                                if exception_collector.has_exception():
                                    break
                                hash_output_stream.write(b'Hash')
                                hash_output_stream.write(block_pos.to_bytes(8, 'big'))
                                hash_output_stream.write(block_data_length.to_bytes(4, 'big'))
                                hash_output_stream.write(block_hash)
                    finally:
                        send_queue.task_done()
            except Exception as exc:
                logger.exception('do_checksum send_worker failed: %r', exc)
                exception_collector.collect_exception(exc)
            except BaseException as exc:
                # not sure what the exception could be, but let's log it and re-raise it
                logger.exception('do_checksum send_worker failed (BaseException): %r', exc)
                exception_collector.collect_exception(exc)
                raise exc

        futures = [
            executor.submit(read_worker),
            *[executor.submit(hash_worker) for _ in range(worker_count)],
            executor.submit(send_worker),
        ]
        for f in futures:
            f.result()

    # no threads should be running any more at this point
    exception_collector.check_and_raise()

    with hash_output_stream_lock:
        if source_end_offset is not None:
            # Instruct the retrieve process to send data after the last hashed block.
            # This is necessary when the destination file (which we are checksumming now)
            # is smaller than the source file and we want to copy the whole source file.
            hash_output_stream.write(b'rest')
            hash_output_stream.write(source_end_offset.to_bytes(8, 'big'))

        hash_output_stream.write(b'done')
        hash_output_stream.flush()

    if show_progress:
        checksum_duration = monotonic() - checksum_start_time
        print(f'Checksum done in {checksum_duration:.3f} seconds', file=stderr, flush=True)


def do_retrieve(file_path, hash_input_stream, block_output_stream, use_lzma, verbose=False):
    '''
    Read the file in blocks, calculate hash of each block, read hash from
    hash_input_stream and if those hashes differ, write the block to
    block_output_stream.

    The output stream is a binary stream of the following format:

    - 4 bytes: command "data"
    - 8 bytes: position of the block in the file
    - 4 bytes: size of the block
    - N bytes: block data
    - 4 bytes: command "data"
    - 8 bytes: position of the block in the file
    - 4 bytes: size of the block
    - N bytes: block data
    - ...
    - 4 bytes: command "Done"
    - 8 bytes: size of the source file
    '''
    if use_lzma:
        from lzma import compress as lzma_compress

    if file_path == '-':
        exit('ERROR (retrieve): file_path must be actual file or device, not `-`')
    file_path = Path(file_path).resolve()
    assert file_path.is_file() or file_path.is_block_device()

    if block_output_stream.isatty():
        exit('ERROR (retrieve): block_output_stream is a tty - will not write binary data to terminal')

    with ThreadPoolExecutor(worker_count + 2, thread_name_prefix='retrieve') as executor:
        block_output_stream_lock = Lock()
        hash_queue = Queue(worker_count * 3)
        send_queue = Queue(worker_count * 3)
        exception_collector = ExceptionCollector()
        received_done = False
        encountered_incomplete_read = None

        def read_worker():
            # Only one will run
            nonlocal received_done, encountered_incomplete_read
            try:
                with file_path.open(mode='rb') as f:
                    hash_batch = []

                    def flush_hash_batch():
                        nonlocal hash_batch
                        if hash_batch:
                            hash_result_event = Event()
                            hash_result_container = []
                            hash_queue.put((hash_batch, hash_result_event, hash_result_container))
                            send_queue.put((hash_result_event, hash_result_container))
                            hash_batch = []

                    while True:
                        if exception_collector.has_exception():
                            break

                        command = hash_input_stream.read(4)

                        if not command:
                            flush_hash_batch()
                            raise IncompleteReadError('The hash input stream was closed unexpectedly without receiving the `done` command')

                        if len(command) != 4:
                            flush_hash_batch()
                            raise IncompleteReadError('Incomplete read of command from hash input stream')

                        if command == b'done':
                            if verbose:
                                logger.debug('Retrieve received command %r', command)
                            received_done = True
                            flush_hash_batch()
                            break

                        elif command == b'hash':
                            # This is the deprecated version of the hash command - does not contain
                            # block position.

                            block_size_b = hash_input_stream.read(4)
                            destination_hash = hash_input_stream.read(hash_digest_size)
                            if len(block_size_b) != 4 or len(destination_hash) != hash_digest_size:
                                raise IncompleteReadError('Incomplete read of hash from hash input stream')
                            block_size = int.from_bytes(block_size_b, 'big')
                            if verbose:
                                logger.debug(
                                    'Retrieve received command %r block_size=%d destination_hash=%s',
                                    command, block_size, b64encode(destination_hash).decode('utf-8'))
                            assert len(destination_hash) == hash_digest_size
                            block_pos = f.tell()
                            block_data = f.read(block_size)
                            assert block_data

                            hash_batch.append((destination_hash, block_pos, block_data))

                            if len(hash_batch) >= 16:
                                flush_hash_batch()

                        elif command == b'Hash':
                            block_pos_b = hash_input_stream.read(8)
                            block_size_b = hash_input_stream.read(4)
                            destination_hash = hash_input_stream.read(hash_digest_size)
                            if len(block_pos_b) != 8 or len(block_size_b) != 4 or len(destination_hash) != hash_digest_size:
                                raise IncompleteReadError('Incomplete read of hash from hash input stream')
                            block_pos = int.from_bytes(block_pos_b, 'big')
                            block_size = int.from_bytes(block_size_b, 'big')
                            if verbose:
                                logger.debug(
                                    'Retrieve received command %r block_pos=%d block_size=%d destination_hash=%s',
                                    command, block_pos, block_size, b64encode(destination_hash).decode('utf-8'))
                            assert len(destination_hash) == hash_digest_size
                            if f.tell() != block_pos:
                                if verbose:
                                    logger.debug('Seeking to %d', block_pos)
                                f.seek(block_pos)
                                assert f.tell() == block_pos
                            block_data = f.read(block_size)

                            if len(block_data) == block_size:
                                hash_batch.append((destination_hash, block_pos, block_data))
                            elif block_data:
                                # Probably just at end of source file while the destination file is larger.
                                # Let's send whatever we have read.
                                hash_batch.append((None, block_pos, block_data))
                            else:
                                # Beyond end of source file while the destination file is larger.
                                # Nothing to send.
                                pass

                            if len(hash_batch) >= 16:
                                flush_hash_batch()

                        elif command == b'rest':
                            # Just read the rest of the file.
                            # No hashing - there is nothing to compare with.
                            # We will send all the data to the destination.

                            offset_b = hash_input_stream.read(8)
                            if len(offset_b) != 8:
                                raise IncompleteReadError('Incomplete read of offset from hash input stream')
                            offset = int.from_bytes(offset_b, 'big')
                            if verbose:
                                logger.debug('Retrieve received command %r offset=%d', command, offset)
                            f.seek(offset)
                            assert f.tell() == offset
                            # logger.debug('Sending the rest of the file from offset %d', offset)

                            while True:
                                if exception_collector.has_exception():
                                    break

                                block_batch = []

                                for _ in range(16):
                                    if exception_collector.has_exception():
                                        break
                                    block_pos = f.tell()
                                    block_data = f.read(block_size)
                                    if not block_data:
                                        break
                                    block_batch.append((block_pos, b'data', block_data))

                                if not block_batch:
                                    break

                                hash_result_event = Event()
                                hash_result_event.set()
                                send_queue.put((hash_result_event, [block_batch]))
                                del block_batch

                        else:
                            logger.debug('Retrieve received unknown command: %r', command)
                            raise Exception(f'Unknown command received: {command!r}')

                    assert not hash_batch

            except IncompleteReadError as exc:
                logger.exception('do_retrieve read_worker encountered incomplete read: %s', exc)
                # Do not trigger the exception collector - it would make other threads terminate.
                # But do set some flag that the whole workflow is not running successfully.
                encountered_incomplete_read = exc
            except Exception as exc:
                logger.exception('do_retrieve read_worker failed: %r', exc)
                exception_collector.collect_exception(exc)
            except BaseException as exc:
                # not sure what the exception could be, but let's log it and re-raise it
                logger.exception('do_retrieve read_worker failed (BaseException): %r', exc)
                exception_collector.collect_exception(exc)
                raise exc
            finally:
                for _ in range(worker_count):
                    hash_queue.put(None)
                send_queue.put(None)

        def hash_worker():
            # Will run in multiple threads
            try:
                while True:
                    task = hash_queue.get()
                    try:
                        if task is None:
                            break

                        try:
                            batch, hash_result_event, hash_result_container = task

                            to_send = []
                            for destination_hash, block_pos, block_data in batch:
                                if exception_collector.has_exception():
                                    # Just consume all work, do nothing
                                    break
                                block_hash = hash_factory(block_data).digest()
                                if block_hash != destination_hash:
                                    if use_lzma:
                                        block_data_lzma = lzma_compress(block_data)
                                        if len(block_data_lzma) < len(block_data):
                                            to_send.append((block_pos, b'dlzm', block_data_lzma))
                                        else:
                                            to_send.append((block_pos, b'data', block_data))
                                    else:
                                        to_send.append((block_pos, b'data', block_data))

                            hash_result_container.append(to_send)
                            hash_result_event.set()
                        except Exception as exc:
                            # Should not happen.
                            # If this happens, the send_worker thread could block on hash_result_event.wait().
                            logger.exception('do_retrieve hash_worker failed to process task: %r', exc)
                            exception_collector.collect_exception(exc)
                    finally:
                        hash_queue.task_done()
            except Exception as exc:
                logger.exception('do_retrieve hash_worker failed: %r', exc)
                exception_collector.collect_exception(exc)
            except BaseException as exc:
                # not sure what the exception could be, but let's log it and re-raise it
                logger.exception('do_retrieve hash_worker failed (BaseException): %r', exc)
                exception_collector.collect_exception(exc)
                raise exc

        def send_worker():
            # Only one will run
            try:
                while True:
                    task = send_queue.get()
                    try:
                        if task is None:
                            break

                        if exception_collector.has_exception():
                            # just consume all tasks
                            continue

                        hash_result_event, hash_result_container = task
                        hash_result_event.wait()
                        to_send, = hash_result_container
                        with block_output_stream_lock:
                            for block_pos, block_command, block_data in to_send:
                                if exception_collector.has_exception():
                                    break
                                if block_command == b'dlzm':
                                    assert use_lzma
                                    assert block_data.startswith(b'\xfd7zXZ\x00')
                                elif block_command != b'data':
                                    raise Exception(f'Unknown block command: {block_command!r}')
                                block_output_stream.write(block_command)
                                block_output_stream.write(block_pos.to_bytes(8, 'big'))
                                block_output_stream.write(len(block_data).to_bytes(4, 'big'))
                                block_output_stream.write(block_data)
                            block_output_stream.flush()
                    finally:
                        send_queue.task_done()
            except Exception as exc:
                logger.exception('do_retrieve send_worker failed: %r', exc)
                exception_collector.collect_exception(exc)
            except BaseException as exc:
                # not sure what the exception could be, but let's log it and re-raise it
                logger.exception('do_retrieve send_worker failed (BaseException): %r', exc)
                exception_collector.collect_exception(exc)
                raise exc

        futures = [
            executor.submit(read_worker),
            *[executor.submit(hash_worker) for _ in range(worker_count)],
            executor.submit(send_worker),
        ]
        for f in futures:
            f.result()

    # no threads should be running any more at this point
    exception_collector.check_and_raise()

    if encountered_incomplete_read:
        exit(f'ERROR (retrieve): {encountered_incomplete_read}')

    if not received_done:
        # This should not happen, because that should already trigger the incomplete read exception.
        exit('ERROR (retrieve): Received no done command from the checksum side')

    if environ.get('SKIP_SENDING_META'):
        logger.info('Skipping metadata')
    else:
        # Get metadata, with optional env overrides for deterministic testing
        stat_result = file_path.stat()
        atime_ns = int(environ.get('BLOCKCOPY_OVERRIDE_ATIME_NS', stat_result.st_atime_ns))
        mtime_ns = int(environ.get('BLOCKCOPY_OVERRIDE_MTIME_NS', stat_result.st_mtime_ns))
        mode = int(environ.get('BLOCKCOPY_OVERRIDE_MODE', stat_result.st_mode))
        uid = int(environ.get('BLOCKCOPY_OVERRIDE_UID', stat_result.st_uid))
        gid = int(environ.get('BLOCKCOPY_OVERRIDE_GID', stat_result.st_gid))
        owner_name = environ.get('BLOCKCOPY_OVERRIDE_OWNER', get_user_name_by_uid(stat_result.st_uid))
        group_name = environ.get('BLOCKCOPY_OVERRIDE_GROUP', get_group_name_by_gid(stat_result.st_gid))

        with file_path.open(mode='rb') as f:
            # On block devices, stat_result.st_size is 0, so we need to get size of the device via seek.
            total_size = f.seek(0, SEEK_END)

        owner_name_bytes = owner_name.encode('utf-8')
        group_name_bytes = group_name.encode('utf-8')

        logger.debug(
            'File %s metadata: atime_ns=%d mtime_ns=%d mode=%d uid=%d gid=%d owner_name=%s group_name=%s size(seek)=%d size(stat)=%d',
            file_path, atime_ns, mtime_ns, mode, uid, gid, owner_name, group_name, total_size, stat_result.st_size)

        with block_output_stream_lock:
            # Send metadata command
            block_output_stream.write(b'meta')
            block_output_stream.write(atime_ns.to_bytes(8, 'big', signed=True))
            block_output_stream.write(mtime_ns.to_bytes(8, 'big', signed=True))
            block_output_stream.write(mode.to_bytes(4, 'big'))
            block_output_stream.write(uid.to_bytes(4, 'big'))
            block_output_stream.write(gid.to_bytes(4, 'big'))
            block_output_stream.write(len(owner_name_bytes).to_bytes(2, 'big'))
            block_output_stream.write(owner_name_bytes)
            block_output_stream.write(len(group_name_bytes).to_bytes(2, 'big'))
            block_output_stream.write(group_name_bytes)
            block_output_stream.write(total_size.to_bytes(8, 'big'))
            block_output_stream.write(b'end')

        del atime_ns, mtime_ns, mode, uid, gid, owner_name_bytes, group_name_bytes, total_size

    with block_output_stream_lock:
        # Send done command
        block_output_stream.write(b'done')
        block_output_stream.flush()


def do_save(file_path, block_input_stream, truncate=False,
            preserve_times=False, preserve_perms=False,
            preserve_owner=False, preserve_group=False,
            numeric_ids=False):
    '''
    Read blocks from block_input_stream and write them to the file.

    If truncate is True, truncate the file to the size of the source file.
    '''
    lzma_decompress = None

    if file_path == '-':
        exit('ERROR (save): file_path must be actual file or device, not `-`')
    file_path = Path(file_path).resolve()
    assert file_path.is_file() or file_path.is_block_device()

    try:
        received_any_data = False
        received_done = False
        received_atime_ns = None
        received_mtime_ns = None
        received_mode = None
        received_uid = None
        received_gid = None
        received_owner_name = None
        received_group_name = None
        received_total_size = None

        with open(file_path, 'r+b') as f:
            while True:
                command = block_input_stream.read(4)
                if not command:
                    raise IncompleteReadError('The block input stream was closed unexpectedly without receiving the `done` command')
                if len(command) != 4:
                    raise IncompleteReadError('Incomplete read of command from block input stream')

                if command == b'done':
                    received_done = True
                    f.flush()
                    if truncate and received_total_size is not None:
                        if received_total_size == 0 and received_any_data:
                            raise Exception('Would truncate to 0 bytes, but received some data - this is probably some error')
                        f.truncate(received_total_size)
                    break

                elif command in (b'data', b'dlzm'):
                    block_pos_b = block_input_stream.read(8)
                    block_size_b = block_input_stream.read(4)
                    if len(block_pos_b) != 8 or len(block_size_b) != 4:
                        raise IncompleteReadError('Incomplete read of block position and size from block input stream')
                    block_pos = int.from_bytes(block_pos_b, 'big')
                    block_size = int.from_bytes(block_size_b, 'big')
                    block_data = block_input_stream.read(block_size)
                    if len(block_data) != block_size:
                        raise IncompleteReadError('Incomplete read of block data from block input stream')
                    received_any_data = True
                    if command == b'dlzm':
                        if lzma_decompress is None:
                            from lzma import decompress as lzma_decompress
                        block_data = lzma_decompress(block_data)
                    f.seek(block_pos)
                    f.write(block_data)

                elif command == b'meta':
                    # Read metadata: atime_ns(8) + mtime_ns(8) + mode(4) + uid(4) + gid(4) = 28 bytes
                    meta_data = block_input_stream.read(28)
                    if len(meta_data) != 28:
                        raise IncompleteReadError('Incomplete read of metadata from block input stream')
                    received_atime_ns = int.from_bytes(meta_data[0:8], 'big', signed=True)
                    received_mtime_ns = int.from_bytes(meta_data[8:16], 'big', signed=True)
                    received_mode = int.from_bytes(meta_data[16:20], 'big')
                    received_uid = int.from_bytes(meta_data[20:24], 'big')
                    received_gid = int.from_bytes(meta_data[24:28], 'big')

                    # Read owner name
                    owner_name_len_bytes = block_input_stream.read(2)
                    if len(owner_name_len_bytes) != 2:
                        raise IncompleteReadError('Incomplete read of owner name length')
                    owner_name_len = int.from_bytes(owner_name_len_bytes, 'big')
                    received_owner_name = block_input_stream.read(owner_name_len).decode('utf-8') if owner_name_len > 0 else ''

                    # Read group name
                    group_name_len_bytes = block_input_stream.read(2)
                    if len(group_name_len_bytes) != 2:
                        raise IncompleteReadError('Incomplete read of group name length')
                    group_name_len = int.from_bytes(group_name_len_bytes, 'big')
                    received_group_name = block_input_stream.read(group_name_len).decode('utf-8') if group_name_len > 0 else ''

                    # Read total size
                    total_size_bytes = block_input_stream.read(8)
                    if len(total_size_bytes) != 8:
                        raise IncompleteReadError('Incomplete read of total size from block input stream')
                    received_total_size = int.from_bytes(total_size_bytes, 'big')

                    # Read end marker
                    end_marker = block_input_stream.read(3)
                    if end_marker != b'end':
                        raise IncompleteReadError(f'Expected end marker, got {end_marker!r}')
                else:
                    raise Exception(f'Unknown command received: {command!r}')

        if not received_done:
            # Should not happen - should already trigger the incomplete read exception.
            exit('ERROR (save): Received no done command from the retrieve side')

        # Apply metadata after file is closed
        if preserve_perms and received_mode is not None:
            old_mode = file_path.stat().st_mode
            chmod(file_path, received_mode)
            new_mode = file_path.stat().st_mode
            logger.info('chmod %s: %o -> %o', file_path, old_mode, new_mode)

        if (preserve_owner or preserve_group) and (received_uid is not None or received_gid is not None):
            uid_to_set = -1
            gid_to_set = -1

            if preserve_owner and received_uid is not None:
                if numeric_ids or not received_owner_name:
                    uid_to_set = received_uid
                else:
                    try:
                        uid_to_set = getpwnam(received_owner_name).pw_uid
                    except KeyError:
                        logger.warning('User %r not found, falling back to uid %d', received_owner_name, received_uid)
                        uid_to_set = received_uid

            if preserve_group and received_gid is not None:
                if numeric_ids or not received_group_name:
                    gid_to_set = received_gid
                else:
                    try:
                        gid_to_set = getgrnam(received_group_name).gr_gid
                    except KeyError:
                        logger.warning('Group %r not found, falling back to gid %d', received_group_name, received_gid)
                        gid_to_set = received_gid

            try:
                old_stat = file_path.stat()
                chown(file_path, uid_to_set, gid_to_set)
                new_stat = file_path.stat()
                logger.info(
                    'chown %s: uid %d (%s) -> %d (%s), gid %d (%s) -> %d (%s)', file_path,
                    old_stat.st_uid, get_user_name_by_uid(old_stat.st_uid),
                    new_stat.st_uid, get_user_name_by_uid(new_stat.st_uid),
                    old_stat.st_gid, get_group_name_by_gid(old_stat.st_gid),
                    new_stat.st_gid, get_group_name_by_gid(new_stat.st_gid))
            except PermissionError as e:
                logger.warning('Failed to change owner/group: %s', e)

        if preserve_times and received_atime_ns is not None and received_mtime_ns is not None:
            old_stat = file_path.stat()
            utime(file_path, ns=(received_atime_ns, received_mtime_ns))
            new_stat = file_path.stat()
            logger.info('utime %s: atime_ns %d -> %d, mtime_ns %d -> %d', file_path, old_stat.st_atime_ns, new_stat.st_atime_ns, old_stat.st_mtime_ns, new_stat.st_mtime_ns)

    except IncompleteReadError as exc:
        exit(f'ERROR (save): {exc}')


def do_checksum_recursive(dir_path, input_stream, output_stream, show_progress=False):
    '''
    Recursive checksum mode.

    Scan destination directory and send file list with block hashes.
    For each file, we send metadata followed by SHA3-512 hashes of each 128KB block.
    This enables block-level diff on the retrieve side.
    '''
    if output_stream.isatty():
        exit('ERROR (checksum): output_stream is a tty - will not write binary data to terminal')

    dir_path = Path(dir_path).resolve()
    if not dir_path.is_dir():
        exit(f'ERROR (checksum): {dir_path} is not a directory')

    # Scan destination directory
    logger.debug('Scanning destination directory: %s', dir_path)
    files, dirs, symlinks = scan_directory(dir_path)
    logger.debug('Found %d files, %d dirs, %d symlinks', len(files), len(dirs), len(symlinks))

    # Send file list header
    total_entries = len(files) + len(dirs) + len(symlinks)
    output_stream.write(b'list')
    output_stream.write(total_entries.to_bytes(8, 'big'))

    # Send files with block hashes
    for entry in sorted(files, key=lambda e: e.path):
        write_file_entry(output_stream, entry)

        # Now send block hashes for this file
        file_path = dir_path / entry.path
        try:
            with open(file_path, 'rb') as f:
                block_pos = 0
                while True:
                    block_data = f.read(block_size)
                    if not block_data:
                        break
                    block_hash = hash_factory(block_data).digest()
                    output_stream.write(b'Hash')
                    output_stream.write(block_pos.to_bytes(8, 'big'))
                    output_stream.write(len(block_data).to_bytes(4, 'big'))
                    output_stream.write(block_hash)
                    block_pos += len(block_data)
        except OSError as e:
            logger.warning('Failed to read file %s for hashing: %s', file_path, e)

        # End of file hashes
        output_stream.write(b'fend')

    # Send directories
    for entry in sorted(dirs, key=lambda e: e.path):
        write_dir_entry(output_stream, entry)

    # Send symlinks
    for entry in sorted(symlinks, key=lambda e: e.path):
        write_symlink_entry(output_stream, entry)

    # End of list and done
    output_stream.write(b'ends')
    output_stream.write(b'done')
    output_stream.flush()


def do_retrieve_recursive(dir_path, input_stream, output_stream, use_lzma=False, delete=False, verbose=False):
    '''
    Recursive retrieve mode with block-level diff.

    1. Receive file list with block hashes from destination
    2. Scan source directory
    3. Compare block hashes and send only differing blocks
    '''
    if use_lzma:
        from lzma import compress as lzma_compress

    if output_stream.isatty():
        exit('ERROR (retrieve): output_stream is a tty - will not write binary data to terminal')

    dir_path = Path(dir_path).resolve()
    if not dir_path.is_dir():
        exit(f'ERROR (retrieve): {dir_path} is not a directory')

    # Read destination file list with hashes
    command = input_stream.read(4)
    if command != b'list':
        raise Exception(f'Expected list command, got {command!r}')

    entry_count = int.from_bytes(input_stream.read(8), 'big')
    logger.debug('Receiving %d entries from destination', entry_count)

    # Store destination entries with their block hashes
    # dst_files: path -> (FileEntry, [(pos, size, hash), ...])
    dst_files = {}
    dst_dirs = {}
    dst_symlinks = {}

    for _ in range(entry_count):
        cmd = input_stream.read(4)
        if cmd == b'file':
            entry = read_file_entry(input_stream)
            # Read block hashes until fend
            block_hashes = []
            while True:
                hash_cmd = input_stream.read(4)
                if hash_cmd == b'fend':
                    break
                elif hash_cmd == b'Hash':
                    block_pos = int.from_bytes(input_stream.read(8), 'big')
                    blk_size = int.from_bytes(input_stream.read(4), 'big')
                    block_hash = input_stream.read(hash_digest_size)
                    if len(block_hash) != hash_digest_size:
                        raise IncompleteReadError('Incomplete block hash read')
                    block_hashes.append((block_pos, blk_size, block_hash))
                else:
                    raise Exception(f'Expected Hash or fend, got {hash_cmd!r}')
            dst_files[entry.path] = (entry, block_hashes)
        elif cmd == b'dirs':
            entry = read_dir_entry(input_stream)
            dst_dirs[entry.path] = entry
        elif cmd == b'link':
            entry = read_symlink_entry(input_stream)
            dst_symlinks[entry.path] = entry
        else:
            raise Exception(f'Unknown entry type: {cmd!r}')

    # Read end marker and done
    end_cmd = input_stream.read(4)
    if end_cmd != b'ends':
        raise Exception(f'Expected ends command, got {end_cmd!r}')

    done_cmd = input_stream.read(4)
    if done_cmd != b'done':
        raise Exception(f'Expected done command, got {done_cmd!r}')

    logger.debug('Received %d files, %d dirs, %d symlinks from destination',
                 len(dst_files), len(dst_dirs), len(dst_symlinks))

    # Scan source directory
    src_files, src_dirs, src_symlinks = scan_directory(dir_path)
    src_files_dict = {e.path: e for e in src_files}
    src_dirs_dict = {e.path: e for e in src_dirs}
    src_symlinks_dict = {e.path: e for e in src_symlinks}

    logger.debug('Source has %d files, %d dirs, %d symlinks',
                 len(src_files_dict), len(src_dirs_dict), len(src_symlinks_dict))

    # Send directories to create (sorted by path to create parents first)
    for path in sorted(src_dirs_dict.keys()):
        if path not in dst_dirs:
            entry = src_dirs_dict[path]
            output_stream.write(b'mkdr')
            path_bytes = path.encode('utf-8')
            owner_bytes = entry.owner_name.encode('utf-8')
            group_bytes = entry.group_name.encode('utf-8')
            output_stream.write(len(path_bytes).to_bytes(2, 'big'))
            output_stream.write(path_bytes)
            output_stream.write(entry.mtime_ns.to_bytes(8, 'big', signed=True))
            output_stream.write(entry.mode.to_bytes(4, 'big'))
            output_stream.write(entry.uid.to_bytes(4, 'big'))
            output_stream.write(entry.gid.to_bytes(4, 'big'))
            output_stream.write(len(owner_bytes).to_bytes(2, 'big'))
            output_stream.write(owner_bytes)
            output_stream.write(len(group_bytes).to_bytes(2, 'big'))
            output_stream.write(group_bytes)
            logger.debug('Will create directory: %s', path)

    # Send symlinks
    for path, entry in sorted(src_symlinks_dict.items()):
        dst_entry = dst_symlinks.get(path)
        if dst_entry is None or dst_entry.target != entry.target:
            output_stream.write(b'syml')
            path_bytes = path.encode('utf-8')
            target_bytes = entry.target.encode('utf-8')
            output_stream.write(len(path_bytes).to_bytes(2, 'big'))
            output_stream.write(path_bytes)
            output_stream.write(len(target_bytes).to_bytes(2, 'big'))
            output_stream.write(target_bytes)
            output_stream.write(entry.mtime_ns.to_bytes(8, 'big', signed=True))
            output_stream.write(entry.uid.to_bytes(4, 'big'))
            output_stream.write(entry.gid.to_bytes(4, 'big'))
            logger.debug('Will create/update symlink: %s -> %s', path, entry.target)

    output_stream.flush()

    # Process files with block-level diff
    for path, src_entry in sorted(src_files_dict.items()):
        dst_data = dst_files.get(path)
        file_path = dir_path / path

        if dst_data is None:
            # New file - send everything
            logger.debug('File %s: new file, sending all blocks', path)
            dst_hashes = {}
        else:
            dst_entry, block_hashes = dst_data
            # Build hash lookup: position -> (size, hash)
            dst_hashes = {pos: (size, h) for pos, size, h in block_hashes}

        # Signal start of file
        output_stream.write(b'FILE')
        path_bytes = path.encode('utf-8')
        output_stream.write(len(path_bytes).to_bytes(2, 'big'))
        output_stream.write(path_bytes)

        blocks_sent = 0
        blocks_skipped = 0

        # Read source file and compare block by block
        with open(file_path, 'rb') as f:
            block_pos = 0
            while True:
                block_data = f.read(block_size)
                if not block_data:
                    break

                # Check if this block matches destination
                dst_block = dst_hashes.get(block_pos)
                if dst_block is not None:
                    dst_blk_size, dst_hash = dst_block
                    if dst_blk_size == len(block_data):
                        src_hash = hash_factory(block_data).digest()
                        if src_hash == dst_hash:
                            # Block matches, skip it
                            blocks_skipped += 1
                            block_pos += len(block_data)
                            continue

                # Block differs or is new - send it
                blocks_sent += 1
                if use_lzma:
                    compressed = lzma_compress(block_data)
                    if len(compressed) < len(block_data):
                        output_stream.write(b'dlzm')
                        output_stream.write(block_pos.to_bytes(8, 'big'))
                        output_stream.write(len(compressed).to_bytes(4, 'big'))
                        output_stream.write(compressed)
                    else:
                        output_stream.write(b'data')
                        output_stream.write(block_pos.to_bytes(8, 'big'))
                        output_stream.write(len(block_data).to_bytes(4, 'big'))
                        output_stream.write(block_data)
                else:
                    output_stream.write(b'data')
                    output_stream.write(block_pos.to_bytes(8, 'big'))
                    output_stream.write(len(block_data).to_bytes(4, 'big'))
                    output_stream.write(block_data)

                block_pos += len(block_data)

        logger.debug('File %s: sent %d blocks, skipped %d blocks', path, blocks_sent, blocks_skipped)

        # Send file end marker
        output_stream.write(b'fend')

        # Send metadata for this file
        stat_result = file_path.stat()
        atime_ns = stat_result.st_atime_ns
        mtime_ns = stat_result.st_mtime_ns
        mode = stat_result.st_mode
        uid = stat_result.st_uid
        gid = stat_result.st_gid
        owner_name = get_user_name_by_uid(uid)
        group_name = get_group_name_by_gid(gid)

        with open(file_path, 'rb') as f:
            total_size = f.seek(0, SEEK_END)

        owner_bytes = owner_name.encode('utf-8')
        group_bytes = group_name.encode('utf-8')

        output_stream.write(b'meta')
        output_stream.write(atime_ns.to_bytes(8, 'big', signed=True))
        output_stream.write(mtime_ns.to_bytes(8, 'big', signed=True))
        output_stream.write(mode.to_bytes(4, 'big'))
        output_stream.write(uid.to_bytes(4, 'big'))
        output_stream.write(gid.to_bytes(4, 'big'))
        output_stream.write(len(owner_bytes).to_bytes(2, 'big'))
        output_stream.write(owner_bytes)
        output_stream.write(len(group_bytes).to_bytes(2, 'big'))
        output_stream.write(group_bytes)
        output_stream.write(total_size.to_bytes(8, 'big'))
        output_stream.write(b'end')

        output_stream.flush()

    # Handle deletions if --delete flag is set
    if delete:
        # Delete files that exist on destination but not on source
        for path in sorted(dst_files.keys(), reverse=True):
            if path not in src_files_dict:
                output_stream.write(b'dele')
                path_bytes = path.encode('utf-8')
                output_stream.write(len(path_bytes).to_bytes(2, 'big'))
                output_stream.write(path_bytes)
                output_stream.write(ENTRY_FILE.to_bytes(1, 'big'))
                logger.debug('Will delete file: %s', path)

        # Delete symlinks
        for path in sorted(dst_symlinks.keys(), reverse=True):
            if path not in src_symlinks_dict:
                output_stream.write(b'dele')
                path_bytes = path.encode('utf-8')
                output_stream.write(len(path_bytes).to_bytes(2, 'big'))
                output_stream.write(path_bytes)
                output_stream.write(ENTRY_SYMLINK.to_bytes(1, 'big'))
                logger.debug('Will delete symlink: %s', path)

        # Delete directories (reverse sorted to delete children first)
        for path in sorted(dst_dirs.keys(), reverse=True):
            if path not in src_dirs_dict:
                output_stream.write(b'dele')
                path_bytes = path.encode('utf-8')
                output_stream.write(len(path_bytes).to_bytes(2, 'big'))
                output_stream.write(path_bytes)
                output_stream.write(ENTRY_DIR.to_bytes(1, 'big'))
                logger.debug('Will delete directory: %s', path)

    # Final done
    output_stream.write(b'DONE')
    output_stream.flush()


def do_save_recursive(dir_path, input_stream,
                      preserve_times=False, preserve_perms=False,
                      preserve_owner=False, preserve_group=False,
                      numeric_ids=False):
    '''
    Recursive save mode.

    Receives commands from retrieve and applies them to destination directory.
    '''
    lzma_decompress = None

    dir_path = Path(dir_path).resolve()
    if not dir_path.is_dir():
        exit(f'ERROR (save): {dir_path} is not a directory')

    # Track files that need metadata applied at the end
    files_metadata = []
    dirs_metadata = []

    try:
        while True:
            command = input_stream.read(4)
            if not command:
                raise IncompleteReadError('Stream closed unexpectedly')
            if len(command) != 4:
                raise IncompleteReadError('Incomplete command read')

            if command == b'DONE':
                # All done
                break

            elif command == b'mkdr':
                # Create directory
                path_len = int.from_bytes(input_stream.read(2), 'big')
                rel_path = input_stream.read(path_len).decode('utf-8')
                mtime_ns = int.from_bytes(input_stream.read(8), 'big', signed=True)
                mode = int.from_bytes(input_stream.read(4), 'big')
                uid = int.from_bytes(input_stream.read(4), 'big')
                gid = int.from_bytes(input_stream.read(4), 'big')
                owner_len = int.from_bytes(input_stream.read(2), 'big')
                owner_name = input_stream.read(owner_len).decode('utf-8')
                group_len = int.from_bytes(input_stream.read(2), 'big')
                group_name = input_stream.read(group_len).decode('utf-8')

                full_path = dir_path / rel_path
                full_path.mkdir(parents=True, exist_ok=True)
                logger.debug('Created directory: %s', full_path)

                # Store for later metadata application
                dirs_metadata.append((full_path, mtime_ns, mode, uid, gid, owner_name, group_name))

            elif command == b'syml':
                # Create symlink
                path_len = int.from_bytes(input_stream.read(2), 'big')
                rel_path = input_stream.read(path_len).decode('utf-8')
                target_len = int.from_bytes(input_stream.read(2), 'big')
                target = input_stream.read(target_len).decode('utf-8')
                mtime_ns = int.from_bytes(input_stream.read(8), 'big', signed=True)
                uid = int.from_bytes(input_stream.read(4), 'big')
                gid = int.from_bytes(input_stream.read(4), 'big')

                full_path = dir_path / rel_path

                # Remove existing file/symlink if exists
                if full_path.exists() or full_path.is_symlink():
                    full_path.unlink()

                # Create parent directory if needed
                full_path.parent.mkdir(parents=True, exist_ok=True)

                symlink(target, full_path)
                logger.debug('Created symlink: %s -> %s', full_path, target)

                # Apply ownership if requested (can't apply mode/time to symlinks portably)
                if preserve_owner or preserve_group:
                    try:
                        uid_to_set = uid if preserve_owner else -1
                        gid_to_set = gid if preserve_group else -1
                        chown(full_path, uid_to_set, gid_to_set, follow_symlinks=False)
                    except (PermissionError, OSError) as e:
                        logger.warning('Failed to chown symlink %s: %s', full_path, e)

            elif command == b'FILE':
                # Start of file data
                path_len = int.from_bytes(input_stream.read(2), 'big')
                rel_path = input_stream.read(path_len).decode('utf-8')

                full_path = dir_path / rel_path
                logger.debug('Receiving file: %s', full_path)

                # Create parent directory if needed
                full_path.parent.mkdir(parents=True, exist_ok=True)

                # Open file for writing (create if doesn't exist)
                if full_path.exists():
                    f = open(full_path, 'r+b')
                else:
                    f = open(full_path, 'wb')

                received_total_size = None

                try:
                    while True:
                        file_cmd = input_stream.read(4)
                        if not file_cmd or len(file_cmd) != 4:
                            raise IncompleteReadError('Incomplete file command')

                        if file_cmd in (b'data', b'dlzm'):
                            block_pos = int.from_bytes(input_stream.read(8), 'big')
                            blk_size = int.from_bytes(input_stream.read(4), 'big')
                            block_data = input_stream.read(blk_size)
                            if len(block_data) != blk_size:
                                raise IncompleteReadError('Incomplete block data')

                            if file_cmd == b'dlzm':
                                if lzma_decompress is None:
                                    from lzma import decompress as lzma_decompress
                                block_data = lzma_decompress(block_data)

                            f.seek(block_pos)
                            f.write(block_data)

                        elif file_cmd == b'fend':
                            # File data ended, metadata follows
                            break

                        else:
                            raise Exception(f'Unexpected command in file stream: {file_cmd!r}')

                    # Read metadata
                    meta_cmd = input_stream.read(4)
                    if meta_cmd != b'meta':
                        raise Exception(f'Expected meta command, got {meta_cmd!r}')

                    meta_data = input_stream.read(28)
                    atime_ns = int.from_bytes(meta_data[0:8], 'big', signed=True)
                    mtime_ns = int.from_bytes(meta_data[8:16], 'big', signed=True)
                    mode = int.from_bytes(meta_data[16:20], 'big')
                    uid = int.from_bytes(meta_data[20:24], 'big')
                    gid = int.from_bytes(meta_data[24:28], 'big')

                    owner_len = int.from_bytes(input_stream.read(2), 'big')
                    owner_name = input_stream.read(owner_len).decode('utf-8')
                    group_len = int.from_bytes(input_stream.read(2), 'big')
                    group_name = input_stream.read(group_len).decode('utf-8')

                    total_size = int.from_bytes(input_stream.read(8), 'big')
                    end_marker = input_stream.read(3)
                    if end_marker != b'end':
                        raise IncompleteReadError(f'Expected end marker, got {end_marker!r}')

                    received_total_size = total_size

                finally:
                    if received_total_size is not None:
                        f.truncate(received_total_size)
                    f.close()

                # Store metadata for later application
                files_metadata.append((full_path, atime_ns, mtime_ns, mode, uid, gid, owner_name, group_name))

            elif command == b'dele':
                # Delete file/directory
                path_len = int.from_bytes(input_stream.read(2), 'big')
                rel_path = input_stream.read(path_len).decode('utf-8')
                entry_type = int.from_bytes(input_stream.read(1), 'big')

                full_path = dir_path / rel_path

                if entry_type == ENTRY_FILE:
                    if full_path.is_file():
                        os_remove(full_path)
                        logger.debug('Deleted file: %s', full_path)
                elif entry_type == ENTRY_SYMLINK:
                    if full_path.is_symlink():
                        full_path.unlink()
                        logger.debug('Deleted symlink: %s', full_path)
                elif entry_type == ENTRY_DIR:
                    if full_path.is_dir():
                        rmtree(full_path)
                        logger.debug('Deleted directory: %s', full_path)

            else:
                raise Exception(f'Unknown command: {command!r}')

        # Apply metadata to files
        for full_path, atime_ns, mtime_ns, mode, uid, gid, owner_name, group_name in files_metadata:
            if preserve_perms:
                try:
                    chmod(full_path, mode)
                except (PermissionError, OSError) as e:
                    logger.warning('Failed to chmod %s: %s', full_path, e)

            if preserve_owner or preserve_group:
                uid_to_set = -1
                gid_to_set = -1

                if preserve_owner:
                    if numeric_ids or not owner_name:
                        uid_to_set = uid
                    else:
                        try:
                            uid_to_set = getpwnam(owner_name).pw_uid
                        except KeyError:
                            uid_to_set = uid

                if preserve_group:
                    if numeric_ids or not group_name:
                        gid_to_set = gid
                    else:
                        try:
                            gid_to_set = getgrnam(group_name).gr_gid
                        except KeyError:
                            gid_to_set = gid

                try:
                    chown(full_path, uid_to_set, gid_to_set)
                except (PermissionError, OSError) as e:
                    logger.warning('Failed to chown %s: %s', full_path, e)

            if preserve_times:
                try:
                    utime(full_path, ns=(atime_ns, mtime_ns))
                except (PermissionError, OSError) as e:
                    logger.warning('Failed to utime %s: %s', full_path, e)

        # Apply metadata to directories (in reverse order to handle nested dirs)
        for full_path, mtime_ns, mode, uid, gid, owner_name, group_name in reversed(dirs_metadata):
            if preserve_perms:
                try:
                    chmod(full_path, mode)
                except (PermissionError, OSError) as e:
                    logger.warning('Failed to chmod %s: %s', full_path, e)

            if preserve_owner or preserve_group:
                uid_to_set = -1
                gid_to_set = -1

                if preserve_owner:
                    if numeric_ids or not owner_name:
                        uid_to_set = uid
                    else:
                        try:
                            uid_to_set = getpwnam(owner_name).pw_uid
                        except KeyError:
                            uid_to_set = uid

                if preserve_group:
                    if numeric_ids or not group_name:
                        gid_to_set = gid
                    else:
                        try:
                            gid_to_set = getgrnam(group_name).gr_gid
                        except KeyError:
                            gid_to_set = gid

                try:
                    chown(full_path, uid_to_set, gid_to_set)
                except (PermissionError, OSError) as e:
                    logger.warning('Failed to chown %s: %s', full_path, e)

            if preserve_times:
                try:
                    utime(full_path, ns=(mtime_ns, mtime_ns))
                except (PermissionError, OSError) as e:
                    logger.warning('Failed to utime %s: %s', full_path, e)

    except IncompleteReadError as exc:
        exit(f'ERROR (save): {exc}')


if __name__ == "__main__":
    main()
