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
from grp import getgrgid, getgrnam
from hashlib import sha3_512
from logging import getLogger
from os import chmod, chown, cpu_count, environ, getpid, kill, SEEK_END, utime
from pathlib import Path
from pwd import getpwuid, getpwnam
from queue import Queue
from sys import exit, stderr, stdin, stdout
from threading import Event, Lock
from time import monotonic, time_ns


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

    p_retrieve.add_argument('source_file')
    p_retrieve.add_argument('--lzma', action='store_true', help='use lzma compression')

    p_save.add_argument('destination_file')
    p_save.add_argument('--truncate', action='store_true', help='truncate the destination file to the size of the source file')
    p_save.add_argument('--times', '-t', action='store_true', help='preserve timestamps from source file')
    p_save.add_argument('--perms', '-p', action='store_true', help='preserve permissions from source file')
    p_save.add_argument('--owner', '-o', action='store_true', help='preserve owner from source file')
    p_save.add_argument('--group', '-g', action='store_true', help='preserve group from source file')
    p_save.add_argument('--numeric-ids', action='store_true', help='use numeric uid/gid instead of looking up user/group names')

    args = parser.parse_args()

    setup_logging(args.verbose or environ.get('DEBUG'))
    logger.debug('Args: %r', args)

    ctrl_c_will_terminate_immediately()

    try:
        if args.command == 'checksum':
            do_checksum(args.destination_file, stdout.buffer, start_offset=args.start, end_offset=args.end, show_progress=args.progress)
        elif args.command == 'retrieve':
            do_retrieve(args.source_file, stdin.buffer, stdout.buffer, use_lzma=args.lzma, verbose=args.verbose)
        elif args.command == 'save':
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
            nonlocal source_end_offset
            show_progress_total_size = None
            show_progress_last_output_time_ns = time_ns()
            show_progress_last_output_pct = 0
            show_progress_last_output_pos = start_offset
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
                                show_progress_total_size = None
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
                            if show_progress_total_size:
                                show_progress_pct = (100 * (block_pos - start_offset) / show_progress_total_size)
                            else:
                                show_progress_pct = None  # e.g. when processing a pipe we don't know the total size beforehand
                            now_ns = time_ns()
                            if abs(now_ns - show_progress_last_output_time_ns) >= 60e9 or (show_progress_pct and show_progress_pct - show_progress_last_output_pct >= 5):
                                elapsed_s = (now_ns - show_progress_last_output_time_ns) / 1e9
                                bytes_since_last = block_pos - show_progress_last_output_pos
                                speed_mb_s = bytes_since_last / 2**20 / elapsed_s if elapsed_s > 0 else 0
                                show_progress_last_output_time_ns = now_ns
                                show_progress_last_output_pct = show_progress_pct
                                show_progress_last_output_pos = block_pos
                                show_progress_total_size_str = f'{show_progress_total_size/2**30:.2f} GB' if show_progress_total_size else '?'
                                show_progress_pct_str = f'{show_progress_pct:5.2f} %' if show_progress_pct else '? %'
                                print(
                                    f'Checksum progress: {block_pos/2**30:7.2f} GB / {show_progress_total_size_str}'
                                    f' ({show_progress_pct_str}) {speed_mb_s:7.2f} MB/s',
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


if __name__ == "__main__":
    main()
