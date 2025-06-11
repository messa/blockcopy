#!/usr/bin/env python3

r'''
Copy large files (or block devices) effeciently over network.

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
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
from logging import getLogger
import os
from queue import Queue
import sys
from threading import Event, Lock


logger = getLogger(__name__)

block_size = 128 * 1024
hash_factory = hashlib.sha3_512
hash_digest_size = hash_factory().digest_size
assert hash_digest_size == 512 / 8
worker_count = min(os.cpu_count(), 8)


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


def main():
    parser = ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true')

    subparsers = parser.add_subparsers(dest='command', required=True)
    p_checksum = subparsers.add_parser('checksum')
    p_retrieve = subparsers.add_parser('retrieve')
    p_save = subparsers.add_parser('save')

    p_checksum.add_argument('file')
    p_checksum.add_argument('--start', type=int, default=0)
    p_checksum.add_argument('--end', type=int, default=None)

    p_retrieve.add_argument('file')

    p_save.add_argument('file')

    args = parser.parse_args()

    setup_logging(args.verbose or os.environ.get('DEBUG'))
    logger.debug('Args: %r', args)

    ctrl_c_will_terminate_immediately()

    try:
        if args.command == 'checksum':
            do_checksum(args.file, sys.stdout.buffer, args.start, args.end)
        elif args.command == 'retrieve':
            do_retrieve(args.file, sys.stdin.buffer, sys.stdout.buffer)
        elif args.command == 'save':
            do_save(args.file, sys.stdin.buffer)
        else:
            raise Exception(f'Not implemented: {args.command}')

        logger.debug('Done')
    except CollectedExceptions as exc:
        logger.error('Failed: %r', exc)
        sys.exit(f'ERROR ({args.command}): {exc}')
    except Exception as exc:
        logger.exception('Failed: %r', exc)
        sys.exit(f'ERROR ({args.command}): {exc}')


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
    signal(SIGINT, lambda *args: os.kill(os.getpid(), SIGKILL))
    signal(SIGTERM, lambda *args: os.kill(os.getpid(), SIGKILL))


def do_checksum(file_path, hash_output_stream, start_offset, end_offset):
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
        sys.exit('ERROR (checksum): hash_output_stream is a tty - will not write binary data to terminal')

    with ThreadPoolExecutor(worker_count + 2, thread_name_prefix='checksum') as executor:
        hash_output_stream_lock = Lock()
        block_queue = Queue(worker_count * 3)
        send_queue = Queue(worker_count * 3)
        source_end_offset = None
        exception_collector = ExceptionCollector()

        def read_worker():
            # Only one will run
            nonlocal source_end_offset
            try:
                with ExitStack() as stack:
                    if file_path == '-':
                        f = sys.stdin.buffer
                    else:
                        f = stack.enter_context(open(file_path, 'rb'))

                    if start_offset:
                        f.seek(start_offset)
                        block_pos = f.tell()
                        assert block_pos == start_offset
                    else:
                        block_pos = 0

                    while True:
                        if exception_collector.has_exception():
                            break

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

                        if not block_data_batch:
                            break

                        hash_result_event = Event()
                        hash_result_container = []
                        block_queue.put((block_data_batch, hash_result_event, hash_result_container))
                        send_queue.put((hash_result_event, hash_result_container))

                        del block_data_batch

                    try:
                        source_end_offset = f.tell()
                    except OSError:
                        # Probably `[Errno 29] Illegal seek` when reading from pipe e.g. from pv command
                        source_end_offset = None

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
            # Instruct the retrieve process to send data afther the last hashed block.
            # This is necessary when the destination file is smaller than the source file
            # and we want to copy the whole source file.
            hash_output_stream.write(b'rest')
            hash_output_stream.write(source_end_offset.to_bytes(8, 'big'))

        hash_output_stream.write(b'done')
        hash_output_stream.flush()


def do_retrieve(file_path, hash_input_stream, block_output_stream):
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
    - 4 bytes: command "done"
    '''
    if file_path == '-':
        sys.exit('ERROR (retrieve): file_path must be actual file or device, not `-`')

    if block_output_stream.isatty():
        sys.exit('ERROR (retrieve): block_output_stream is a tty - will not write binary data to terminal')

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
                with open(file_path, 'rb') as f:
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

                        logger.debug('Processing command: %r', command)

                        if command == b'done':
                            received_done = True
                            flush_hash_batch()
                            break

                        elif command == b'hash':
                            # This is the deprecated version of the hash commmand - does not contain
                            # block position.

                            block_size_b = hash_input_stream.read(4)
                            destination_hash = hash_input_stream.read(hash_digest_size)
                            if len(block_size_b) != 4 or len(destination_hash) != hash_digest_size:
                                raise IncompleteReadError('Incomplete read of hash from hash input stream')
                            block_size = int.from_bytes(block_size_b, 'big')
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
                            assert len(destination_hash) == hash_digest_size
                            if f.tell() != block_pos:
                                logger.debug('Seeking to %d', block_pos)
                                f.seek(block_pos)
                                assert f.tell() == block_pos
                            block_data = f.read(block_size)

                            if len(block_data) == block_size:
                                hash_batch.append((destination_hash, block_pos, block_data))
                            elif block_data:
                                # Probable just at end of source file while the destination file is larger.
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
                                    block_batch.append((block_pos, block_data))

                                if not block_batch:
                                    break

                                hash_result_event = Event()
                                hash_result_event.set()
                                send_queue.put((hash_result_event, [block_batch]))
                                del block_batch

                        else:
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
                                    to_send.append((block_pos, block_data))

                            hash_result_container.append(to_send)
                            hash_result_event.set()
                        except Exception as exc:
                            # Should not happen.
                            # If this happens, the send_worker thread could block on hash_result_event.wait().
                            logger.exception('do_retrieve hash_worker failed to process task: %r', exc)
                            exception_collector.collect_exception(exc)
                    finally:
                        hash_queue.task_done()
            except BaseException as exc:
                # not sure what the exception could be, but let's log it and re-raise it
                logger.exception('do_retrieve hash_worker failed (BaseException): %r', exc)
                exception_collector.collect_exception(exc)
                raise exc
            except Exception as exc:
                logger.exception('do_retrieve hash_worker failed: %r', exc)
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
                        to_send, = hash_result_container
                        with block_output_stream_lock:
                            for block_pos, block_data in to_send:
                                if exception_collector.has_exception():
                                    break
                                block_output_stream.write(b'data')
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
        sys.exit(f'ERROR (retrieve): {encountered_incomplete_read}')

    if not received_done:
        # This should not happen, becase that should already trigger the incomplete read exception.
        sys.exit('ERROR (retrieve): Received no done command from the checksum side')

    with block_output_stream_lock:
        block_output_stream.write(b'done')
        block_output_stream.flush()


def do_save(file_path, block_input_stream):
    '''
    Read blocks from block_input_stream and write them to the file.
    '''
    if file_path == '-':
        sys.exit('ERROR (save): file_path must be actual file or device, not `-`')

    try:
        received_done = False
        with open(file_path, 'r+b') as f:
            while True:
                command = block_input_stream.read(4)
                if not command:
                    raise IncompleteReadError('The block input stream was closed unexpectedly without receiving the `done` command')
                if len(command) != 4:
                    raise IncompleteReadError('Incomplete read of command from block input stream')
                if command == b'done':
                    received_done = True
                    break
                elif command == b'data':
                    block_pos_b = block_input_stream.read(8)
                    block_size_b = block_input_stream.read(4)
                    if len(block_pos_b) != 8 or len(block_size_b) != 4:
                        raise IncompleteReadError('Incomplete read of block position and size from block input stream')
                    block_pos = int.from_bytes(block_pos_b, 'big')
                    block_size = int.from_bytes(block_size_b, 'big')
                    block_data = block_input_stream.read(block_size)
                    if len(block_data) != block_size:
                        raise IncompleteReadError('Incomplete read of block data from block input stream')
                    f.seek(block_pos)
                    f.write(block_data)
                else:
                    raise Exception(f'Unknown command received: {command!r}')

        if not received_done:
            # Should not happen - should already trigger the incomplete read exception.
            sys.exit('ERROR (save): Received no done command from the retrieve side')
    except IncompleteReadError as exc:
        sys.exit(f'ERROR (save): {exc}')


if __name__ == "__main__":
    main()
