from contextlib import ExitStack
from hashlib import sha3_512
from subprocess import Popen, PIPE, DEVNULL
from sys import executable


def test_checksum_file(tmp_path, script_path, terminate_process):
    test_content = b'Hello World!'
    assert len(test_content) == 12
    sample_path = tmp_path / 'src_file'
    sample_path.write_bytes(test_content)
    cmd1 = [executable, script_path, 'checksum', str(sample_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE, stderr=PIPE))
        stack.callback(lambda: terminate_process(p1))
        p1_output, p1_error = p1.communicate(input=test_content, timeout=5)
        assert p1.wait() == 0
        assert p1_error == b''
        assert p1_output == b''.join([
            b'Hash',
            b'\x00\x00\x00\x00\x00\x00\x00\x00', # block pos
            b'\x00\x00\x00\x0c', # block data length
            sha3_512(test_content).digest(), # block hash
            b'rest',
            b'\x00\x00\x00\x00\x00\x00\x00\x0c', # size of the sample file
            b'done',
        ])


def test_checksum_devstdin(tmp_path, script_path, terminate_process):
    test_content = b'Hello World!'
    assert len(test_content) == 12
    sample_path = tmp_path / 'src_file'
    sample_path.write_bytes(test_content)
    cmd = [executable, script_path, 'checksum', '/dev/stdin']

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        stack.callback(lambda: terminate_process(p1))
        p1_output, p1_error = p1.communicate(input=test_content, timeout=5)
        assert p1.wait() == 0
        assert p1_error == b''
        assert p1_output == b''.join([
            b'Hash',
            b'\x00\x00\x00\x00\x00\x00\x00\x00', # block pos
            b'\x00\x00\x00\x0c', # block data length
            sha3_512(test_content).digest(), # block hash
            b'done',
        ])


def test_checksum_stdin(tmp_path, script_path, terminate_process):
    test_content = b'Hello World!'
    assert len(test_content) == 12
    sample_path = tmp_path / 'src_file'
    sample_path.write_bytes(test_content)
    cmd = [executable, script_path, 'checksum', '-']

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        stack.callback(lambda: terminate_process(p1))
        p1_output, p1_error = p1.communicate(input=test_content, timeout=5)
        assert p1.wait() == 0
        assert p1_error == b''
        assert p1_output == b''.join([
            b'Hash',
            b'\x00\x00\x00\x00\x00\x00\x00\x00', # block pos
            b'\x00\x00\x00\x0c', # block data length
            sha3_512(test_content).digest(), # block hash
            b'done',
        ])


def test_checksum_file_progress(tmp_path, script_path, terminate_process):
    test_content = b'Hello World!'
    assert len(test_content) == 12
    sample_path = tmp_path / 'src_file'
    sample_path.write_bytes(test_content)
    cmd1 = [executable, script_path, 'checksum', '--progress', str(sample_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE, stderr=PIPE))
        stack.callback(lambda: terminate_process(p1))
        p1_output, p1_error = p1.communicate(input=test_content, timeout=5)
        assert p1.wait() == 0
        assert p1_output == b''.join([
            b'Hash',
            b'\x00\x00\x00\x00\x00\x00\x00\x00', # block pos
            b'\x00\x00\x00\x0c', # block data length
            sha3_512(test_content).digest(), # block hash
            b'rest',
            b'\x00\x00\x00\x00\x00\x00\x00\x0c', # size of the sample file
            b'done',
        ])


def test_checksum_devstdin_progress(tmp_path, script_path, terminate_process):
    test_content = b'Hello World!'
    assert len(test_content) == 12
    sample_path = tmp_path / 'src_file'
    sample_path.write_bytes(test_content)
    cmd = [executable, script_path, 'checksum', '--progress', '/dev/stdin']

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        stack.callback(lambda: terminate_process(p1))
        p1_output, p1_error = p1.communicate(input=test_content, timeout=5)
        assert p1.wait() == 0
        assert p1_output == b''.join([
            b'Hash',
            b'\x00\x00\x00\x00\x00\x00\x00\x00', # block pos
            b'\x00\x00\x00\x0c', # block data length
            sha3_512(test_content).digest(), # block hash
            b'done',
        ])


def test_checksum_stdin_progress(tmp_path, script_path, terminate_process):
    test_content = b'Hello World!'
    assert len(test_content) == 12
    sample_path = tmp_path / 'src_file'
    sample_path.write_bytes(test_content)
    cmd = [executable, script_path, 'checksum', '--progress', '-']

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        stack.callback(lambda: terminate_process(p1))
        p1_output, p1_error = p1.communicate(input=test_content, timeout=5)
        assert p1.wait() == 0
        assert p1_output == b''.join([
            b'Hash',
            b'\x00\x00\x00\x00\x00\x00\x00\x00', # block pos
            b'\x00\x00\x00\x0c', # block data length
            sha3_512(test_content).digest(), # block hash
            b'done',
        ])
