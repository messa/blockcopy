from contextlib import ExitStack
import subprocess
from subprocess import Popen, PIPE, DEVNULL
from sys import executable


def test_help(script_path):
    cmd = [script_path, '--help']
    assert subprocess.run(cmd, stdout=DEVNULL, stderr=DEVNULL).returncode == 0


def test_copy_tiny(tmp_path, script_path):
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', str(src_path)]
    cmd3 = [executable, script_path, 'save', str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p1_output = p1.stdout.read()
        assert p1.wait() == 0
        assert p1_output == (
            b'Hash'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c.Ja|\xde-\x00G'
            b'\xb1\xc3\xce\xb7)\xf2gGlXF\xfbG\xde\xdb+\xd0\xa6o\x13\x83z?%2\xfd\xa5\x06'
            b'^\xb0z\xd1\xdc}\xd5\xa09>\xa5\xa3\xa3hr\xe1_T\xb3\x1c\xfe<\x92\xbc'
            b'j\xa7\xa4\x83'
            b'rest'
            b'\x00\x00\x00\x00\x00\x00\x00\x0c'
            b'done'
        )

        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 0
        assert p2_output == b'data\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0cHello World!done'

        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE, stdout=PIPE))
        p3_output, _ = p3.communicate(input=p2_output, timeout=5)
        assert p3.wait() == 0
        assert p3_output == b''

    assert dst_path.read_bytes() == test_content


def test_copy_tiny_incomplete_hash_stream(tmp_path, script_path):
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))

    cmd1 = [executable, script_path, '-v', 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, '-v', 'retrieve', str(src_path)]
    cmd3 = [executable, script_path, '-v', 'save', str(dst_path)]

    with ExitStack() as stack:
        print('Running: ', cmd1)
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p1_output = p1.stdout.read()
        assert p1.wait() == 0
        assert p1_output == (
            b'Hash'
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0c.Ja|\xde-\x00G'
            b'\xb1\xc3\xce\xb7)\xf2gGlXF\xfbG\xde\xdb+\xd0\xa6o\x13\x83z?%2\xfd\xa5\x06'
            b'^\xb0z\xd1\xdc}\xd5\xa09>\xa5\xa3\xa3hr\xe1_T\xb3\x1c\xfe<\x92\xbc'
            b'j\xa7\xa4\x83'
            b'rest'
            b'\x00\x00\x00\x00\x00\x00\x00\x0c'
            b'done'
        )
        # remove the `done` command
        p1_output = p1_output[:-4]

        print('Running: ', cmd2)
        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 1
        assert p2_output == b'data\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0cHello World!'

        print('Running: ', cmd3)
        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE, stdout=PIPE))
        p3_output, _ = p3.communicate(input=p2_output, timeout=5)
        assert p3.wait() == 1
        assert p3_output == b''

    assert dst_path.read_bytes() == test_content



def test_copy(tmp_path, script_path):
    test_content = b'Test content.' * 1024000
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))

    cmd1 = [script_path, 'checksum', str(dst_path)]
    cmd2 = [script_path, 'retrieve', str(src_path)]
    cmd3 = [script_path, 'save', str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout))
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    assert dst_path.read_bytes() == test_content


def test_copy_to_larger_file_tiny(tmp_path, script_path):
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content) + b'extra')

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', str(src_path)]
    cmd3 = [executable, script_path, 'save', str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p1_output = p1.stdout.read()
        assert p1.wait() == 0
        assert p1_output == (
            b'Hash'
            b'\x00\x00\x00\x00\x00\x00\x00\x00'
            b'\x00\x00\x00\x11'
            b'Q\x00\x01\xd0\xb0G\x0c<'
            b'\xc08\xc1\x8as\x05\xcc\x1c\xbb\xb1\xd5\xc2;\x0b\xa56\x8c\xe8uf\\\r\xebd'
            b'\xc4\r\x1aQ\x84\xc0f\xa2s4\x8a\xd6\x7f\x000\xca\x18)\\\xb6`X\x11\x12'
            b'\xad\xc6\x07\x94=,\x80j'
            b'rest'
            b'\x00\x00\x00\x00\x00\x00\x00\x11'
            b'done'
        )

        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 0
        assert p2_output == b'data\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0cHello World!done'

        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE, stdout=PIPE))
        p3_output, _ = p3.communicate(input=p2_output, timeout=5)
        assert p3.wait() == 0
        assert p3_output == b''

    assert dst_path.read_bytes() == test_content + b'extra'


def test_copy_to_larger_file(tmp_path, script_path):
    '''
    This tests simulates copying from smaller to larger device or file.
    Contents of the destination file is replaced with the source file contents.
    Contents of the destination file beyond the size of the source file is not changed.
    '''
    test_content = b'Test content.' * 1024000
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content) + b'extra')

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', str(src_path)]
    cmd3 = [executable, script_path, 'save', str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout))
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    assert dst_path.read_bytes() == test_content + b'extra'


def test_copy_identical(tmp_path, script_path):
    '''
    This tests checks that no actual data is copied when source and destination are identical.
    '''
    test_content = b'Test content.' * 1024000
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(test_content)

    cmd1 = [script_path, 'checksum', str(dst_path)]
    cmd2 = [script_path, 'retrieve', str(src_path)]
    cmd3 = [script_path, 'save', str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE))

        p2_output = p2.stdout.read()
        p3.stdin.write(p2_output)
        p3.stdin.flush()

        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

        # retrieve output should be trivial
        assert p2_output == b'done'

    assert dst_path.read_bytes() == test_content


def test_copy_to_smaller_file(tmp_path, script_path):
    '''
    This tests simulates copying from larger source file to smaller destination file.
    This could happend for example when previous copy was interrupted.
    It is expected that the destination file will become equal to the source file,
    including extending the destination file size.
    '''
    test_content = b'Test content.' * 1024000
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * (len(test_content) // 2))

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', str(src_path)]
    cmd3 = [executable, script_path, 'save', str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout))
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    assert dst_path.read_bytes() == test_content


def test_copy_start_offset_tiny(tmp_path, script_path):
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))

    cmd1 = [executable, script_path, 'checksum', str(dst_path), '--start', str(5)]
    cmd2 = [executable, script_path, 'retrieve', str(src_path)]
    cmd3 = [executable, script_path, 'save', str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p1_output = p1.stdout.read()
        assert p1.wait() == 0
        assert p1_output == (
            b'Hash'
            b'\x00\x00\x00\x00\x00\x00\x00\x05'
            b'\x00\x00\x00\x07'
            b'\xd7\x8c\x18\x90'
            b'\x07\xf5\'\xc8\xc8nK\xd6:\x9d\xbc \x10\x91\xcd7(^\x99\xaa\x062\x80X.KH['
            b'\tl\x0f\xe2\x0e\xea\xfd\x1f\x16\xc6\xd8\x1e\t\xa5H\x80~\x12Z\xb7'
            b'\x91\xef\x18\x8a%\xb8\xa2\x10Bl\xb2\x7f'
            b'rest'
            b'\x00\x00\x00\x00\x00\x00\x00\x0c'
            b'done'
        )

        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 0
        assert p2_output == (
            b'data'
            b'\x00\x00\x00\x00\x00\x00\x00\x05'
            b'\x00\x00\x00\x07'
            b' World!'
            b'done'
        )

        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE, stdout=PIPE))
        p3_output, _ = p3.communicate(input=p2_output, timeout=5)
        assert p3.wait() == 0
        assert p3_output == b''

    assert dst_path.read_bytes() == b'-' * 5 + test_content[5:]
