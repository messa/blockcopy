from contextlib import ExitStack
from os import environ
from pytest import mark
from subprocess import DEVNULL, PIPE, Popen, run
from sys import executable


# Environment variables for deterministic metadata in byte-level tests
DETERMINISTIC_META_ENV = {
    'BLOCKCOPY_OVERRIDE_ATIME_NS': '0',
    'BLOCKCOPY_OVERRIDE_MTIME_NS': '0',
    'BLOCKCOPY_OVERRIDE_MODE': '33188',  # 0o100644
    'BLOCKCOPY_OVERRIDE_UID': '1000',
    'BLOCKCOPY_OVERRIDE_GID': '1000',
    'BLOCKCOPY_OVERRIDE_OWNER': 'testuser',
    'BLOCKCOPY_OVERRIDE_GROUP': 'testgroup',
}


# Expected Meta command bytes with deterministic values:
# meta(4) + atime_ns(8) + mtime_ns(8) + mode(4) + uid(4) + gid(4) +
# owner_len(2) + owner(8) + group_len(2) + group(9) + total_size(8) + end(3) = 64 bytes
def expected_meta_bytes(total_size):
    return (
        b'meta'
        b'\x00\x00\x00\x00\x00\x00\x00\x00'  # atime_ns = 0
        b'\x00\x00\x00\x00\x00\x00\x00\x00'  # mtime_ns = 0
        b'\x00\x00\x81\xa4'                  # mode = 33188 (0x81a4)
        b'\x00\x00\x03\xe8'                  # uid = 1000
        b'\x00\x00\x03\xe8'                  # gid = 1000
        b'\x00\x08' b'testuser'              # owner_name
        b'\x00\x09' b'testgroup'             # group_name
        + total_size.to_bytes(8, 'big')      # total_size
        + b'end'                             # end marker
    )


def test_help(script_path):
    cmd = [script_path, '--help']
    assert run(cmd, stdout=DEVNULL, stderr=DEVNULL).returncode == 0


@mark.parametrize('use_truncate', [False, True], ids=['no_truncate', 'truncate'])
@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
@mark.parametrize('skip_sending_meta', [False, True], ids=['sending_meta', 'skip_sending_meta'])
def test_copy_tiny(tmp_path, script_path, use_truncate, use_lzma, skip_sending_meta):
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))

    env = {
        **environ,
        **DETERMINISTIC_META_ENV,
    }
    if skip_sending_meta:
        env['SKIP_SENDING_META'] = '1'

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', *(['--lzma'] if use_lzma else []), str(src_path)]
    cmd3 = [executable, script_path, 'save', *(['--truncate'] if use_truncate else []), str(dst_path)]

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

        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE, env=env))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 0
        assert p2_output == (
            b'data\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0cHello World!'
            + (b'' if skip_sending_meta else expected_meta_bytes(12))
            + b'done'
        )

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

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', str(src_path)]
    cmd3 = [executable, script_path, 'save', str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE, stderr=PIPE))
        p1_output, _ = p1.communicate(timeout=5)
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

        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 1
        assert p2_output == b'data\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0cHello World!'

        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p3_output, _ = p3.communicate(input=p2_output, timeout=5)
        assert p3.wait() == 1
        assert p3_output == b''

    assert dst_path.read_bytes() == test_content



@mark.parametrize('use_truncate', [False, True], ids=['no_truncate', 'truncate'])
@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
@mark.parametrize('skip_sending_meta', [False, True], ids=['sending_meta', 'skip_sending_meta'])
def test_copy(tmp_path, script_path, use_truncate, use_lzma, skip_sending_meta):
    test_content = b'Test content.' * 1024000
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))
    assert dst_path.read_bytes() != test_content

    env = dict(environ)
    if skip_sending_meta:
        env['SKIP_SENDING_META'] = '1'

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', *(['--lzma'] if use_lzma else []), str(src_path)]
    cmd3 = [executable, script_path, 'save', *(['--truncate'] if use_truncate else []), str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE, env=env))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, env=env))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, env=env))
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    assert src_path.read_bytes() == test_content
    assert dst_path.read_bytes() == test_content


@mark.parametrize('use_truncate', [False, True], ids=['no_truncate', 'truncate'])
@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
def test_copy_to_larger_file_tiny(tmp_path, script_path, use_truncate, use_lzma):
    '''
    Copying from smaller source file to larger destination file.

    Without truncate: the destination file size remains the same.
    With truncate: the destination file size will be equal to the source file.
    '''
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content) + b'extra')

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', *(['--lzma'] if use_lzma else []), str(src_path)]
    cmd3 = [executable, script_path, 'save', *(['--truncate'] if use_truncate else []), str(dst_path)]

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

        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE, env={**environ, **DETERMINISTIC_META_ENV}))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 0
        assert p2_output == (
            b'data\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0cHello World!'
            + expected_meta_bytes(12)
            + b'done'
        )

        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE, stdout=PIPE))
        p3_output, _ = p3.communicate(input=p2_output, timeout=5)
        assert p3.wait() == 0
        assert p3_output == b''

    if use_truncate:
        assert dst_path.read_bytes() == test_content
    else:
        assert dst_path.read_bytes() == test_content + b'extra'


@mark.parametrize('use_truncate', [False, True], ids=['no_truncate', 'truncate'])
@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
def test_copy_to_larger_file(tmp_path, script_path, use_truncate, use_lzma):
    '''
    This test simulates copying from smaller to larger device or file.
    Contents of the destination file is replaced with the source file contents.
    Contents of the destination file beyond the size of the source file is not changed.
    '''
    test_content = b'Test content.' * 1024000
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content) + b'extra')

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', *(['--lzma'] if use_lzma else []), str(src_path)]
    cmd3 = [executable, script_path, 'save', *(['--truncate'] if use_truncate else []), str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout))
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    if use_truncate:
        assert dst_path.read_bytes() == test_content
    else:
        assert dst_path.read_bytes() == test_content + b'extra'


@mark.parametrize('use_truncate', [False, True], ids=['no_truncate', 'truncate'])
@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
def test_copy_identical(tmp_path, script_path, use_truncate, use_lzma):
    '''
    This tests checks that no actual data is copied when source and destination are identical.
    '''
    test_content = b'Test content.' * 1024000
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(test_content)

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', *(['--lzma'] if use_lzma else []), str(src_path)]
    cmd3 = [executable, script_path, 'save', *(['--truncate'] if use_truncate else []), str(dst_path)]

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

        # retrieve output should be trivial - only meta and done, no data blocks
        # Check that output starts with meta and ends with done
        assert p2_output[:4] == b'meta'
        assert p2_output[-4:] == b'done'
        # No 'data' blocks should be present
        assert b'data' not in p2_output

    assert src_path.read_bytes() == test_content
    assert dst_path.read_bytes() == test_content


@mark.parametrize('use_truncate', [False, True], ids=['no_truncate', 'truncate'])
@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
def test_copy_to_smaller_file(tmp_path, script_path, use_truncate, use_lzma):
    '''
    This test simulates copying from larger source file to smaller destination file.
    This could happen for example when previous copy was interrupted.
    It is expected that the destination file will become equal to the source file,
    including extending the destination file size.
    '''
    test_content = b'Test content.' * 1024000
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * (len(test_content) // 2))

    cmd1 = [executable, script_path, 'checksum', str(dst_path)]
    cmd2 = [executable, script_path, 'retrieve', *(['--lzma'] if use_lzma else []), str(src_path)]
    cmd3 = [executable, script_path, 'save', *(['--truncate'] if use_truncate else []), str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout))
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    assert dst_path.read_bytes() == test_content


@mark.parametrize('use_truncate', [False, True], ids=['no_truncate', 'truncate'])
@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
def test_copy_start_offset_tiny(tmp_path, script_path, use_truncate, use_lzma):
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))

    cmd1 = [executable, script_path, 'checksum', str(dst_path), '--start', str(5)]
    cmd2 = [executable, script_path, 'retrieve', *(['--lzma'] if use_lzma else []), str(src_path)]
    cmd3 = [executable, script_path, 'save', *(['--truncate'] if use_truncate else []), str(dst_path)]

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

        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE, env={**environ, **DETERMINISTIC_META_ENV}))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 0
        assert p2_output == (
            b'data'
            b'\x00\x00\x00\x00\x00\x00\x00\x05'
            b'\x00\x00\x00\x07'
            b' World!'
            + expected_meta_bytes(12)
            + b'done'
        )

        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE, stdout=PIPE))
        p3_output, _ = p3.communicate(input=p2_output, timeout=5)
        assert p3.wait() == 0
        assert p3_output == b''

    assert dst_path.read_bytes() == b'-' * 5 + test_content[5:]
    assert dst_path.read_bytes() == b'----- World!'


@mark.parametrize('use_truncate', [False, True], ids=['no_truncate', 'truncate'])
@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
def test_copy_end_offset_tiny(tmp_path, script_path, use_truncate, use_lzma):
    '''
    Test copying with --end offset - only copies up to specified byte offset.
    This is useful for partial copies or resumable transfers.
    '''
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))

    # Only copy first 5 bytes
    cmd1 = [executable, script_path, 'checksum', str(dst_path), '--end', str(5)]
    cmd2 = [executable, script_path, 'retrieve', *(['--lzma'] if use_lzma else []), str(src_path)]
    cmd3 = [executable, script_path, 'save', *(['--truncate'] if use_truncate else []), str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p1_output = p1.stdout.read()
        assert p1.wait() == 0
        # With --end, there should be no 'rest' command
        assert b'rest' not in p1_output
        # Check structure: Hash(4) + pos(8) + size(4) + hash(64) + done(4) = 84 bytes
        assert len(p1_output) == 84
        assert p1_output[:4] == b'Hash'
        assert p1_output[4:12] == b'\x00\x00\x00\x00\x00\x00\x00\x00'  # block pos = 0
        assert p1_output[12:16] == b'\x00\x00\x00\x05'  # block size = 5
        assert p1_output[-4:] == b'done'

        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE, env={**environ, **DETERMINISTIC_META_ENV}))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 0
        # Should have data block for "Hello" (5 bytes)
        assert p2_output[:4] == b'data'
        assert p2_output[4:12] == b'\x00\x00\x00\x00\x00\x00\x00\x00'  # block pos = 0
        assert p2_output[12:16] == b'\x00\x00\x00\x05'  # block size = 5
        assert p2_output[16:21] == b'Hello'

        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE, stdout=PIPE))
        p3_output, _ = p3.communicate(input=p2_output, timeout=5)
        assert p3.wait() == 0
        assert p3_output == b''

    # Only first 5 bytes should be copied, rest remains unchanged
    # truncate has no effect here since meta contains size=12 (source file size)
    # and we only wrote to first 5 bytes
    if use_truncate:
        # With truncate, file is truncated to source size (12 bytes)
        assert dst_path.read_bytes() == b'Hello-------'
    else:
        assert dst_path.read_bytes() == b'Hello-------'


@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
def test_copy_start_and_end_offset_tiny(tmp_path, script_path, use_lzma):
    '''
    Test copying with both --start and --end offsets.
    Only the specified range is checksummed and potentially copied.
    '''
    test_content = b'Hello World!'
    src_path = tmp_path / 'src_file'
    src_path.write_bytes(test_content)
    dst_path = tmp_path / 'dst_file'
    dst_path.write_bytes(b'-' * len(test_content))

    # Only process bytes 6-11 ("World" without "!")
    cmd1 = [executable, script_path, 'checksum', str(dst_path), '--start', '6', '--end', '11']
    cmd2 = [executable, script_path, 'retrieve', *(['--lzma'] if use_lzma else []), str(src_path)]
    cmd3 = [executable, script_path, 'save', str(dst_path)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE))
        p1_output = p1.stdout.read()
        assert p1.wait() == 0
        # With --end, there should be no 'rest' command
        assert b'rest' not in p1_output

        p2 = stack.enter_context(Popen(cmd2, stdin=PIPE, stdout=PIPE, env={**environ, **DETERMINISTIC_META_ENV}))
        p2_output, _ = p2.communicate(input=p1_output, timeout=5)
        assert p2.wait() == 0
        # Should have data block starting at position 6
        assert p2_output[:4] == b'data'
        assert p2_output[4:12] == b'\x00\x00\x00\x00\x00\x00\x00\x06'  # block pos = 6
        assert p2_output[12:16] == b'\x00\x00\x00\x05'  # block size = 5
        assert p2_output[16:21] == b'World'

        p3 = stack.enter_context(Popen(cmd3, stdin=PIPE, stdout=PIPE))
        p3_output, _ = p3.communicate(input=p2_output, timeout=5)
        assert p3.wait() == 0
        assert p3_output == b''

    # Only bytes 6-10 should be copied, rest remains unchanged
    assert dst_path.read_bytes() == b'------World-'
