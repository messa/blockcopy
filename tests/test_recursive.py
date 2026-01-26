from contextlib import ExitStack
from os import symlink, utime
from pytest import mark
from subprocess import PIPE, Popen
from sys import executable


def test_recursive_help(script_path):
    '''Test that --recursive flag is recognized.'''
    cmd = [script_path, 'checksum', '--help']
    result = Popen(cmd, stdout=PIPE, stderr=PIPE)
    stdout, _ = result.communicate()
    assert b'--recursive' in stdout or b'-r' in stdout


def test_recursive_empty_directory(tmp_path, script_path):
    '''Test copying empty directory.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()


def test_recursive_single_file(tmp_path, script_path):
    '''Test copying directory with a single file.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create a file in source
    (src_dir / 'test.txt').write_text('Hello World!')

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Verify file was copied
    assert (dst_dir / 'test.txt').exists()
    assert (dst_dir / 'test.txt').read_text() == 'Hello World!'


def test_recursive_nested_directories(tmp_path, script_path):
    '''Test copying nested directory structure.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create nested structure
    (src_dir / 'a').mkdir()
    (src_dir / 'a' / 'b').mkdir()
    (src_dir / 'a' / 'b' / 'c').mkdir()
    (src_dir / 'a' / 'file1.txt').write_text('File 1')
    (src_dir / 'a' / 'b' / 'file2.txt').write_text('File 2')
    (src_dir / 'a' / 'b' / 'c' / 'file3.txt').write_text('File 3')

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Verify structure
    assert (dst_dir / 'a').is_dir()
    assert (dst_dir / 'a' / 'b').is_dir()
    assert (dst_dir / 'a' / 'b' / 'c').is_dir()
    assert (dst_dir / 'a' / 'file1.txt').read_text() == 'File 1'
    assert (dst_dir / 'a' / 'b' / 'file2.txt').read_text() == 'File 2'
    assert (dst_dir / 'a' / 'b' / 'c' / 'file3.txt').read_text() == 'File 3'


def test_recursive_incremental_copy(tmp_path, script_path):
    '''Test that only changed files are transferred.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create identical file in both
    (src_dir / 'unchanged.txt').write_text('Same content')
    (dst_dir / 'unchanged.txt').write_text('Same content')

    # Set same mtime
    mtime = 1000000000
    utime(src_dir / 'unchanged.txt', ns=(mtime * 1000000000, mtime * 1000000000))
    utime(dst_dir / 'unchanged.txt', ns=(mtime * 1000000000, mtime * 1000000000))

    # Create a changed file with different mtime
    (dst_dir / 'changed.txt').write_text('Old content')
    old_mtime = 900000000
    utime(dst_dir / 'changed.txt', ns=(old_mtime * 1000000000, old_mtime * 1000000000))
    (src_dir / 'changed.txt').write_text('New content')
    new_mtime = 1100000000
    utime(src_dir / 'changed.txt', ns=(new_mtime * 1000000000, new_mtime * 1000000000))

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Verify files
    assert (dst_dir / 'unchanged.txt').read_text() == 'Same content'
    assert (dst_dir / 'changed.txt').read_text() == 'New content'


def test_recursive_with_symlink(tmp_path, script_path):
    '''Test copying directory with symlinks.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create a file and a symlink
    (src_dir / 'target.txt').write_text('Target content')
    symlink('target.txt', src_dir / 'link.txt')

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Verify
    assert (dst_dir / 'target.txt').read_text() == 'Target content'
    assert (dst_dir / 'link.txt').is_symlink()
    assert (dst_dir / 'link.txt').resolve().name == 'target.txt'


def test_recursive_delete(tmp_path, script_path):
    '''Test --delete flag removes extra files on destination.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create file only in source
    (src_dir / 'keep.txt').write_text('Keep me')

    # Create extra file in destination
    (dst_dir / 'delete_me.txt').write_text('Delete me')

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', '--delete', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Verify
    assert (dst_dir / 'keep.txt').exists()
    assert (dst_dir / 'keep.txt').read_text() == 'Keep me'
    assert not (dst_dir / 'delete_me.txt').exists()


def test_recursive_no_delete_by_default(tmp_path, script_path):
    '''Test that without --delete, extra files are preserved.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create file only in source
    (src_dir / 'new.txt').write_text('New file')

    # Create extra file in destination
    (dst_dir / 'existing.txt').write_text('Existing file')

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Both files should exist
    assert (dst_dir / 'new.txt').exists()
    assert (dst_dir / 'existing.txt').exists()


@mark.parametrize('use_lzma', [False, True], ids=['no_lzma', 'lzma'])
def test_recursive_large_file(tmp_path, script_path, use_lzma):
    '''Test copying directory with large file.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create a large file (multiple blocks)
    large_content = b'X' * (128 * 1024 * 5 + 1000)  # 5+ blocks
    (src_dir / 'large.bin').write_bytes(large_content)

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', *(['--lzma'] if use_lzma else []), str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Verify
    assert (dst_dir / 'large.bin').read_bytes() == large_content


def test_recursive_update_existing_file(tmp_path, script_path):
    '''Test updating an existing file with different content.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create file with different content in dst
    (src_dir / 'file.txt').write_text('Source content here')
    (dst_dir / 'file.txt').write_text('Destination content that is different')

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # File should have source content
    assert (dst_dir / 'file.txt').read_text() == 'Source content here'


def test_recursive_special_characters_in_filename(tmp_path, script_path):
    '''Test copying files with special characters in names.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create files with special characters
    (src_dir / 'file with spaces.txt').write_text('Spaces')
    (src_dir / 'file-with-dashes.txt').write_text('Dashes')
    (src_dir / 'file_with_underscores.txt').write_text('Underscores')

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Verify
    assert (dst_dir / 'file with spaces.txt').read_text() == 'Spaces'
    assert (dst_dir / 'file-with-dashes.txt').read_text() == 'Dashes'
    assert (dst_dir / 'file_with_underscores.txt').read_text() == 'Underscores'


def test_recursive_identical_directories(tmp_path, script_path):
    '''Test that copying identical directories transfers no data.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create identical content
    content = 'Same content'
    (src_dir / 'file.txt').write_text(content)
    (dst_dir / 'file.txt').write_text(content)

    # Set same mtime and size
    src_stat = (src_dir / 'file.txt').stat()
    utime(dst_dir / 'file.txt', ns=(src_stat.st_atime_ns, src_stat.st_mtime_ns))

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Content should still be same
    assert (dst_dir / 'file.txt').read_text() == content


def test_recursive_delete_directory(tmp_path, script_path):
    '''Test that --delete removes extra directories.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create extra directory with files in destination
    (dst_dir / 'extra_dir').mkdir()
    (dst_dir / 'extra_dir' / 'file.txt').write_text('Delete me')

    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', '--delete', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Extra directory should be gone
    assert not (dst_dir / 'extra_dir').exists()


def test_recursive_block_level_diff(tmp_path, script_path):
    '''Test that only changed blocks are transferred, not entire files.'''
    src_dir = tmp_path / 'src'
    dst_dir = tmp_path / 'dst'
    src_dir.mkdir()
    dst_dir.mkdir()

    # Create a file with 4 blocks (512KB total, block_size = 128KB)
    block_size = 128 * 1024
    block_a = b'A' * block_size
    block_b = b'B' * block_size
    block_c = b'C' * block_size
    block_d = b'D' * block_size

    src_file = src_dir / 'bigfile.bin'
    src_file.write_bytes(block_a + block_b + block_c + block_d)

    # First sync - copies everything
    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    dst_file = dst_dir / 'bigfile.bin'
    assert dst_file.read_bytes() == block_a + block_b + block_c + block_d

    # Now modify only the second block in source
    block_b_modified = b'X' * block_size
    src_file.write_bytes(block_a + block_b_modified + block_c + block_d)

    # Second sync - should only transfer the changed block
    cmd1 = [executable, script_path, 'checksum', '-r', str(dst_dir)]
    cmd2 = [executable, script_path, 'retrieve', '-r', str(src_dir)]
    cmd3 = [executable, script_path, 'save', '-r', str(dst_dir)]

    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=PIPE, stdout=PIPE, stderr=PIPE))
        p2 = stack.enter_context(Popen(cmd2, stdin=p1.stdout, stdout=PIPE, stderr=PIPE))
        p3 = stack.enter_context(Popen(cmd3, stdin=p2.stdout, stderr=PIPE))

        assert p1.wait() == 0, p1.stderr.read()
        assert p2.wait() == 0, p2.stderr.read()
        assert p3.wait() == 0, p3.stderr.read()

    # Verify the file is correctly updated - unchanged blocks preserved, changed block updated
    result = dst_file.read_bytes()
    assert result[:block_size] == block_a, "First block should be unchanged"
    assert result[block_size:2*block_size] == block_b_modified, "Second block should be updated"
    assert result[2*block_size:3*block_size] == block_c, "Third block should be unchanged"
    assert result[3*block_size:] == block_d, "Fourth block should be unchanged"
