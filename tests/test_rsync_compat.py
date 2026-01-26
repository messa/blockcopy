'''
Tests for rsync compatibility.

These tests verify that after blockcopy sync with --truncate --times,
rsync should see no differences and transfer nothing.
'''

from contextlib import ExitStack
from os import chmod, environ, stat, utime
from subprocess import Popen, PIPE, run
from sys import executable

from pytest import fixture, skip


@fixture
def rsync_executable():
    '''Return path to rsync if available, skip test otherwise.'''
    from shutil import which
    p = which('rsync')
    if p is None:
        if environ.get('CI') or environ.get('GITHUB_ACTIONS'):
            raise RuntimeError('rsync is required for CI but not found in PATH')
        else:
            skip('rsync not available')
    return p


def test_rsync_sees_no_changes_after_blockcopy(tmp_path, script_path, rsync_executable):
    """
    After blockcopy sync with --truncate --times, rsync should see
    no differences and transfer nothing.
    """
    # Setup: create source file with specific content and metadata
    src_file = tmp_path / 'src.bin'

    test_content = b'Hello World! ' * 1000  # ~13KB
    src_file.write_bytes(test_content)

    # Set specific mtime (not "now" to avoid race conditions)
    # Use a time in the past with zero nanoseconds for clean comparison
    fixed_mtime_ns = 1700000000_000000000  # 2023-11-14 22:13:20 UTC
    utime(src_file, ns=(fixed_mtime_ns, fixed_mtime_ns))

    # Setup: create destination file (different content, will be overwritten)
    dst_file = tmp_path / 'dst.bin'
    dst_file.write_bytes(b'To be overwritten')

    # Run blockcopy pipeline with --truncate --times
    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(
            [executable, script_path, 'checksum', str(dst_file)],
            stdout=PIPE,
        ))
        p2 = stack.enter_context(Popen(
            [executable, script_path, 'retrieve', str(src_file)],
            stdin=p1.stdout,
            stdout=PIPE,
        ))
        p1.stdout.close()
        p3 = stack.enter_context(Popen(
            [executable, script_path, 'save', '--truncate', '--times', str(dst_file)],
            stdin=p2.stdout,
            stdout=PIPE,
        ))
        p2.stdout.close()

        p3.communicate()
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    # Verify content was synced
    assert dst_file.read_bytes() == test_content

    # Verify mtime matches (at least seconds)
    src_stat = stat(src_file)
    dst_stat = stat(dst_file)
    assert int(src_stat.st_mtime) == int(dst_stat.st_mtime), \
        f"mtime mismatch: src={src_stat.st_mtime}, dst={dst_stat.st_mtime}"

    # NOW: Run rsync --dry-run --itemize-changes and verify NO transfers
    result = run(
        [
            rsync_executable,
            '--dry-run',
            '--itemize-changes',  # Shows what would be transferred
            '-a',                 # Archive mode
            '--no-owner',         # Skip owner check (we didn't sync owner)
            '--no-group',         # Skip group check (we didn't sync group)
            '--no-perms',         # Skip perms check (we didn't sync perms)
            str(src_file),
            str(dst_file),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"rsync failed: {result.stderr}"

    # itemize-changes output should be empty if no transfer needed
    assert result.stdout.strip() == '', \
        f"rsync would transfer file! itemize output:\n{result.stdout}"


def test_rsync_sees_no_changes_with_full_metadata(tmp_path, script_path, rsync_executable):
    """
    After blockcopy sync with all metadata options, rsync -a should see
    no differences at all.
    """
    src_file = tmp_path / 'src.bin'

    test_content = b'Test content for full metadata sync'
    src_file.write_bytes(test_content)

    # Set specific metadata
    chmod(src_file, 0o644)
    fixed_mtime_ns = 1700000000_000000000
    utime(src_file, ns=(fixed_mtime_ns, fixed_mtime_ns))

    dst_file = tmp_path / 'dst.bin'
    dst_file.write_bytes(b'-' * len(test_content))
    chmod(dst_file, 0o640)

    # Run blockcopy with full metadata
    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(
            [executable, script_path, 'checksum', str(dst_file)],
            stdout=PIPE,
        ))
        p2 = stack.enter_context(Popen(
            [executable, script_path, 'retrieve', str(src_file)],
            stdin=p1.stdout,
            stdout=PIPE,
        ))
        p1.stdout.close()
        p3 = stack.enter_context(Popen(
            [executable, script_path, 'save',
             '--truncate', '--times', '--perms',
             str(dst_file)],
            stdin=p2.stdout,
            stdout=PIPE,
        ))
        p2.stdout.close()

        p3.communicate()
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    # Verify perms were synced
    src_stat = stat(src_file)
    dst_stat = stat(dst_file)
    assert dst_stat.st_mode == src_stat.st_mode, \
        f"mode mismatch: src={oct(src_stat.st_mode)}, dst={oct(dst_stat.st_mode)}"

    # Run rsync -a (without --no-owner/group since we're not root anyway)
    result = run(
        [
            rsync_executable,
            '--dry-run',
            '--itemize-changes',
            '-a',
            '--no-owner',  # Can't test owner without root
            '--no-group',  # Can't test group without root
            str(src_file),
            str(dst_file),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == '', \
        f"rsync would make changes! itemize output:\n{result.stdout}"


def test_rsync_with_nanoseconds_sees_no_changes(tmp_path, script_path, rsync_executable):
    """
    With rsync --modify-window=-1 (nanosecond comparison), blockcopy
    should still produce identical files because it preserves full
    nanosecond precision via os.utime(ns=...).
    """
    src_file = tmp_path / 'src.bin'
    dst_file = tmp_path / 'dst.bin'

    test_content = b'Nanosecond precision test'
    src_file.write_bytes(test_content)

    # Set mtime with non-zero nanoseconds
    mtime_ns = 1700000000_123456789  # Has nanosecond component
    utime(src_file, ns=(mtime_ns, mtime_ns))

    dst_file.write_bytes(b'X' * len(test_content))

    # Run blockcopy with --truncate --times
    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(
            [executable, script_path, 'checksum', str(dst_file)],
            stdout=PIPE,
        ))
        p2 = stack.enter_context(Popen(
            [executable, script_path, 'retrieve', str(src_file)],
            stdin=p1.stdout,
            stdout=PIPE,
        ))
        p1.stdout.close()
        p3 = stack.enter_context(Popen(
            [executable, script_path, 'save', '--truncate', '--times', str(dst_file)],
            stdin=p2.stdout,
            stdout=PIPE,
        ))
        p2.stdout.close()

        p3.communicate()
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    # Verify nanoseconds match exactly
    src_stat = stat(src_file)
    dst_stat = stat(dst_file)
    assert src_stat.st_mtime_ns == dst_stat.st_mtime_ns, \
        f"mtime_ns mismatch: src={src_stat.st_mtime_ns}, dst={dst_stat.st_mtime_ns}"

    # Run rsync with --modify-window=-1 (nanosecond comparison)
    result = run(
        [
            rsync_executable,
            '--dry-run',
            '--itemize-changes',
            '-a',
            '--modify-window=-1',  # Compare nanoseconds too!
            '--no-owner',  # Can't test owner without root
            '--no-group',  # Can't test group without root
            '--no-perms',
            str(src_file),
            str(dst_file),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"rsync failed: {result.stderr}"
    assert result.stdout.strip() == '', \
        f"rsync --modify-window=-1 would transfer! output:\n{result.stdout}"


def test_rsync_without_times_would_transfer(tmp_path, script_path, rsync_executable):
    """
    Negative test: without --times, rsync SHOULD see differences
    (because mtime won't match).
    """
    src_file = tmp_path / 'src.bin'
    dst_file = tmp_path / 'dst.bin'

    test_content = b'Test'
    src_file.write_bytes(test_content)

    # Set old mtime on source
    old_mtime_ns = 1600000000_000000000
    utime(src_file, ns=(old_mtime_ns, old_mtime_ns))

    dst_file.write_bytes(b'X' * len(test_content))

    # Run blockcopy WITHOUT --times
    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(
            [executable, script_path, 'checksum', str(dst_file)],
            stdout=PIPE,
        ))
        p2 = stack.enter_context(Popen(
            [executable, script_path, 'retrieve', str(src_file)],
            stdin=p1.stdout,
            stdout=PIPE,
        ))
        p1.stdout.close()
        p3 = stack.enter_context(Popen(
            [executable, script_path, 'save', '--truncate', str(dst_file)],
            stdin=p2.stdout,
            stdout=PIPE,
        ))
        p2.stdout.close()

        p3.communicate()
        assert p1.wait() == 0
        assert p2.wait() == 0
        assert p3.wait() == 0

    # rsync SHOULD see difference (mtime differs)
    result = run(
        [
            rsync_executable,
            '--dry-run',
            '--itemize-changes',
            '-a',
            '--no-owner',
            '--no-group',
            '--no-perms',
            str(src_file),
            str(dst_file),
        ],
        capture_output=True,
        text=True,
    )

    # Expect non-empty output (rsync would transfer)
    assert result.stdout.strip() != '', \
        "rsync should see mtime difference without --times!"
