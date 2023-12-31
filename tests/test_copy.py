from contextlib import ExitStack
from pathlib import Path
from pytest import fixture
import subprocess
from subprocess import Popen, PIPE, DEVNULL


@fixture
def script_path():
    return Path(__file__).resolve().parent.parent / 'blockcopy.py'


def test_help(script_path):
    cmd = [script_path, '--help']
    assert subprocess.run(cmd, stdout=DEVNULL, stderr=DEVNULL).returncode == 0


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
