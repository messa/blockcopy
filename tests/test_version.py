from contextlib import ExitStack
from subprocess import Popen, PIPE, DEVNULL
from sys import executable


def test_version(tmp_path, script_path, terminate_process):
    cmd1 = [executable, script_path, '--version']
    with ExitStack() as stack:
        p1 = stack.enter_context(Popen(cmd1, stdin=DEVNULL, stdout=PIPE, stderr=PIPE))
        stack.callback(lambda: terminate_process(p1))
        p1_output, p1_error = p1.communicate(input=None, timeout=5)
        assert p1.wait() == 0
        assert p1_error == b''
        assert p1_output == b'blockcopy 0.0.2\n'
