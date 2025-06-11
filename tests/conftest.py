from pathlib import Path
from pytest import fixture


@fixture
def script_path():
    p = Path(__file__).resolve().parent.parent / 'blockcopy.py'
    assert p.is_file()
    return p


@fixture
def terminate_process():
    '''
    Usage:

        with ExitStack() as stack:
            p1 = stack.enter_context(Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE))
            stack.callback(lambda: terminate_process(p1))
    '''
    def do_terminate_process(process):
        if process.poll() is None:
            print('Terminating process', process)
            process.terminate()
            process.wait()
    return do_terminate_process
