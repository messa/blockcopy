python=python3

default: check

check:
	$(python) -m pytest -sv tests $(pytest_args)
