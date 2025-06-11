python=python3

default: check

check:
	$(python) -m pytest -s -vv tests $(pytest_args)

lint:
	$(python) -m flake8
