uv=uv

default: check

check:
	$(uv) run pytest -s -vv tests $(pytest_args)

lint:
	$(uv) run flake8
