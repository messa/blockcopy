uv=uv

default: check

check:
	$(uv) run pytest -vv tests $(pytest_args)

lint:
	$(uv) run flake8
