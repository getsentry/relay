venv/bin/python: setup.py
	rm -rf venv
	virtualenv venv
	venv/bin/pip install --upgrade pip
	SEMAPHORE_DEBUG=1 venv/bin/pip install -v --editable .
	venv/bin/pip install pytest

venv: venv/bin/python

test: venv
	@venv/bin/pytest tests

.PHONY: venv test
