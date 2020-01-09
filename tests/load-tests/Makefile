SHELL=/bin/bash
export PYTHON_VERSION := python3

all: config msg
.PHONY: all

msg:
	@echo
	@echo
	@echo ---------------------------------------------------
	@echo type '`make about`' for a description of the project
	@echo ---------------------------------------------------
	@echo
	@echo
.PHONY: msg

about:
	less readme.txt
.PHONY: about

config: setup-deps setup-brew
.PHONY: config

fake-sentry: setup-deps
	.venv/bin/python -m relay_load_tests.fake_sentry

load-test: setup-deps
	.venv/bin/locust -f locustfile.py

setup-brew:
	brew bundle
.PHONY: setup-brew

setup-deps: setup-venv
	.venv/bin/pip install -U -r requirements.txt
.PHONY: setup-python-deps

setup-venv: .venv/bin/python
.PHONY: setup-venv

.venv/bin/python: Makefile
	@rm -rf .venv
	@which virtualenv || sudo easy_install virtualenv
	virtualenv -p $$PYTHON_VERSION .venv
