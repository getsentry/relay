SHELL=/bin/bash
export PYTHON_VERSION := python3

DEFAULT_CONFIG_FILES := $(wildcard default_config/*.yml)
CONFIG_FILES := $(DEFAULT_CONFIG_FILES:default_config/%=config/%)

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
	less readme.md
.PHONY: about

config: setup-deps setup-brew setup-config
.PHONY: config

fake-sentry: setup-deps
	.venv/bin/python -m fake_sentry.fake_sentry
.PHONY: fake-sentry

check-test:
ifndef TEST
	$(error TEST is undefined. Please specify a test name such as `make TEST=simple load-test` or `make TEST=kafka_consumers load-test`)
endif
.PHONY: check-test

load-test: check-test setup-deps
	.venv/bin/locust -f $(TEST)_locustfile.py
.PHONY: load-test

setup-brew:
	brew bundle
.PHONY: setup-brew

setup-deps: setup-venv
	.venv/bin/pip install -U -r requirements.txt
.PHONY: setup-python-deps

setup-config: $(CONFIG_FILES)
.PHONY: setup-config

config/%.yml: default_config/%.yml
	@mkdir -p config
	cp $< $@

setup-venv: .venv/bin/python
.PHONY: setup-venv

.venv/bin/python: Makefile
	@rm -rf .venv
	@which virtualenv || sudo easy_install virtualenv
	virtualenv -p $$PYTHON_VERSION .venv
