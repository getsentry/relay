SHELL=/bin/bash
export RELAY_PYTHON_VERSION := python3
export RELAY_FEATURES := ssl

all: check test ## run all checks and tests
.PHONY: all

check: style lint ## run the lints and check the code style for rust and python code
.PHONY: check

clean: ## remove python virtual environment and delete cached rust files together with target folder
	cargo clean
	rm -rf .venv
.PHONY: clean

# Builds

build: setup-git ## build relay with all features enabled without debug info
	cargo +stable build --all-features
.PHONY: build

release: setup-git ## build production binary of the relay with debug info
	@cd relay && cargo +stable build --release --locked --features ${RELAY_FEATURES}
.PHONY: release

build-linux-release: setup-git ## build linux release of the relay
	cd relay && cargo build --release --locked --features ${RELAY_FEATURES} --target=${TARGET}
	objcopy --only-keep-debug target/${TARGET}/release/relay{,.debug}
	objcopy --strip-debug --strip-unneeded target/${TARGET}/release/relay
	objcopy --add-gnu-debuglink target/${TARGET}/release/relay{.debug,}
.PHONY: build-linux-release

sdist: setup-git setup-venv ## create a sdist of the Python library
	cd py && ../.venv/bin/python setup.py sdist --format=zip
.PHONY: sdist

wheel: setup-git setup-venv ## build a wheel of the Python library
	cd py && ../.venv/bin/python setup.py bdist_wheel
.PHONY: wheel

wheel-manylinux: setup-git ## build manylinux 2014 compatible wheels, you must to have a pre-built image from py/Dockerfile
	@scripts/docker-build-lib-linux.sh
.PHONY: wheel-manylinux

# Tests

test: test-rust-all test-python test-integration ## run all unit and integration tests
.PHONY: test

test-rust: setup-git ## run tests for Rust code with default features enabled
	cargo test --workspace
.PHONY: test-rust

test-rust-all: setup-git ## run tests for Rust code with all the features enabled
	cargo test --workspace --all-features
.PHONY: test-rust-all

test-python: setup-git setup-venv ## run tests for Python code
	RELAY_DEBUG=1 .venv/bin/pip install -v --editable py
	.venv/bin/pytest -v py
.PHONY: test-python

test-integration: build setup-venv ## run integration tests
	.venv/bin/pytest tests -n auto -v
.PHONY: test-integration

# Documentation

doc: doc-rust ## generate all API docs
.PHONY: doc

doc-rust: setup-git ## generate API docs for Rust code
	cargo doc --workspace --all-features --no-deps
.PHONY: doc-rust

# Style checking

style: style-rust style-python ## check code style
.PHONY: style

style-rust: ## check Rust code style
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt --all -- --check
.PHONY: style-rust

style-python: setup-venv ## check Python code style
	.venv/bin/black --check py tests --exclude '\.eggs|sentry_relay/_lowlevel.*'
.PHONY: style-python

# Linting

lint: lint-rust lint-python ## runt lint on Python and Rust code
.PHONY: lint

lint-rust: setup-git ## run lint on Rust code using clippy
	@rustup component add clippy --toolchain stable 2> /dev/null
	cargo +stable clippy --workspace --all-targets --all-features --no-deps -- -D warnings
.PHONY: lint-rust

lint-python: setup-venv ## run lint on Python code using flake8
	.venv/bin/flake8 py
.PHONY: lint-python

# Formatting

format: format-rust format-python ## format all the Rust and Python code
.PHONY: format

format-rust: ## format the Rust code
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt --all
.PHONY: format-rust

format-python: setup-venv ## format the Python code
	.venv/bin/black py tests scripts --exclude '\.eggs|sentry_relay/_lowlevel.*'
.PHONY: format-python

# Development

setup: setup-git setup-venv ## run setup tasks to create and configure development environment
.PHONY: setup

init-submodules:
	@git submodule update --init --recursive
.PHONY: init-submodules

setup-git: .git/hooks/pre-commit init-submodules ## make sure all git configured and all the submodules are fetched
.PHONY: setup-git

setup-venv: .venv/bin/python .venv/python-requirements-stamp ## create a Python virtual environment with development requirements installed
.PHONY: setup-venv

devserver: ## run an auto-reloading development server
	@systemfd --no-pid -s http::3000 -- cargo watch -x "run -- run"
.PHONY: devserver

clean-target-dir:
	if [ "$$(du -s target/ | cut -f 1)" -gt 4000000 ]; then \
		rm -rf target/; \
	fi
.PHONY: clean-target-dir

# Dependencies

.venv/bin/python: Makefile
	rm -rf .venv

	@# --copies is necessary because OS X make checks the mtime of the symlink
	@# target (/usr/local/bin/python), which is always much older than the
	@# Makefile, and then proceeds to unconditionally rebuild the venv.
	$$RELAY_PYTHON_VERSION -m venv --copies .venv
	.venv/bin/pip install -U pip wheel

.venv/python-requirements-stamp: requirements-dev.txt
	@# Work around https://github.com/confluentinc/confluent-kafka-python/issues/1190
	@if [ "$$(uname -sm)" = "Darwin arm64" ]; then \
		echo "Using 'librdkafka' from homebrew to build confluent-kafka"; \
		export C_INCLUDE_PATH="$$(brew --prefix librdkafka)/include"; \
	fi; \
	.venv/bin/pip install -U -r requirements-dev.txt
	# Bump the mtime of an empty file.
	# Make will re-run 'pip install' if the mtime on requirements-dev.txt is higher again.
	touch .venv/python-requirements-stamp

.git/hooks/pre-commit:
	@cd .git/hooks && ln -sf ../../scripts/git-precommit-hook pre-commit

help: ## this help
	@ awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m\t%s\n", $$1, $$2 }' $(MAKEFILE_LIST) | column -s$$'\t' -t
.PHONY: help
