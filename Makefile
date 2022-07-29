SHELL=/bin/bash
export RELAY_PYTHON_VERSION := python3
export RELAY_FEATURES := ssl

all: check test
.PHONY: all

check: style lint
.PHONY: check

clean:
	cargo clean
	rm -rf .venv
.PHONY: clean

# Builds

build: setup-git
	cargo +stable build --all-features
.PHONY: build

release: setup-git
	@cd relay && cargo +stable build --release --locked --features ${RELAY_FEATURES}
.PHONY: release

build-linux-release: setup-git
	cd relay && cargo build --release --locked --features ${RELAY_FEATURES} --target=${TARGET}
	objcopy --only-keep-debug target/${TARGET}/release/relay{,.debug}
	objcopy --strip-debug --strip-unneeded target/${TARGET}/release/relay
	objcopy --add-gnu-debuglink target/${TARGET}/release/relay{.debug,}
.PHONY: build-linux-release

sdist: setup-git setup-venv
	cd py && ../.venv/bin/python setup.py sdist --format=zip
.PHONY: sdist

wheel: setup-git setup-venv
	cd py && ../.venv/bin/python setup.py bdist_wheel
.PHONY: wheel

wheel-manylinux: setup-git
	@scripts/docker-manylinux.sh
.PHONY: wheel-manylinux

# Tests

test: test-rust-all test-python test-integration
.PHONY: test

test-rust: setup-git
	cargo test --workspace
.PHONY: test-rust

test-rust-all: setup-git
	cargo test --workspace --all-features
.PHONY: test-rust-all

test-python: setup-git setup-venv
	RELAY_DEBUG=1 .venv/bin/pip install -v --editable py
	.venv/bin/pytest -v py
.PHONY: test-python

test-integration: build setup-venv
	.venv/bin/pytest tests -n auto -v
.PHONY: test-integration

# Documentation

doc: doc-api
.PHONY: doc-api

doc-api: setup-git
	cargo doc --workspace --all-features --no-deps
.PHONY: doc-api

# Style checking

style: style-rust style-python
.PHONY: style

style-rust:
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt --all -- --check
.PHONY: style-rust

style-python: setup-venv
	.venv/bin/black --check py tests --exclude '\.eggs|sentry_relay/_lowlevel.*'
.PHONY: style-python

# Linting

lint: lint-rust lint-python
.PHONY: lint

lint-rust: setup-git
	@rustup component add clippy --toolchain stable 2> /dev/null
	cargo +stable clippy --workspace --all-features --tests -- -D clippy::all
.PHONY: lint-rust

lint-python: setup-venv
	.venv/bin/flake8 py
.PHONY: lint-python

# Formatting

format: format-rust format-python
.PHONY: format

format-rust:
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt --all
.PHONY: format-rust

format-python: setup-venv
	.venv/bin/black py tests scripts --exclude '\.eggs|sentry_relay/_lowlevel.*'
.PHONY: format-python

# Development

setup: setup-git setup-venv
.PHONY: setup

init-submodules:
	@git submodule update --init --recursive
.PHONY: init-submodules

setup-git: .git/hooks/pre-commit init-submodules
.PHONY: setup-git

setup-venv: .venv/bin/python .venv/python-requirements
.PHONY: setup-venv

devserver:
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

.venv/python-requirements: requirements-dev.txt
	@# Work around https://github.com/confluentinc/confluent-kafka-python/issues/1190
	@if [ "$$(uname -sm)" = "Darwin arm64" ]; then \
		echo "Using 'librdkafka' from homebrew to build confluent-kafka"; \
		export C_INCLUDE_PATH="$$(brew --prefix librdkafka)/include"; \
	fi; \
	.venv/bin/pip install -U -r requirements-dev.txt
	# Bump the mtime of an empty file.
	# Make will re-run 'pip install' if the mtime on requirements-dev.txt is higher again.
	touch .venv/python-requirements

.git/hooks/pre-commit:
	@cd .git/hooks && ln -sf ../../scripts/git-precommit-hook pre-commit
