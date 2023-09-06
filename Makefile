SHELL=/bin/bash
export RELAY_PYTHON_VERSION := python3
export RELAY_FEATURES :=

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
	@cd relay && cargo +stable build --release --locked $(if ${RELAY_FEATURES}, --features ${RELAY_FEATURES})
.PHONY: release

build-linux-release: setup-git ## build linux release of the relay
	cd relay && cargo build --release --locked $(if ${RELAY_FEATURES}, --features ${RELAY_FEATURES}) --target=${TARGET}
	objcopy --only-keep-debug target/${TARGET}/release/relay{,.debug}
	objcopy --strip-debug --strip-unneeded target/${TARGET}/release/relay
	objcopy --add-gnu-debuglink target/${TARGET}/release/relay{.debug,}
.PHONY: build-linux-release

collect-source-bundle: setup-git ## copy the built relay binary to current folder and collects debug bundles
	mv target/${TARGET}/release/relay ./relay-bin
	zip relay-debug.zip target/${TARGET}/release/relay.debug
	sentry-cli --version
	sentry-cli difutil bundle-sources target/${TARGET}/release/relay.debug
	mv target/${TARGET}/release/relay.src.zip ./relay.src.zip
.PHONY: collect-source-bundle

build-release-with-bundles: build-linux-release collect-source-bundle
.PHONY: build-relase-with-bundles

sdist: setup-git setup-venv ## create a sdist of the Python library
	cd py && ../.venv/bin/python setup.py sdist --format=zip
.PHONY: sdist

wheel: setup-git setup-venv ## build a wheel of the Python library
	cd py && ../.venv/bin/python setup.py bdist_wheel
.PHONY: wheel

wheel-manylinux: setup-git ## build manylinux 2014 compatible wheels. This will build docker image for the requested architecture and cross compile the code inside
	@scripts/docker-manylinux.sh
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
	.venv/bin/flake8 py tests
	.venv/bin/mypy py tests
.PHONY: lint-python

lint-rust-beta: setup-git ## run lint on Rust using clippy and beta toolchain
	@rustup toolchain install beta 2>/dev/null
	@rustup component add clippy --toolchain beta 2> /dev/null
	cargo +beta clippy --workspace --all-targets --all-features --no-deps -- -D warnings
.PHONY: lint-rust-beta

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
	.venv/bin/pip install -U -r requirements-dev.txt
	RELAY_DEBUG=1 .venv/bin/pip install -v --editable py
	# Bump the mtime of an empty file.
	# Make will re-run 'pip install' if the mtime on requirements-dev.txt is higher again.
	touch .venv/python-requirements-stamp

.git/hooks/pre-commit:
	@cd .git/hooks && ln -sf ../../scripts/git-precommit-hook pre-commit

help: ## this help
	@ awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m\t%s\n", $$1, $$2 }' $(MAKEFILE_LIST) | column -s$$'\t' -t
.PHONY: help

gocd: ## Build GoCD pipelines
	@ rm -rf ./gocd/generated-pipelines
	@ mkdir -p ./gocd/generated-pipelines
	@ cd ./gocd/templates && jb install && jb update
	@ find . -type f \( -name '*.libsonnet' -o -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnetfmt -i
	@ find . -type f \( -name '*.libsonnet' -o -name '*.jsonnet' \) -print0 | xargs -n 1 -0 jsonnet-lint -J ./gocd/templates/vendor
	@ cd ./gocd/templates && jsonnet --ext-code output-files=true -J vendor -m ../generated-pipelines ./relay.jsonnet
	@ cd ./gocd/generated-pipelines && find . -type f \( -name '*.yaml' \) -print0 | xargs -n 1 -0 yq -p json -o yaml -i
.PHONY: gocd

web: ## Install and run frontend DEV web server for admin dashboard
	@ cargo install --locked trunk && cd relay-dashboard && trunk serve --open --proxy-backend ws://localhost:3001/api/  --proxy-ws
