SHELL=/bin/bash
export RELAY_PYTHON_VERSION := python3
export RELAY_FEATURES := ssl

all: check test
.PHONY: all

check: style lint
.PHONY: check

clean:
	cargo clean
	cargo clean --manifest-path relay-cabi/Cargo.toml
	rm -rf .venv
.PHONY: clean

# Builds

build: setup-git
	@cargo +stable build --all-features
.PHONY: build

release: setup-git
	@cargo +stable build --release --locked --features ${RELAY_FEATURES}
.PHONY: release

docker: setup-git
	@scripts/docker-build-linux.sh
.PHONY: docker

build-linux-release: setup-git
	cargo build --release --locked --features ${RELAY_FEATURES} --target=${TARGET}
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
	cargo test --all
.PHONY: test-rust

test-rust-all: setup-git
	cargo test --all --all-features
.PHONY: test-rust-all

test-python: setup-git setup-venv
	.venv/bin/pip install -U pytest
	RELAY_DEBUG=1 .venv/bin/pip install -v --editable py
	.venv/bin/pytest -v py
.PHONY: test-python

test-integration: build setup-venv
	.venv/bin/pip install -U -r requirements-test.txt
	.venv/bin/pytest tests -n auto -v
.PHONY: test-integration

test-process-event:
	# Process a basic event and assert its output
	bash -c 'diff \
		<(cargo run ${CARGO_ARGS} -- process-event <tests/fixtures/basic-event-input.json) \
		tests/fixtures/basic-event-output.json'
	@echo 'OK'
.PHONY: test-process-event

# Documentation

doc: docs
.PHONY: doc

docs: api-docs prose-docs
.PHONY: api-docs prose-docs

api-docs: setup-git
	@cargo doc
.PHONY: api-docs

prose-docs: .venv/bin/python extract-metric-docs
	.venv/bin/mkdocs build
	touch site/.nojekyll
.PHONY: prose-docs

extract-metric-docs: .venv/bin/python
	.venv/bin/pip install -U -r requirements-doc.txt
	cd scripts && ../.venv/bin/python extract_metric_docs.py

jsonschema: init-submodules
	rm -rf docs/event-schema/event.schema.*
	set -e && cargo run --features jsonschema -- event-json-schema \
		> docs/event-schema/event.schema.json
.PHONY: jsonschema

docserver: prose-docs
	.venv/bin/mkdocs serve
.PHONY: docserver

travis-upload-prose-docs: prose-docs
	cd site && zip -r gh-pages .
	set -e && zeus upload -t "application/zip+docs" site/gh-pages.zip \
		|| [[ ! "$(TRAVIS_BRANCH)" =~ ^release/ ]]
	set -e && zeus upload -t "application/octet-stream" -n event.schema.json docs/event-schema/event.schema.json \
		|| [[ ! "$(TRAVIS_BRANCH)" =~ ^release/ ]]
.PHONY: travis-upload-docs

local-upload-prose-docs: prose-docs
	# Use this for hotfixing docs, prefer a new release
	.venv/bin/pip install -U ghp-import
	.venv/bin/ghp-import -pf site/

# Style checking

style: style-rust style-python
.PHONY: style

style-rust:
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt -- --check
.PHONY: style-rust

style-python: setup-venv
	.venv/bin/pip install -U -r requirements-dev.txt
	.venv/bin/black --check py tests --exclude '\.eggs|sentry_relay/_lowlevel.*'
.PHONY: style-python

# Linting

lint: lint-rust lint-python
.PHONY: lint

lint-rust: setup-git
	@rustup component add clippy --toolchain stable 2> /dev/null
	cargo +stable clippy --all-features --all --tests --examples -- -D clippy::all
.PHONY: lint-rust

lint-python: setup-venv
	.venv/bin/pip install -U -r requirements-dev.txt
	.venv/bin/flake8 py
.PHONY: lint-python

# Formatting

format: format-rust format-python
.PHONY: format

format-rust:
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt
.PHONY: format-rust

format-python: setup-venv
	.venv/bin/pip install -U -r requirements-dev.txt
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

setup-venv: .venv/bin/python
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
	@rm -rf .venv
	@which virtualenv || sudo pip install virtualenv
	virtualenv -p $$RELAY_PYTHON_VERSION .venv

.git/hooks/pre-commit:
	@cd .git/hooks && ln -sf ../../scripts/git-precommit-hook.py pre-commit
