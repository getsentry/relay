SHELL=/bin/bash
export SEMAPHORE_PYTHON_VERSION := python3

all: check test
.PHONY: all

check: style lint
.PHONY: check

clean:
	cargo clean
	cargo clean --manifest-path cabi/Cargo.toml
	rm -rf .venv
	rm -f GeoLite2-City.mmdb
.PHONY: clean

# Builds

build: setup
	@cargo +stable build --all --all-features
.PHONY: build

release: setup
	@cargo +stable build --all --all-features --release
.PHONY: build-release

docker: setup
	@scripts/docker-build-linux.sh
.PHONY: build-docker

sdist: setup-venv
	cd py && ../.venv/bin/python setup.py sdist --format=zip
.PHONY: sdist

wheel: setup
	cd py && ../.venv/bin/python setup.py bdist_wheel
.PHONY: wheel

wheel-manylinux: setup
	@scripts/docker-manylinux.sh
.PHONY: manylinux

# Tests

test: test-rust test-python test-integration
.PHONY: test

test-rust: setup
	cargo test --all --all-features
.PHONY: test-rust

test-python: setup
	.venv/bin/pip install -U pytest
	SEMAPHORE_DEBUG=1 .venv/bin/pip install -v --editable py
	.venv/bin/pytest -v py
.PHONY: test-python

test-integration: build
	.venv/bin/pip install -U pytest pytest-localserver requests flask "sentry-sdk>=0.2.0" pytest-rerunfailures pytest-xdist "git+https://github.com/untitaker/pytest-sentry#egg=pytest-sentry"
	.venv/bin/pytest tests -n12 --reruns 5 -v
.PHONY: test-integration

test-coverage: setup
	@cargo tarpaulin -v --all --out Xml
	@bash <(curl -s https://codecov.io/bash)
.PHONY: test-coverage

test-process-event: setup
	# Process a basic event and assert its output
	bash -c 'diff \
		<(cargo run ${CARGO_ARGS} -- process-event <fixtures/basic-event-input.json) \
		fixtures/basic-event-output.json'
	@echo 'OK'
.PHONY: test-process-event

# Documentation

doc: setup
	@cargo doc
.PHONY: doc

# Style checking

style: style-rust style-python
.PHONY: style

style-rust: setup
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt -- --check
.PHONY: style-rust

style-python: setup-venv
	.venv/bin/pip install -U black
	.venv/bin/black --check py --exclude '\.eggs|semaphore/_lowlevel.*'
.PHONY: style-python

# Linting

lint: lint-rust lint-python
.PHONY: lint

lint-rust: setup
	@rustup component add clippy --toolchain stable 2> /dev/null
	cargo +stable clippy --all-features --all --tests --examples -- -D clippy::all
.PHONY: lint-rust

lint-python: setup-venv
	.venv/bin/pip install -U flake8
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
	.venv/bin/pip install -U black
	.venv/bin/black py --exclude '\.eggs|semaphore/_lowlevel.*'
.PHONY: format-python

# Development

setup: setup-geoip setup-git
.PHONY: setup

setup-git: .git/hooks/pre-commit
	@git submodule update --init --recursive
.PHONY: setup-git

setup-geoip: GeoLite2-City.mmdb
.PHONY: setup-geoip

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
	@which virtualenv || sudo easy_install virtualenv
	virtualenv -p $$SEMAPHORE_PYTHON_VERSION .venv

GeoLite2-City.mmdb:
	@curl http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz | gzip -cd > $@

.git/hooks/pre-commit:
	cd .git/hooks && ln -sf ../../scripts/git-precommit-hook.py pre-commit
