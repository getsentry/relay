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
.PHONY: clean

# Builds

build: GeoLite2-City.mmdb
	@cargo +stable build --all --all-features
.PHONY: build

release: GeoLite2-City.mmdb
	@cargo +stable build --all --all-features --release
.PHONY: build-release

docker:
	@scripts/docker-build-linux.sh
.PHONY: build-docker

sdist:
	cd py && ../.venv/bin/python setup.py sdist --format=zip
.PHONY: sdist

wheel:
	cd py && ../.venv/bin/python setup.py bdist_wheel
.PHONY: wheel

wheel-manylinux:
	@scripts/docker-manylinux.sh
.PHONY: manylinux

# Tests

test: test-rust test-python test-integration
.PHONY: test

test-rust: GeoLite2-City.mmdb
	cargo test --all --all-features
.PHONY: test-rust

test-python: GeoLite2-City.mmdb .venv/bin/python
	.venv/bin/pip install -U pytest
	SEMAPHORE_DEBUG=1 .venv/bin/pip install -v --editable py
	.venv/bin/pytest -v py
.PHONY: test-python

test-integration: build .venv/bin/python
	.venv/bin/pip install -U pytest pytest-localserver requests flask "sentry-sdk>=0.2.0" pytest-rerunfailures pytest-xdist "git+https://github.com/untitaker/pytest-sentry#egg=pytest-sentry"
	.venv/bin/pytest py -n12 --reruns 5
.PHONY: test-integration

test-coverage: GeoLite2-City.mmdb
	@cargo tarpaulin -v --skip-clean --all --out Xml
	@bash <(curl -s https://codecov.io/bash)
.PHONY: test-coverage

test-process-event:
	# Process a basic event and assert its output
	bash -c 'diff \
		<(cargo run ${CARGO_ARGS} -- process-event <fixtures/basic-event-input.json) \
		fixtures/basic-event-output.json'
	@echo 'OK'
.PHONY: test-process-event

# Documentation

doc:
	@cargo doc
.PHONY: doc

# Style checking

style: style-rust style-python
.PHONY: style

style-rust:
	@rustup component add rustfmt --toolchain stable 2> /dev/null
	cargo +stable fmt -- --check
.PHONY: style-rust

style-python: .venv/bin/python
	.venv/bin/pip install -U black
	.venv/bin/black --check py --exclude '\.eggs|semaphore/_lowlevel.*'
.PHONY: style-python

# Linting

lint: lint-rust lint-python
.PHONY: lint

lint-rust:
	@rustup component add clippy --toolchain stable 2> /dev/null
	cargo +stable clippy --all-features --all --tests --examples -- -D clippy::all
.PHONY: lint-rust

lint-python: .venv/bin/python
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

format-python: .venv/bin/python
	.venv/bin/pip install -U black
	.venv/bin/black py --exclude '\.eggs|semaphore/_lowlevel.*'
.PHONY: format-python

# Development

setup: GeoLite2-City.mmdb .git/hooks/pre-commit
.PHONY: setup

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
