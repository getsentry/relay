SHELL=/bin/bash

export SEMAPHORE_PYTHON_VERSION := python3

all: test
.PHONY: all

setup-git:
	cd .git/hooks && ln -sf ../../scripts/git-precommit-hook.py pre-commit
.PHONY: setup-git

build:
.PHONY: build

releasebuild:
	cargo build --release --locked
	# Smoke test
	@$(MAKE) test-process-event CARGO_ARGS="--release"
.PHONY: releasebuild

releasebuild-docker:
	@scripts/docker-build-linux.sh
.PHONY: releasebuild-docker

doc:
	@cargo doc
.PHONY: doc

test: cargotest pytest
.PHONY: test

cargotest: GeoLite2-City.mmdb
	@cargo test --all
.PHONY: cargotest

cargotest-cov: GeoLite2-City.mmdb
	@cargo tarpaulin -v --skip-clean --all --out Xml
	@bash <(curl -s https://codecov.io/bash)
.PHONY: cargotest-cov

manylinux:
	@scripts/docker-manylinux.sh
.PHONY: manylinux

wheel:
	@$(MAKE) -C py wheel
.PHONY: wheel

sdist:
	@$(MAKE) -C py sdist
.PHONY: sdist

pytest:
	@$(MAKE) -C py test
.PHONY: pytest

.venv/bin/python: Makefile
	rm -rf .venv
	virtualenv -p $$SEMAPHORE_PYTHON_VERSION .venv

integration-test: .venv/bin/python
	.venv/bin/pip install -U pytest pytest-localserver requests flask "sentry-sdk>=0.2.0" pytest-rerunfailures pytest-xdist "git+https://github.com/untitaker/pytest-sentry#egg=pytest-sentry"
	cargo build
	@.venv/bin/pytest tests -n12 --reruns 5
.PHONY: integration-test

python-format: .venv/bin/python
	.venv/bin/pip install -U black
	.venv/bin/black .
.PHONY: python-format

python-format-check: .venv/bin/python
	.venv/bin/pip install -U black
	.venv/bin/black --check .
.PHONY: python-format

python-lint: .venv/bin/python
	.venv/bin/pip install -U flake8
	.venv/bin/flake8
.PHONY: python-format

format: python-format
	@rustup component add rustfmt 2> /dev/null
	@cargo fmt
.PHONY: format

format-check: python-format-check
	@rustup component add rustfmt 2> /dev/null
	@cargo fmt -- --check
.PHONY: format-check

clippy:
	@rustup component add clippy 2> /dev/null
	@cargo clippy --tests --all -- -D clippy::all
.PHONY: clippy

lint: clippy python-lint
.PHONY: lint

check: format test
.PHONY: check

test-process-event:
	# Process a basic event and assert its output
	bash -c 'diff \
		<(cargo run ${CARGO_ARGS} -- process-event <fixtures/basic-event-input.json) \
		fixtures/basic-event-output.json'
	@echo 'OK'
.PHONY: test-process-event

devserver:
	@systemfd --no-pid -s http::3000 -- cargo watch -x "run -- run"
.PHONY: devserver

GeoLite2-City.mmdb:
	@curl http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz | gzip -cd > $@

clean-target-dir:
	if [ "$$(du -s target/ | cut -f 1)" -gt 4000000 ]; then \
		rm -rf target/; \
	fi
