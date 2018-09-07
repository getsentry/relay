SHELL=/bin/bash

all: test
.PHONY: all

setup-git:
	cd .git/hooks && ln -sf ../../scripts/git-precommit-hook.py pre-commit
.PHONY: setup-git

build:
	@cargo build --all
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

test: cargotest pytest integration-test
.PHONY: test

cargotest:
	@cargo test --all
.PHONY: cargotest

cargotest-cov:
	@cargo tarpaulin -v --all --out Xml
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

tests/venv/bin/python: Makefile
	rm -rf tests/venv
	virtualenv -ppython3 tests/venv
	tests/venv/bin/pip install -U pytest pytest-localserver requests flask sentry-sdk pytest-rerunfailures pytest-xdist

integration-test: tests/venv/bin/python
	cargo build
	@tests/venv/bin/pytest tests -n12 --reruns 5
.PHONY: integration-test

format:
	@cargo fmt
.PHONY: format

format-check:
	@cargo fmt -- --check
.PHONY: format-check

lint:
	@cargo +nightly clippy --tests --all -- -D clippy::all
.PHONY: lint

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
