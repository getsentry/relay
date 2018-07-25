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
.PHONY: releasebuild

releasebuild-docker:
	@scripts/docker-build-linux.sh
.PHONY: releasebuild-docker

doc:
	@cargo doc
.PHONY: doc

test: cargotest pytest
.PHONY: test

cargotest:
	@cargo test --all
.PHONY: cargotest

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

format:
	@cargo fmt
.PHONY: format

format-check:
	@cargo fmt -- --write-mode diff
.PHONY: format-check

lint:
	@cargo +nightly clippy --tests --all -- -D clippy
.PHONY: lint

devserver:
	@systemfd --no-pid -s http::3000 -- cargo watch -x "run -- run"
.PHONY: devserver
