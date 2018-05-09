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

dockerbuild:
	@scripts/docker-build.sh
.PHONY: dockerbuild

doc:
	@cargo doc
.PHONY: doc

test: cargotest pytest
.PHONY: test

cargotest:
	@cargo test --all
.PHONY: cargotest

pymanylinux:
	@scripts/docker-manylinux.sh
.PHONY: pymanylinux

pywheel:
	@$(MAKE) -C py wheel
.PHONY: pywheel

pytest:
	@$(MAKE) -C py test
.PHONY: pytest

format-check:
	@cargo fmt -- --write-mode diff
.PHONY: format-check

devserver:
	@catflap -p 3000 -- cargo watch -x "run -- run"
.PHONY: devserver
