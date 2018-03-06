all: test

setup-git:
	cd .git/hooks && ln -sf ../../scripts/git-precommit-hook.py pre-commit

build:
	@cargo build --all

doc:
	@cargo doc

test: cargotest pytest

cargotest:
	@cargo test --all

pytest:
	@$(MAKE) -C py test

format-check:
	@cargo fmt -- --write-mode diff

.PHONY: all setup-git doc test cargotest pytest format-check
