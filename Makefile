all: test

setup-git:
	cd .git/hooks && ln -sf ../../scripts/git-precommit-hook.py pre-commit

build:
	@cargo build --all

releasebuild:
	@CARGO_INCREMENTAL= OPENSSL_STATIC=1 cargo build --release

doc:
	@cargo doc

test: cargotest pytest

cargotest:
	@cargo test --all

pytest:
	@$(MAKE) -C py test

format-check:
	@cargo fmt -- --write-mode diff

devserver:
	@catflap -p 3000 -- cargo watch -x "run -- run"

.PHONY: all setup-git build releasebuild doc test cargotest pytest format-check devserver
