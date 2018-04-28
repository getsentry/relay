all: test

setup-git:
	cd .git/hooks && ln -sf ../../scripts/git-precommit-hook.py pre-commit

build:
	@cargo build --all

releasebuild:
	@OPENSSL_STATIC=1 cargo build --release

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
	@catflap -p 3000 -- cargo watch -x "run -- -c relay.yml"

.PHONY: all setup-git doc test cargotest pytest format-check devserver
