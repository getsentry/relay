all: test

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

.PHONY: all doc test cargotest pytest format-check
