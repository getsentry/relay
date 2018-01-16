all: test

build:
	@cargo build --all

doc:
	@cargo doc

test: cargotest

cargotest:
	@cargo test --all

.PHONY: all doc test cargotest
