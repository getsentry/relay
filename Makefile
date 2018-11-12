all: format-check lint test
.PHONY: all

build:
	@cargo build --all-features
.PHONY: build

doc:
	@cargo doc
.PHONY: doc

test: cargotest
.PHONY: test

cargotest: GeoLiteCity.dat
	@cargo test --all-features --all
.PHONY: cargotest

format:
	@rustup component add rustfmt-preview 2> /dev/null
	@cargo fmt
.PHONY: format

format-check:
	@rustup component add rustfmt-preview 2> /dev/null
	@cargo fmt -- --check
.PHONY: formatcheck

lint:
	@rustup component add clippy-preview 2> /dev/null
	@cargo clippy --all-features --tests --examples -- -D clippy::all
.PHONY: lint

GeoLite2-City.mmdb:
	@curl http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz | gzip -cd > $@
