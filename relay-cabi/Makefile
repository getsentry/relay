all: header release
.PHONY: all

header: include/relay.h
.PHONY: header

release:
	cargo build --release
.PHONY: release

clean:
	cargo clean
.PHONY: clean

include/relay.h: *.toml src/*.rs
	@touch src/lib.rs  # Ensure that rustc outputs something
	cbindgen -c cbindgen.toml . -o $@
	@touch include/relay.h  # Ensure that we don't build again

.PHONY: include/relay.h
