# Sentry AWS Extension

## Compilation on Apple M1

**NOTE:** This applies only if you have an **ARM based Apple M1**!

Follow these steps below on your Apple M1 machine to compile relay to be
run in the AWS Lambda execution environment.

(See https://github.com/messense/homebrew-macos-cross-toolchains for details)

```bash
# Install cross compiling tool chain
brew tap messense/macos-cross-toolchains
brew install x86_64-unknown-linux-gnu

export CC_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-gcc
export CXX_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-g++
export AR_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-ar
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc

rustup target add x86_64-unknown-linux-gnu

# Install objcopy (used in `Makefile`)
brew install binutils
ln -s "$(brew --prefix binutils)/bin/gobjcopy" /usr/local/bin/objcopy

# Build the relay binary
export TARGET=x86_64-unknown-linux-gnu
make build-linux-release
```

The `relay` binary can be found at `target/x86_64-unknown-linux-gnu/release/relay`
and can be used as an AWS Lambda extension.
