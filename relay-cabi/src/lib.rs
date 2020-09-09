//! This package contains the C-ABI of Relay. It builds a C header file and
//! associated library that can be linked to any C or C++ program. Note that the
//! header is C only. Primarily, this package is meant to be consumed by
//! higher-level wrappers in other languages, such as the
//! [`sentry-relay`](https://pypi.org/project/sentry-relay/) Python package.
//!
//! # Building
//!
//! Building the dynamic library has the same requirements as building the
//! `relay` crate:
//!
//! - Latest stable Rust and Cargo
//! - A checkout of this repository and all its GIT submodules
//!
//! To build, run `make release` in this directory. This creates a release build of
//! the dynamic library in `target/release/librelay_cabi.*`.
//!
//! # Usage
//!
//! The header comes packaged in the `include/` directory. It does not have any
//! dependencies other than standard C headers. Then, include it in sources and use
//! like this:
//!
//! ```c
//! #include "relay.h"
//!
//! int main() {
//!     RelayUuid uuid = relay_generate_relay_id();
//!     // use uuid
//!
//!     return 0;
//! }
//! ```
//!
//! In your application, point to the Relay include directory and specify the
//! `relay` library:
//!
//! ```bash
//! $(CC) -Irelay-cabi/include -Ltarget/release -lrelay -o myprogram main.c
//! ```
//!
//! # Development
//!
//! ## Requirements
//!
//! In addition to the build requirements, development requires a recent _Rust
//! nightly_ toolchain and the `cbindgen` tool. To set this up, run:
//!
//! ```bash
//! rustup toolchain add nightly
//! cargo install cbindgen
//! ```
//!
//! If your machine already has `cbindgen` installed, check the header of
//! [`include/relay.h`] for the minimum version required. This can be verified by
//! running `cbindgen --version`. It is generally advisable to keep `cbindgen`
//! updated to its latest version, and always check in cbindgen updates separate
//! from changes to the public interface.
//!
//! ## Makefile
//!
//! This package contains the Rust crate `relay-cabi` that exposes a public FFI
//! interface. There are additional build steps involved in generating the header
//! file located at `include/relay.h`. To aid development, there is a _Makefile_
//! that exposes all relevant commands for building:
//!
//! - `make build`: Builds the library using `cargo`.
//! - `make header`: Updates the header file based on the public interface.
//! - `make clean`: Removes all build artifacts but leaves the header.
//! - `make`: Builds the library and the header.
//!
//! For ease of development, the header is always checked into the repository. After
//! making changes, do not forget to run at least `make header` to ensure the header
//! is in sync with the library.
//!
//! ## Development Flow
//!
//! 1. Make changes to the `relay` crates and add tests.
//! 2. Update `relay-cabi` and add, remove or update functions as needed.
//! 3. Regenerate the header by running `make header` in the `relay-cabi/` directory.
//! 4. Go to the Python package in the `py/` folder and update the high-level wrappers.
//! 5. Consider whether this changeset requires a major version bump.
//!
//! The general rule for major versions is:
//!
//! - If everything is backward compatible, do **not major** bump.
//! - If the C interface breaks compatibility but high-level wrappers are still
//!   backwards compatible, do **not major** bump. The C interface is currently
//!   viewed as an implementation detail.
//! - If high-level wrappers are no longer backwards compatible or there are
//!   breaking changes in the `relay` crate itself, **do major** bump.

#![allow(clippy::missing_safety_doc)]
#![warn(missing_docs)]

#[macro_use]
mod utils;

mod auth;
mod constants;
mod core;
mod processing;

pub use crate::auth::*;
pub use crate::constants::*;
pub use crate::core::*;
pub use crate::processing::*;
