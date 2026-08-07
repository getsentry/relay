//Prost types for `tests/fixtures/protos/relay/test/v1/tree.proto`.

#![allow(missing_docs)]
pub mod relay {
    pub mod test {
        pub mod common {
            pub mod v1 {
                include!("relay.test.common.v1.rs");
            }
        }

        pub mod v1 {
            include!("relay.test.v1.rs");
        }
    }
}
