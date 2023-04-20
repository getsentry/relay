fn main() {
    println!("Hello, world!");
}

pub mod anotherfile;

use crate::anotherfile::{BarFoo, FooBar};

struct MyStruct {
    #[metastructure(pii = "true")]
    foo: String,
    sub_struct: SubStruct,
}

struct SubStruct {
    field1: String,
    #[metastructure(pii = "true")]
    field2: FooBar,
    // Checks that it goes spearately into both HasGeneric and InnerGeneric to check for pii fields.
    mystery_field: HasGeneric<InnerGeneric>,
}

struct HasGeneric<T> {
    inner_generic: T,
    #[metastructure(pii = "maybe")]
    maybe_sensitive_stuff: u32,
}

struct InnerGeneric {
    #[metastructure(pii = "true")]
    bitcoin_wallet_key: String,
}
