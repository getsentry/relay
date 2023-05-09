fn main() {
    println!("Hello, world!");
}

pub mod anothermod;

use crate::anothermod::{BarFoo, FooBar, WeirdTypes};

struct MyStruct {
    #[metastructure(foo = "bar", pii = "true")]
    foo: String,
    sub_struct: SubStruct,
}

struct SubStruct {
    field1: String,
    #[metastructure(pii = "true")]
    field2: FooBar,
    //  Checks that it goes spearately into both HasGeneric and InnerGeneric to check for pii fields.
    mystery_field: HasGeneric<InnerGeneric>,
}

struct HasGeneric<T> {
    inner_generic: T,
    #[metastructure(pii = "maybe", apple = "oranges")]
    maybe_sensitive_stuff: u32,
}

struct InnerGeneric {
    #[metastructure(pii = "true")]
    bitcoin_wallet_key: String,
}

struct SelfReferential {
    #[metastructure(pii = "true")]
    pretty_sensitive: String,
    // Tests that it doesn't get stuck in an infinite loop.
    oh_no_its_self_referential: Box<SelfReferential>,
}
