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
}
