pub struct FooBar {
    #[metastructure(pii = "false")]
    not_sensitive: u32,
}

pub struct BarFoo;
