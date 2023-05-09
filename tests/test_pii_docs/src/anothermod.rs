pub struct FooBar {
    #[metastructure(pii = "false")]
    not_sensitive: u32,
}

pub struct BarFoo;

pub struct WeirdTypes<'a> {
    reference: &'a WeirdType,
    tuple: (WeirdType, u32),
    ptr: *const WeirdType,
    slice: [WeirdType],
    func: fn(WeirdType),
}

pub struct WeirdType {
    #[metastructure(pii = "true")]
    pii_true: u32,
}
