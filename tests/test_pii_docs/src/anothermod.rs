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

pub struct TruthTableTest {
    #[metastructure(pii = "truth_table_test", retain = "true", additional_properties)]
    foo: u32,
    #[metastructure(pii = "truth_table_test", additional_properties)]
    bar: u32,
    #[metastructure(pii = "truth_table_test", retain = "true")]
    baz: u32,
    #[metastructure(pii = "truth_table_test")]
    qux: u32,
}
