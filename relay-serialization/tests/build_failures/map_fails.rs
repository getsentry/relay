use prost::Message;
use relay_serialization_derive::RuntimeDescription;
use std::collections::BTreeMap;
use std::collections::HashMap;

#[derive(Clone, PartialEq, Message, RuntimeDescription)]
struct MapDoesNotCompile {
    #[prost(map = "string, string", tag = "1")]
    value: HashMap<String, String>,
}

#[derive(Clone, PartialEq, Message, RuntimeDescription)]
struct HashMapDoesNotCompile {
    #[prost(hash_map = "string, string", tag = "1")]
    value: HashMap<String, String>,
}

#[derive(Clone, PartialEq, Message, RuntimeDescription)]
struct BTreeMapDoesNotCompile {
    #[prost(btree_map = "string, string", tag = "1")]
    value: BTreeMap<String, String>,
}

fn main() {}
