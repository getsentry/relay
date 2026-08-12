use prost::Message;
use relay_serialization_derive::BoundedMessage as DeriveBoundedMessage;
use std::collections::HashMap;

#[derive(Clone, PartialEq, Message, DeriveBoundedMessage)]
struct MapDoesNotCompile {
    #[prost(map = "string, string", tag = "1")]
    value: HashMap<String, String>,
}

fn main() {}
