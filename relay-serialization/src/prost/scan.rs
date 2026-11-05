use prost::{DecodeError, Message};
use std::fmt;

use crate::meter::{LimitExceeded, Meter};

/// Costs associated with different kinds of operations; right now, just one cost for every field
/// occurrence on the wire (but leave the door open for more.)
mod cost {
    pub const FIELD: usize = 1;
}

/// The maximum nesting depth the scanner walks before giving up.  This matches prost's own
/// `RECURSION_LIMIT`.
const RECURSION_LIMIT: u32 = 100;

/// Protobuf wire types, as encoded in the bottom three bits of a field key.
mod wire_type {
    pub const VARINT: u8 = 0;
    pub const SIXTY_FOUR_BIT: u8 = 1;
    pub const LENGTH_DELIMITED: u8 = 2;
    pub const START_GROUP: u8 = 3;
    pub const END_GROUP: u8 = 4;
    pub const THIRTY_TWO_BIT: u8 = 5;
}

/// Describes the parts of the proto message we're interested in: the name, as well as the nested
/// types into which we'll need to descend.
pub struct MessageDesc {
    /// The name of the message.
    pub name: &'static str,

    /// The types nested within this message.
    pub nested: &'static [(u32, &'static MessageDesc)],
}

impl MessageDesc {
    /// Returns the descriptor for the nested message at `tag`, if that field holds one.
    fn nested(&self, tag: u32) -> Option<&'static MessageDesc> {
        self.nested
            .iter()
            .find(|(nested_tag, _)| *nested_tag == tag)
            .map(|(_, desc)| *desc)
    }
}

/// An error returned by [`scan`] or [`decode`].
#[derive(Debug)]
pub enum Error {
    /// A scanner error (reading a primitive, reaching a recursion limit, etc.)
    ScanError(String),
    /// Actually hitting the op budget for the decode.
    LimitExceeded,
    /// A decoder error, originating from prost.
    Decode(DecodeError),
}

impl Error {
    /// Returns `true` if the message decoding exceeded the budget.
    pub fn is_limit_exceeded(&self) -> bool {
        matches!(self, Self::LimitExceeded)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScanError(msg) => write!(f, "scanner error: {}", msg),
            Self::LimitExceeded => write!(f, "message exceeds the operation limit"),
            Self::Decode(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ScanError(_) => None,
            Self::LimitExceeded => None,
            Self::Decode(error) => Some(error),
        }
    }
}

/// Decodes an `M` from `buf`, spending at most `max_ops` doing so.  Returns
/// [`Error::LimitExceeded`] if the message exceeds the budget.
pub fn decode<M>(buf: &[u8], desc: &MessageDesc, max_ops: usize) -> Result<M, Error>
where
    M: Message + Default,
{
    scan(buf, desc, max_ops)?;
    M::decode(buf).map_err(Error::Decode)
}

/// Checks that the message in `buf` fits within `max_ops`, without decoding it.  Returns
/// the number of ops spent doing the scan.
pub fn scan(buf: &[u8], desc: &MessageDesc, max_ops: usize) -> Result<usize, Error> {
    let mut meter = Meter::new(max_ops);

    match scan_message(buf, desc, &mut meter, 0) {
        Ok(()) => Ok(meter.spent()),
        // The budget is checked first, because it travels as an ordinary decode error.
        Err(_) if meter.exceeded() => Err(Error::LimitExceeded),
        Err(error) => Err(error),
    }
}

impl From<LimitExceeded> for Error {
    fn from(_: LimitExceeded) -> Self {
        Error::LimitExceeded
    }
}

fn scan_message(
    buf: &[u8],
    desc: &MessageDesc,
    meter: &mut Meter,
    depth: u32,
) -> Result<(), Error> {
    if depth > RECURSION_LIMIT {
        return Err(Error::ScanError(format!(
            "{}: recursion limit reached",
            desc.name
        )));
    }

    let mut reader = Reader(buf);
    while !reader.is_empty() {
        let (tag, wire_type) = key(&mut reader)?;
        meter.spend(cost::FIELD)?;
        scan_field(&mut reader, tag, wire_type, Some(desc), meter, depth)?;
    }

    Ok(())
}

/// Consumes the body of a single field, recursing if the schema says it holds a message.
fn scan_field(
    reader: &mut Reader<'_>,
    tag: u32,
    wire_type: u8,
    desc: Option<&MessageDesc>,
    meter: &mut Meter,
    depth: u32,
) -> Result<(), Error> {
    match wire_type {
        wire_type::VARINT => {
            reader.read_varint()?;
        }
        wire_type::SIXTY_FOUR_BIT => {
            reader.read_exact(8)?;
        }
        wire_type::THIRTY_TWO_BIT => {
            reader.read_exact(4)?;
        }
        wire_type::LENGTH_DELIMITED => {
            let len: usize = usize::try_from(reader.read_varint()?)
                .map_err(|_| Error::ScanError("buffer underflow".to_owned()))?;
            let payload = reader.read_exact(len)?;

            // Only recurse where our generated schema tells us we have a nested message.  This is
            // how we distinguish between strings/repeated bytes, and genuine nested messages.
            if let Some(nested) = desc.and_then(|desc| desc.nested(tag)) {
                scan_message(payload, nested, meter, depth + 1)?;
            }
        }
        // proto3 has no groups, so nothing inside one can have a descriptor. This exists so that a
        // payload carrying group wire types is walked rather than rejected, matching what prost's
        // `skip_field` accepts.
        wire_type::START_GROUP => scan_group(reader, tag, meter, depth + 1)?,
        wire_type::END_GROUP => {
            return Err(Error::ScanError("unexpected end group tag".to_owned()));
        }
        _ => return Err(Error::ScanError("invalid wire type value".to_owned())),
    }

    Ok(())
}

fn scan_group(
    reader: &mut Reader<'_>,
    group_tag: u32,
    meter: &mut Meter,
    depth: u32,
) -> Result<(), Error> {
    if depth > RECURSION_LIMIT {
        return Err(Error::ScanError("recursion limit reached".to_owned()));
    }

    loop {
        let (tag, wire_type) = key(reader)?;
        meter.spend(cost::FIELD)?;

        if wire_type == wire_type::END_GROUP {
            if tag != group_tag {
                return Err(Error::ScanError("unexpected end group tag".to_owned()));
            }
            return Ok(());
        }

        scan_field(reader, tag, wire_type, None, meter, depth)?;
    }
}

fn key(reader: &mut Reader<'_>) -> Result<(u32, u8), Error> {
    let key = reader.read_varint()?;
    let wire_type = (key & 0b111) as u8;
    let tag =
        u32::try_from(key >> 3).map_err(|_| Error::ScanError("invalid tag value".to_owned()))?;

    if tag == 0 {
        return Err(Error::ScanError("invalid tag value".to_owned()));
    }

    Ok((tag, wire_type))
}

// A little wrapper to assist with reading and consuming bytes from a proto byte-buffer.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn read_varint(&mut self) -> Result<u64, Error> {
        leb128::read::unsigned(&mut self.0)
            .map_err(|_| Error::ScanError("invalid varint".to_owned()))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], Error> {
        if len > self.0.len() {
            return Err(Error::ScanError("buffer underflow".to_owned()));
        }

        let (payload, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A leaf message with one field of every shape the scanner dispatches on.
    #[derive(Clone, PartialEq, Message)]
    struct Leaf {
        #[prost(string, tag = "1")]
        text: String,
        #[prost(uint64, tag = "2")]
        number: u64,
        #[prost(double, tag = "3")]
        double: f64,
        #[prost(fixed32, tag = "4")]
        fixed: u32,
        #[prost(bytes = "vec", tag = "5")]
        blob: Vec<u8>,
    }

    static LEAF: MessageDesc = MessageDesc {
        name: "Leaf",
        nested: &[],
    };

    /// A message which nests, so the scanner has to recurse to see the leaves.
    #[derive(Clone, PartialEq, Message)]
    struct Branch {
        #[prost(message, repeated, tag = "1")]
        leaves: Vec<Leaf>,
        #[prost(message, optional, boxed, tag = "2")]
        branch: Option<Box<Branch>>,
        #[prost(string, tag = "3")]
        label: String,
    }

    static BRANCH: MessageDesc = MessageDesc {
        name: "Branch",
        nested: &[(1, &LEAF), (2, &BRANCH)],
    };

    fn ops(buf: &[u8], desc: &MessageDesc) -> usize {
        let mut meter = Meter::new(usize::MAX);
        scan_message(buf, desc, &mut meter, 0).unwrap();
        meter.spent()
    }

    #[test]
    fn test_scan_charges_each_empty_element() {
        // The case a size bound cannot see: every element is tiny, but there are a great many.
        for count in [0, 1, 2, 512] {
            let branch = Branch {
                leaves: vec![Leaf::default(); count],
                ..Default::default()
            };

            // One op for each `leaves` occurrence. An empty `Leaf` has no fields of its own, and
            // default scalars are not encoded at all in proto3.
            assert_eq!(ops(&branch.encode_to_vec(), &BRANCH), count);
        }
    }

    #[test]
    fn test_scan_charges_opaque_payload_once() {
        // A megabyte in one field costs one op, where a byte meter would charge a megabyte.
        let branch = Branch {
            label: "a".repeat(1 << 20),
            ..Default::default()
        };

        assert_eq!(ops(&branch.encode_to_vec(), &BRANCH), 1);
    }

    #[test]
    fn test_scan_charges_every_depth() {
        let branch = Branch {
            leaves: vec![
                Leaf {
                    text: "one".to_owned(),
                    number: 1,
                    double: 1.5,
                    fixed: 2,
                    blob: vec![1, 2, 3],
                },
                Leaf {
                    number: 7,
                    ..Default::default()
                },
            ],
            branch: Some(Box::new(Branch {
                label: "inner".to_owned(),
                ..Default::default()
            })),
            label: "outer".to_owned(),
        };

        // Two `leaves` occurrences carrying five and one field, one `branch` carrying one field,
        // and the outer `label`.
        let expected = (1 + 5) + (1 + 1) + (1 + 1) + 1;
        assert_eq!(ops(&branch.encode_to_vec(), &BRANCH), expected);
    }

    #[test]
    fn test_scan_exceeds_budget() {
        let branch = Branch {
            leaves: vec![Leaf::default(); 4096],
            ..Default::default()
        };

        let error = scan(&branch.encode_to_vec(), &BRANCH, 128).unwrap_err();
        assert!(error.is_limit_exceeded());
        assert_eq!(error.to_string(), "message exceeds the operation limit");
    }

    #[test]
    fn test_scan_exceeds_budget_when_nested() {
        // The budget has to survive recursion: the fields are all four levels down.
        let leaves = vec![Leaf::default(); 4096];
        let branch = Branch {
            branch: Some(Box::new(Branch {
                branch: Some(Box::new(Branch {
                    leaves,
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };

        let error = scan(&branch.encode_to_vec(), &BRANCH, 256).unwrap_err();
        assert!(error.is_limit_exceeded());
    }

    #[test]
    fn test_decode_rejects_before_decoding() {
        let branch = Branch {
            leaves: vec![Leaf::default(); 4096],
            ..Default::default()
        };
        let payload = branch.encode_to_vec();

        assert!(decode::<Branch>(&payload, &BRANCH, 128).is_err());
        // The same payload decodes once the budget accommodates it.
        let decoded = decode::<Branch>(&payload, &BRANCH, 1 << 20).unwrap();
        assert_eq!(decoded, branch);
    }

    #[test]
    fn test_scan_accepts_what_prost_accepts() {
        // An unknown field prost would skip: tag 9, length delimited, holding a nested message the
        // scanner has no descriptor for. It is charged once and walked over.
        let payload = [0x4a, 0x04, 0x08, 0x01, 0x10, 0x02];

        assert_eq!(ops(&payload, &BRANCH), 1);
        assert!(Branch::decode(payload.as_slice()).is_ok());
        assert!(scan(&payload, &BRANCH, 1).is_ok());
    }

    #[test]
    fn test_scan_rejects_malformed_payloads() {
        // A truncated length delimiter, a tag of zero, and a varint with no terminator.
        for payload in [
            [0x1a, 0x08, 0x61].as_slice(),
            [0x00, 0x01].as_slice(),
            [0x08, 0xff].as_slice(),
        ] {
            let error = scan(payload, &BRANCH, 1 << 20).unwrap_err();
            assert!(!error.is_limit_exceeded(), "{payload:?}");
            // Whatever the scanner rejects, prost rejects too.
            assert!(Branch::decode(payload).is_err(), "{payload:?}");
        }
    }

    #[test]
    fn test_scan_bounds_its_own_recursion() {
        // Deeper than the recursion limit, so the scanner must not run out of stack walking it.
        let mut payload = Vec::new();
        for _ in 0..RECURSION_LIMIT + 10 {
            let mut framed = vec![0x12, payload.len() as u8];
            framed.extend_from_slice(&payload);
            payload = framed;
        }

        let error = scan(&payload, &BRANCH, 1 << 20).unwrap_err();
        assert!(!error.is_limit_exceeded());
        assert!(Branch::decode(payload.as_slice()).is_err());
    }
}
