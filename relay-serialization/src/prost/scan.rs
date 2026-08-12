use prost::{DecodeError, Message};
use std::fmt;

use crate::{LimitExceeded, Meter};

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

/// The kind of nested field encountered in the proto.  Either a regular tagged field, or
/// a "oneof" which will have its tags on the actual nested message,
pub enum Nested {
    /// A regular nested message field.
    Field(u32, fn() -> &'static [Nested]),

    /// A oneof nested field.
    Oneof(fn() -> &'static [Nested]),
}

/// A trait for prost Messages to implement, allowing them to self-describe the kinds of nested
/// fields that have on themselves.
pub trait BoundedMessage {
    /// Returns the one and only array of nested fields (fields that refer to other messages)
    /// on this particular Message.
    fn desc() -> &'static [Nested];
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
pub fn decode<M>(buf: &[u8], max_ops: usize) -> Result<M, Error>
where
    M: Message + Default + BoundedMessage,
{
    scan::<M>(buf, max_ops)?;
    M::decode(buf).map_err(Error::Decode)
}

/// Checks that the message in `buf` fits within `max_ops`, without decoding it.  Returns
/// the number of ops spent doing the scan.
pub fn scan<T: BoundedMessage + Message>(buf: &[u8], max_ops: usize) -> Result<usize, Error> {
    let mut meter = Meter::new(max_ops);

    match scan_message(buf, T::desc(), &mut meter, 0) {
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
fn find(table: &'static [Nested], tag: u32, limit: u32) -> Option<&'static [Nested]> {
    if limit == 0 {
        return None;
    }
    table.iter().find_map(|entry| match entry {
        Nested::Field(t, desc) if *t == tag => Some(desc()),
        Nested::Oneof(group) => find(group(), tag, limit - 1),
        _ => None,
    })
}

fn scan_message(
    buf: &[u8],
    desc: &'static [Nested],
    meter: &mut Meter,
    depth: u32,
) -> Result<(), Error> {
    if depth > RECURSION_LIMIT {
        return Err(Error::ScanError("recursion limit reached".to_owned()));
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
    desc: Option<&'static [Nested]>,
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
            if let Some(nested) = desc.and_then(|desc| find(desc, tag, RECURSION_LIMIT)) {
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

/// Returns the number of ops consumed deserializing the supplied message.
pub fn ops<T>(msg: &T) -> usize
where
    T: BoundedMessage + Message,
{
    let mut meter = Meter::new(usize::MAX);
    scan_message(&msg.encode_to_vec(), T::desc(), &mut meter, 0).unwrap();
    meter.spent()
}
