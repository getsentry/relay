//! A UTF-16 little-endian string type.
//!
//! The main type in this crate is [`WStr`] which is a type similar to [`str`] but with the
//! underlying data encoded as UTF-16 with little-endian byte order.
//!
//! See [`WStr`] for examples.

#![deny(missing_docs, missing_debug_implementations)]

use std::error::Error;
use std::fmt;
use std::iter::FusedIterator;
use std::slice::ChunksExact;

mod slicing;

#[doc(inline)]
pub use crate::slicing::SliceIndex;

/// Error for invalid UTF-16 encoded bytes.
#[derive(Debug, Copy, Clone)]
pub struct Utf16Error {
    valid_up_to: usize,
    error_len: Option<u8>,
}

impl fmt::Display for Utf16Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid UTF-16LE data in byte slice")
    }
}

impl Error for Utf16Error {}

impl Utf16Error {
    /// Returns the index in given bytes up to which valid UTF-16 was verified.
    pub fn valid_up_to(&self) -> usize {
        self.valid_up_to
    }

    /// Return the length of the error if it is recoverable.
    ///
    ///  - `None`: the end of the input was reached unexpectedly.  [`Utf16Error::valid_up_to`] is 1
    ///    to 3 bytes from the end of the input.  If a byte stream such as a file or a network
    ///    socket is being decoded incrementally, this could still be a valid char whose byte
    ///    sequence is spanning multiple chunks.
    ///
    /// - `Some(len)`: an unexpected byte was encountered.  The length provided is that of the
    ///   invalid byte sequence that starts at the index given by [`Utf16Error::valid_up_to`].
    ///   Decoding should resume after that sequence (after inserting a [`U+FFFD REPLACEMENT
    ///   CHARACTER`](std::char::REPLACEMENT_CHARACTER)) in case of lossy decoding.  In fact for UTF-16 the `len` reported here will
    ///   always be exactly 2 since this never looks ahead to see if the bytes following the error
    ///   sequence are valid as well as otherwise you would not know how many replacement characters
    ///   to insert when writing a lossy decoder.
    ///
    /// The semantics of this API are compatible with the semantics of
    /// [`Utf8Error`](std::str::Utf8Error).
    pub fn error_len(&self) -> Option<usize> {
        self.error_len.map(|len| len.into())
    }
}

/// A UTF-16 [`str`]-like type with little-endian byte order.
///
/// This mostly behaves like [`str`] does for UTF-8 encoded bytes slices, but works with
/// UTF-16LE encoded byte slices.
///
/// # Examples
///
/// ```
/// use relay_wstring::WStr;
///
/// let b = b"h\x00e\x00l\x00l\x00o\x00";
/// let s: &WStr = WStr::from_utf16le(b).unwrap();
///
/// let chars: Vec<char> = s.chars().collect();
/// assert_eq!(chars, vec!['h', 'e', 'l', 'l', 'o']);
///
/// assert_eq!(s.to_utf8(), "hello");
/// ```
#[derive(Debug, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct WStr {
    raw: [u8],
}

impl WStr {
    /// Creates a new `&WStr` from an existing UTF-16 little-endian encoded byte-slice.
    ///
    /// If the byte-slice is not valid [`Utf16Error`] is returned.
    pub fn from_utf16le(raw: &[u8]) -> Result<&Self, Utf16Error> {
        validate_raw_utf16le(raw)?;
        Ok(unsafe { Self::from_utf16le_unchecked(raw) })
    }

    /// Creates a new `&mut WStr` from an existing UTF-16 little-endian encoded byte-slice.
    ///
    /// If the byte-slice is not valid [`Utf16Error`] is returned.
    pub fn from_utf16le_mut(raw: &mut [u8]) -> Result<&mut Self, Utf16Error> {
        validate_raw_utf16le(raw)?;
        Ok(unsafe { Self::from_utf16le_unchecked_mut(raw) })
    }

    /// Creates a new [`WStr`] from an existing UTF-16 little-endian encoded byte-slice.
    ///
    /// # Safety
    ///
    /// You must guarantee that the buffer passed in is encoded correctly otherwise you will
    /// get undefined behavior.
    pub unsafe fn from_utf16le_unchecked(raw: &[u8]) -> &Self {
        &*(raw as *const [u8] as *const Self)
    }

    /// Like [`WStr::from_utf16le_unchecked`] but return a mutable reference.
    ///
    /// # Safety
    ///
    /// You must guarantee that the buffer passed in is encoded correctly otherwise you will
    /// get undefined behavior.
    pub unsafe fn from_utf16le_unchecked_mut(raw: &mut [u8]) -> &mut Self {
        &mut *(raw as *mut [u8] as *mut Self)
    }

    /// The length in bytes, not chars or graphemes.
    #[inline]
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// Returns `true` if the [`WStr::len`] is zero.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the index into the bytes is on a char boundary.
    #[inline]
    pub fn is_char_boundary(&self, index: usize) -> bool {
        if index == 0 || index == self.len() {
            return true;
        }
        if index % 2 != 0 {
            return false;
        }

        // Since we always have a valid UTF-16LE string in here we are sure we always have
        // another code unit if this one is a leading surrogate.  The only invalid thing now
        // is a trailing surrogate.  Our index can still be out of range though.
        self.raw
            .get(index..)
            .and_then(u16::copy_from_le_bytes)
            .map_or(false, |u| !is_trailing_surrogate(u))
    }

    /// Converts to a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.raw
    }

    /// Converts to a mutable byte slice.
    ///
    /// # Safety
    ///
    /// When mutating the bytes it must still be valid little-endian encoded UTF-16
    /// otherwise you will get undefined behavior.
    #[inline]
    pub unsafe fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.raw
    }

    /// Converts to a raw pointer to the byte slice.
    #[inline]
    pub const fn as_ptr(&self) -> *const u8 {
        self.raw.as_ptr()
    }

    /// Converts to a mutable raw pointer to the byte slice.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.raw.as_mut_ptr()
    }

    /// Returns a subslice of `WStr`.
    ///
    /// The slice indices are on byte offsets of the underlying UTF-16LE encoded buffer, if
    /// the subslice is not on character boundaries or otherwise invalid this will return
    /// `None`.
    #[inline]
    pub fn get<I>(&self, index: I) -> Option<&<I as SliceIndex<WStr>>::Output>
    where
        I: SliceIndex<WStr>,
    {
        index.get(self)
    }

    /// Returns a mutable subslice of `WStr`.
    ///
    /// The slice indices are on byte offsets of the underlying UTF-16LE encoded buffer, if
    /// the subslice is not on character boundaries or otherwise invalid this will return
    /// `None`.
    #[inline]
    pub fn get_mut<I>(&mut self, index: I) -> Option<&mut <I as SliceIndex<WStr>>::Output>
    where
        I: SliceIndex<WStr>,
    {
        index.get_mut(self)
    }

    /// Returns a subslice of `WStr`.
    ///
    /// # Safety
    ///
    /// Like [`WStr::get`] but this results in undefined behavior if the sublice is not on
    /// character boundaries or otherwise invalid.
    #[inline]
    pub unsafe fn get_unchecked<I>(&self, index: I) -> &<I as SliceIndex<WStr>>::Output
    where
        I: SliceIndex<WStr>,
    {
        index.get_unchecked(self)
    }

    /// Returns a mutable subslice of `WStr`.
    ///
    /// # Safety
    ///
    /// Like [`WStr::get_mut`] but this results in undefined behavior if the subslice is not
    /// on character boundaries or otherwise invalid.
    #[inline]
    pub unsafe fn get_unchecked_mut<I>(&mut self, index: I) -> &mut <I as SliceIndex<WStr>>::Output
    where
        I: SliceIndex<WStr>,
    {
        index.get_unchecked_mut(self)
    }

    /// Returns an iterator of the [`char`]s of a string slice.
    #[inline]
    pub fn chars(&self) -> WStrChars {
        WStrChars {
            chunks: self.raw.chunks_exact(2),
        }
    }

    /// Returns and iterator over the [`char`]s of a string slice and their positions.
    #[inline]
    pub fn char_indices(&self) -> WStrCharIndices {
        WStrCharIndices {
            chars: self.chars(),
            index: 0,
        }
    }

    /// Returns the [`WStr`] as a new owned [`String`].
    pub fn to_utf8(&self) -> String {
        self.chars().collect()
    }

    /// Returns `true` if all characters in the string are ASCII.
    #[inline]
    pub fn is_ascii(&self) -> bool {
        self.as_bytes().is_ascii()
    }
}

/// Iterator yielding [`char`] from a UTF-16 little-endian encoded byte slice.
///
/// The slice must contain valid UTF-16, otherwise this may panic or cause undefined
/// behavior.
#[derive(Debug)]
pub struct WStrChars<'a> {
    chunks: ChunksExact<'a, u8>,
}

impl<'a> Iterator for WStrChars<'a> {
    type Item = char;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // Our input is valid UTF-16LE, so we can take a lot of shortcuts.
        let chunk = self.chunks.next()?;
        // Chunks always returns 2 bytes, avoid generating panicking code
        let u = u16::copy_from_le_bytes(chunk).unwrap_or_default();

        if !is_leading_surrogate(u) {
            // SAFETY: This is now guaranteed a valid Unicode code point.
            Some(unsafe { std::char::from_u32_unchecked(u.into()) })
        } else {
            let chunk = self.chunks.next().expect("missing trailing surrogate");
            // Chunks always returns 2 bytes, avoid generating panicking code
            let u2 = u16::copy_from_le_bytes(chunk).unwrap_or_default();
            debug_assert!(
                is_trailing_surrogate(u2),
                "code unit not a trailing surrogate"
            );

            Some(unsafe { decode_surrogates(u, u2) })
        }
    }

    #[inline]
    fn count(self) -> usize {
        // No need to fully construct all characters
        self.chunks
            // Using filter_map to avoid generating panicking code
            .filter_map(|chunk| u16::copy_from_le_bytes(chunk))
            .filter(|&unit| !is_trailing_surrogate(unit))
            .count()
    }

    #[inline]
    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }
}

impl<'a> FusedIterator for WStrChars<'a> {}

impl<'a> DoubleEndedIterator for WStrChars<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        // Our input is valid UTF-16LE, so we can take a lot of shortcuts.
        let chunk = self.chunks.next_back()?;
        // Chunks always returns 2 bytes, avoid generating panicking code
        let u = u16::copy_from_le_bytes(chunk).unwrap_or_default();

        if !is_trailing_surrogate(u) {
            // SAFETY: This is now guaranteed a valid Unicode code point.
            Some(unsafe { std::char::from_u32_unchecked(u as u32) })
        } else {
            let chunk = self.chunks.next_back().expect("missing leading surrogate");
            // Chunks always returns 2 bytes, avoid generating panicking code
            let u2 = u16::copy_from_le_bytes(chunk).unwrap_or_default();
            debug_assert!(
                is_leading_surrogate(u2),
                "code unit not a leading surrogate"
            );

            Some(unsafe { decode_surrogates(u2, u) })
        }
    }
}

/// An iterator over the [`char`]s of a UTF-16 little-endian encoded byte slice, and their
/// positions.
///
/// The slice must contain valid UTF-16, otherwise this may panic or cause undefined behavior.
#[derive(Debug)]
pub struct WStrCharIndices<'a> {
    chars: WStrChars<'a>,
    index: usize,
}

impl<'a> Iterator for WStrCharIndices<'a> {
    type Item = (usize, char);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.index;
        let c = self.chars.next()?;
        self.index += c.len_utf16() * std::mem::size_of::<u16>();
        Some((pos, c))
    }

    #[inline]
    fn count(self) -> usize {
        // No need to fully construct all characters
        self.chars.count()
    }

    #[inline]
    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }
}

impl<'a> DoubleEndedIterator for WStrCharIndices<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        let c = self.chars.next_back()?;
        let pos = self.index + self.chars.chunks.len() * std::mem::size_of::<u16>();
        Some((pos, c))
    }
}

impl<'a> FusedIterator for WStrCharIndices<'a> {}

impl AsRef<[u8]> for WStr {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Display for WStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_utf8())
    }
}

/// Whether a code unit is a leading or high surrogate.
///
/// If a Unicode code point does not fit in one code unit (i.e. in one [`u16`]) it is split
/// into two code units called a *surrogate pair*.  The first code unit of this pair is the
/// *leading surrogate* and since it carries the high bits of the complete Unicode code
/// point it is also known as the *high surrogate*.
///
/// These surrogate code units have the first 6 bits set to a fixed prefix identifying
/// whether they are the *leading* or *trailing* code unit of the surrogate pair.  And for
/// the leading surrogate this bit prefix is `110110`, thus all leading surrogates have a
/// between 0xD800-0xDBFF.
#[inline]
fn is_leading_surrogate(code_unit: u16) -> bool {
    code_unit & 0xFC00 == 0xD800
}

/// Whether a code unit is a trailing or low surrogate.
///
/// If a Unicode code point does not fit in one code unit (i.e. in one [`u16`]) it is split
/// into two code units called a *surrogate pair*.  The second code unit of this pair is the
/// *trailing surrogate* and since it carries the low bits of the complete Unicode code
/// point it is also know as the *low surrogate*.
///
/// These surrogate code unites have the first 6 bits set to a fixed prefix identifying
/// whether tye are the *leading* or *trailing* code unit of the surrogate pair.  Anf for
/// the trailing surrogate this bit prefix is `110111`, thus all trailing surrogates have a
/// code unit between 0xDC00-0xDFFF.
#[inline]
fn is_trailing_surrogate(code_unit: u16) -> bool {
    code_unit & 0xFC00 == 0xDC00
}

/// Decodes a surrogate pair of code units into a [`char`].
///
/// This results in undefined behavior if the code units do not form a valid surrogate
/// pair.
#[inline]
unsafe fn decode_surrogates(u: u16, u2: u16) -> char {
    #![allow(unused_unsafe)]
    debug_assert!(
        is_leading_surrogate(u),
        "first code unit not a leading surrogate"
    );
    debug_assert!(
        is_trailing_surrogate(u2),
        "second code unit not a trailing surrogate"
    );
    let c = (((u - 0xD800) as u32) << 10 | (u2 - 0xDC00) as u32) + 0x1_0000;
    // SAFETY: This is now guaranteed a valid Unicode code point.
    unsafe { std::char::from_u32_unchecked(c) }
}

/// Checks that the raw bytes are valid UTF-16LE.
///
/// When an error occurs this code needs to set `error_len` to skip forward 2 bytes,
/// *unless* we have a lone leading surrogate and are at the end of the input slice in which
/// case we need to return `None.
///
/// We compute `valid_up_to` lazily for performance, even though it's a little more verbose.
fn validate_raw_utf16le(raw: &[u8]) -> Result<(), Utf16Error> {
    let base_ptr = raw.as_ptr() as usize;
    let mut chunks = raw.chunks_exact(std::mem::size_of::<u16>());

    while let Some(chunk) = chunks.next() {
        let code_point_ptr = chunk.as_ptr() as usize;
        // Chunks always returns 2 bytes, avoid generating panicking code
        let u = u16::copy_from_le_bytes(chunk).unwrap_or_default();

        if is_trailing_surrogate(u) {
            return Err(Utf16Error {
                valid_up_to: code_point_ptr - base_ptr,
                error_len: Some(std::mem::size_of::<u16>() as u8),
            });
        }

        if is_leading_surrogate(u) {
            match chunks.next().and_then(u16::copy_from_le_bytes) {
                Some(u2) => {
                    if !is_trailing_surrogate(u2) {
                        return Err(Utf16Error {
                            valid_up_to: code_point_ptr - base_ptr,
                            error_len: Some(std::mem::size_of::<u16>() as u8),
                        });
                    }
                }
                None => {
                    return Err(Utf16Error {
                        valid_up_to: code_point_ptr - base_ptr,
                        error_len: None,
                    });
                }
            }
        }
    }

    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        return Err(Utf16Error {
            valid_up_to: remainder.as_ptr() as usize - base_ptr,
            error_len: None,
        });
    }

    Ok(())
}

/// Private [`u16`] extension trait.
trait U16Ext {
    /// Decodes the next two bytes from the given slice into a new u16.
    ///
    /// If there are fewer than 2 bytes in the slice, `None` is returned.
    fn copy_from_le_bytes(bytes: &[u8]) -> Option<u16>;
}

impl U16Ext for u16 {
    #[inline]
    fn copy_from_le_bytes(bytes: &[u8]) -> Option<u16> {
        if bytes.len() >= 2 {
            Some(u16::from_le_bytes([bytes[0], bytes[1]]))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wstr_from_utf16le() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        assert_eq!(s.to_utf8(), "hello");

        // Odd number of bytes
        let b = b"h\x00e\x00l\x00l\x00o";
        let s = WStr::from_utf16le(b);
        assert!(s.is_err());

        // Lone leading surrogate
        let b = b"\x00\xd8x\x00";
        let s = WStr::from_utf16le(b);
        assert!(s.is_err());

        // Lone trailing surrogate
        let b = b"\x00\xdcx\x00";
        let s = WStr::from_utf16le(b);
        assert!(s.is_err());
    }

    #[test]
    fn test_wstr_utf16error() {
        // Lone trailing surrogate in 2nd char
        let b = b"h\x00\x00\xdce\x00l\x00l\x00o\x00";
        let e = WStr::from_utf16le(b).err().unwrap();
        assert_eq!(e.valid_up_to(), 2);
        assert_eq!(e.error_len(), Some(2));

        let head = WStr::from_utf16le(&b[..e.valid_up_to()]).unwrap();
        assert_eq!(head.to_utf8(), "h");

        let start = e.valid_up_to() + e.error_len().unwrap();
        let tail = WStr::from_utf16le(&b[start..]).unwrap();
        assert_eq!(tail.to_utf8(), "ello");

        // Leading surrogate, missing trailing surrogate in 2nd char
        let b = b"h\x00\x00\xd8e\x00l\x00l\x00o\x00";
        let e = WStr::from_utf16le(b).err().unwrap();
        assert_eq!(e.valid_up_to(), 2);
        assert_eq!(e.error_len(), Some(2));

        let head = WStr::from_utf16le(&b[..e.valid_up_to()]).unwrap();
        assert_eq!(head.to_utf8(), "h");

        let start = e.valid_up_to() + e.error_len().unwrap();
        let tail = WStr::from_utf16le(&b[start..]).unwrap();
        assert_eq!(tail.to_utf8(), "ello");

        // End of input
        let b = b"h\x00e\x00l\x00l\x00o\x00\x00\xd8";
        let e = WStr::from_utf16le(b).err().unwrap();
        assert_eq!(e.valid_up_to(), 10);
        assert_eq!(e.error_len(), None);

        // End of input, single byte
        let b = b"h\x00e\x00l\x00l\x00o\x00 ";
        let e = WStr::from_utf16le(b).err().unwrap();
        assert_eq!(e.valid_up_to(), 10);
        assert_eq!(e.error_len(), None);
    }

    #[test]
    fn test_wstr_from_utf16le_unchecked() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = unsafe { WStr::from_utf16le_unchecked(b) };
        assert_eq!(s.to_utf8(), "hello");
    }

    #[test]
    fn test_wstr_len() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        assert_eq!(s.len(), b.len());
    }

    #[test]
    fn test_wstr_is_empty() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        assert!(!s.is_empty());

        let s = WStr::from_utf16le(b"").unwrap();
        assert!(s.is_empty());
    }

    #[test]
    fn test_wstr_is_char_boundary() {
        let b = b"\x00\xd8\x00\xdcx\x00"; // "\u{10000}\u{78}"
        let s = WStr::from_utf16le(b).unwrap();
        assert!(s.is_char_boundary(0));
        assert!(!s.is_char_boundary(1));
        assert!(!s.is_char_boundary(2));
        assert!(!s.is_char_boundary(3));
        assert!(s.is_char_boundary(4));
        assert!(!s.is_char_boundary(5));
        assert!(s.is_char_boundary(6));
        assert!(!s.is_char_boundary(7)); // out of range
        assert!(!s.is_char_boundary(8)); // out of range
    }

    #[test]
    fn test_wstr_as_bytes() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        assert_eq!(s.as_bytes(), b);
    }

    #[test]
    fn test_wstr_as_bytes_mut() {
        let mut b = Vec::from(&b"h\x00e\x00l\x00l\x00o\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();
        let world = b"w\x00o\x00r\x00l\x00d\x00";
        unsafe {
            let buf = s.as_bytes_mut();
            buf.copy_from_slice(world);
        }
        assert_eq!(b.as_slice(), world);
    }

    #[test]
    fn test_wstr_get() {
        // This is implemented with get_unchecked() so this is also already tested.
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();

        let t = s.get(0..8).expect("expected Some(&WStr)");
        assert_eq!(t.as_bytes(), b"h\x00e\x00l\x00l\x00");

        let t = s.get(1..8);
        assert!(t.is_none());
    }

    #[test]
    fn test_wstr_get_mut() {
        // This is implemented with get_unchecked_mut() so this is also already tested.
        let mut b = Vec::from(&b"h\x00e\x00l\x00l\x00o\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();

        let t = s.get_mut(0..2).expect("expected Some(&mut Wstr)");
        unsafe {
            let buf = t.as_bytes_mut();
            buf.copy_from_slice(b"x\x00");
        }

        assert_eq!(s.as_bytes(), b"x\x00e\x00l\x00l\x00o\x00");
    }

    #[test]
    fn test_wstr_slice() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let sub = &s[2..8];
        assert_eq!(sub.as_bytes(), b"e\x00l\x00l\x00");
    }

    #[test]
    #[should_panic]
    fn test_wstr_bad_index() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let _r = &s[2..7];
    }

    #[test]
    fn test_wstr_chars() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<char> = s.chars().collect();
        assert_eq!(chars, vec!['h', 'e', 'l', 'l', 'o']);

        let b = b"\x00\xd8\x00\xdcA\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<char> = s.chars().collect();
        assert_eq!(chars, vec!['\u{10000}', 'A']);

        // regression test for `is_leading_surrogate`: bit pattern of 0xf8 is 0b1111, which has all
        // bits of `0b1101` set but is outside of the surrogate range
        let b = b"\x41\xf8A\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<char> = s.chars().collect();
        assert_eq!(chars, vec!['\u{f841}', 'A']);
    }

    #[test]
    fn test_wstr_chars_reverse() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<char> = s.chars().rev().collect();
        assert_eq!(chars, vec!['o', 'l', 'l', 'e', 'h']);

        let b = b"\x00\xd8\x00\xdcA\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<char> = s.chars().rev().collect();
        assert_eq!(chars, vec!['A', '\u{10000}']);

        // regression test for `is_leading_surrogate`: bit pattern of 0xf8 is 0b1111, which has all
        // bits of `0b1101` set but is outside of the surrogate range
        let b = b"\x41\xf8A\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<char> = s.chars().rev().collect();
        assert_eq!(chars, vec!['A', '\u{f841}']);
    }

    #[test]
    fn test_wstr_chars_last() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let c = s.chars().last().unwrap();
        assert_eq!(c, 'o');

        let b = b"\x00\xd8\x00\xdcx\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let c = s.chars().last().unwrap();
        assert_eq!(c, 'x');
    }

    #[test]
    fn test_wstr_chars_count() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let n = s.chars().count();
        assert_eq!(n, 5);

        let b = b"\x00\xd8\x00\xdcx\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let n = s.chars().count();
        assert_eq!(n, 2);

        // regression test for `is_leading_surrogate`: bit pattern of 0xf8 is 0b1111, which has all
        // bits of `0b1101` set but is outside of the surrogate range
        let b = b"\x41\xf8A\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let n = s.chars().count();
        assert_eq!(n, 2);
    }

    #[test]
    fn test_wstr_char_indices() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<(usize, char)> = s.char_indices().collect();
        assert_eq!(
            chars,
            vec![(0, 'h'), (2, 'e'), (4, 'l'), (6, 'l'), (8, 'o')]
        );

        let b = b"\x00\xd8\x00\xdcx\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<(usize, char)> = s.char_indices().collect();
        assert_eq!(chars, vec![(0, '\u{10000}'), (4, 'x')]);

        // regression test for `is_leading_surrogate`: bit pattern of 0xf8 is 0b1111, which has all
        // bits of `0b1101` set but is outside of the surrogate range
        let b = b"\x41\xf8A\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<(usize, char)> = s.char_indices().collect();
        assert_eq!(chars, vec![(0, '\u{f841}'), (2, 'A')]);
    }

    #[test]
    fn test_wstr_char_indices_reverse() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<(usize, char)> = s.char_indices().rev().collect();
        assert_eq!(
            chars,
            vec![(8, 'o'), (6, 'l'), (4, 'l'), (2, 'e'), (0, 'h')]
        );

        let b = b"\x00\xd8\x00\xdcx\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<(usize, char)> = s.char_indices().rev().collect();
        assert_eq!(chars, vec![(4, 'x'), (0, '\u{10000}')]);

        // regression test for `is_leading_surrogate`: bit pattern of 0xf8 is 0b1111, which has all
        // bits of `0b1101` set but is outside of the surrogate range
        let b = b"\x41\xf8A\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<(usize, char)> = s.char_indices().rev().collect();
        assert_eq!(chars, vec![(2, 'A'), (0, '\u{f841}')]);
    }

    #[test]
    fn test_wstr_char_indices_last() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let c = s.char_indices().last().unwrap();
        assert_eq!(c, (8, 'o'));

        let b = b"\x00\xd8\x00\xdcx\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let c = s.char_indices().last().unwrap();
        assert_eq!(c, (4, 'x'));
    }

    #[test]
    fn test_wstr_char_indices_count() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let n = s.char_indices().count();
        assert_eq!(n, 5);

        let b = b"\x00\xd8\x00\xdcx\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let n = s.char_indices().count();
        assert_eq!(n, 2);
    }

    #[test]
    fn test_wstr_to_utf8() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let out: String = s.to_utf8();
        assert_eq!(out, "hello");
    }

    #[test]
    fn test_wstr_is_ascii() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        assert!(s.is_ascii());

        let b = b"\x00\xd8\x00\xdcx\x00";
        let s = WStr::from_utf16le(b).unwrap();
        assert!(!s.is_ascii());
    }

    #[test]
    fn test_wstr_as_ref() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let r: &[u8] = s.as_ref();
        assert_eq!(r, b);
    }

    #[test]
    fn test_display() {
        let b = b"h\x00e\x00l\x00l\x00o\x00";
        let s = WStr::from_utf16le(b).unwrap();
        assert_eq!(format!("{}", s), "hello");
    }
}
