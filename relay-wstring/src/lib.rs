//! A UTF-16 little-endian string type.
//!
//! The main type in this crate is [WStr] which is a type similar to [str] but with the
//! underlying data encoded as UTF-16 with little-endian byte order.
//!
//! # Examples
//!
//! ```
//! use relay_wstring::WStr;
//!
//! let b = b"h\x00e\x00l\x00l\x00o\x00";
//! let s: &WStr = WStr::from_utf16le(b).unwrap();
//!
//! let chars: Vec<char> = s.chars().collect();
//! assert_eq!(chars, vec!['h', 'e', 'l', 'l', 'o']);
//!
//! assert_eq!(s.to_utf8(), "hello");
//! ```

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
pub struct Utf16Error {}

impl fmt::Display for Utf16Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Invalid UTF-16LE data in byte slice")
    }
}

impl Error for Utf16Error {}

impl Utf16Error {
    /// Create a new [Utf16Error].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Utf16Error {
    fn default() -> Self {
        Self::new()
    }
}

/// A UTF-16 [str]-like type with little-endian byte order.
///
/// This mostly behaves like [str] does for UTF-8 encoded bytes slices, but works with
/// UTF-16LE encoded byte slices.
///
/// See the [module-level documentation](index.html) for some simple examples.
#[derive(Debug, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct WStr {
    raw: [u8],
}

impl WStr {
    /// Create a new `&WStr` from an existing UTF-16 little-endian encoded byte-slice.
    ///
    /// If the byte-slice is not valid [Utf16Error] is returned.
    pub fn from_utf16le(raw: &[u8]) -> Result<&Self, Utf16Error> {
        validate_raw_utf16le(raw)?;
        Ok(unsafe { Self::from_utf16le_unchecked(raw) })
    }

    /// Create a new `&mut WStr` from an existing UTF-16 little-endian encoded byte-slice.
    ///
    /// If the byte-slice is not valid [Utf16Error] is returned.
    pub fn from_utf16le_mut(raw: &mut [u8]) -> Result<&mut Self, Utf16Error> {
        validate_raw_utf16le(raw)?;
        Ok(unsafe { Self::from_utf16le_unchecked_mut(raw) })
    }

    /// Create a new [WStr] from an existing UTF-16 little-endian encoded byte-slice.
    ///
    /// # Safety
    ///
    /// You must guarantee that the buffer passed in is encoded correctly otherwise you will
    /// get undefined behaviour.
    pub unsafe fn from_utf16le_unchecked(raw: &[u8]) -> &Self {
        &*(raw as *const [u8] as *const Self)
    }

    /// Like [Self::from_utf16le_unchecked] but return a mutable reference.
    ///
    /// # Safety
    ///
    /// You must guarantee that the buffer passed in is encoded correctly otherwise you will
    /// get undefined behaviour.
    pub unsafe fn from_utf16le_unchecked_mut(raw: &mut [u8]) -> &mut Self {
        &mut *(raw as *mut [u8] as *mut Self)
    }

    /// The length in bytes, not chars or graphemes.
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// Returns `true` if the [Self::len] is zero.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if the index into the bytes is on a char boundary.
    pub fn is_char_boundary(&self, index: usize) -> bool {
        if index == 0 || index == self.len() {
            return true;
        }
        if index % 2 != 0 {
            return false;
        }

        // Since we always have a valid UTF-16LE string in here we now are sure we always
        // have a byte at index + 1.  The only invalid thing now is a trailing surrogate.

        let mut buf = [0u8; 2];
        buf.copy_from_slice(&self.raw[index..index + 2]);
        let u = u16::from_le_bytes(buf);
        u & 0xDC00 != 0xDC00
    }

    /// Convert to a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.raw
    }

    /// Convert to a mutable byte slice.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.raw
    }

    /// Convert to a raw pointer to the byte slice.
    pub const fn as_ptr(&self) -> *const u8 {
        self.raw.as_ptr()
    }

    /// Convert to a mutable raw pointer to the byte slice.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.raw.as_mut_ptr()
    }

    /// Return a subslice of `self`.
    ///
    /// The slice indices are on byte offsets of the underlying UTF-16LE encoded buffer, if
    /// the subslice is not on character boundaries or otherwise invalid this will return
    /// `None`.
    pub fn get<I>(&self, index: I) -> Option<&<I as SliceIndex<WStr>>::Output>
    where
        I: SliceIndex<WStr>,
    {
        index.get(self)
    }

    /// Return a mutable subslice of `Self`.
    ///
    /// The slice indices are on byte offsets of the underlying UTF-16LE encoded buffer, if
    /// the subslice is not on character boundaries or otherwise invalid this will return
    /// `None`.
    pub fn get_mut<I>(&mut self, index: I) -> Option<&mut <I as SliceIndex<WStr>>::Output>
    where
        I: SliceIndex<WStr>,
    {
        index.get_mut(self)
    }

    /// Return a subslice of `Self`.
    ///
    /// # Safety
    ///
    /// Like [Self::get] but this results in undefined behaviour if the sublice is not on
    /// character boundaries or otherwise invalid.
    pub unsafe fn get_unchecked<I>(&self, index: I) -> &<I as SliceIndex<WStr>>::Output
    where
        I: SliceIndex<WStr>,
    {
        index.get_unchecked(self)
    }

    /// Return a mutable subslice of `Self`.
    ///
    /// # Safety
    ///
    /// Lice [Self::get_mut] but this results in undefined behaviour if the subslice is not
    /// on character boundaries or otherwise invalid.
    pub unsafe fn get_unchecked_mut<I>(&mut self, index: I) -> &mut <I as SliceIndex<WStr>>::Output
    where
        I: SliceIndex<WStr>,
    {
        index.get_unchecked_mut(self)
    }

    /// Returns an iterator of the [char]s of a string slice.
    pub fn chars(&self) -> WStrChars {
        WStrChars {
            chunks: self.raw.chunks_exact(2),
        }
    }

    /// Returns and iterator over the [char]s of a string slice and their positions.
    pub fn char_indices(&self) -> WStrCharIndices {
        WStrCharIndices {
            chars: self.chars(),
            index: 0,
        }
    }

    /// Returns the [WStr] as a new owned [String].
    pub fn to_utf8(&self) -> String {
        self.chars().collect()
    }

    /// Returns `true` if all characters in the string are ASCII.
    pub fn is_ascii(&self) -> bool {
        self.as_bytes().is_ascii()
    }
}

/// Iterator yielding `char` from a UTF-16 little-endian encoded byte slice.
///
/// The slice must contain valid UTF-16, otherwise this may panic or cause undefined
/// behaviour.
#[derive(Debug)]
pub struct WStrChars<'a> {
    chunks: ChunksExact<'a, u8>,
}

impl<'a> Iterator for WStrChars<'a> {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        // Our input is valid UTF-16LE, so we can take a lot of shortcuts.
        let chunk = self.chunks.next()?;
        let mut buf = [0u8; 2];
        buf.copy_from_slice(chunk);
        let u = u16::from_le_bytes(buf);

        if u < 0xD800 || 0xDFFF < u {
            // SAFETY: This is now guaranteed a valid Unicode code point.
            Some(unsafe { std::char::from_u32_unchecked(u as u32) })
        } else {
            debug_assert!(u < 0xDC00, "u16 not a leading surrogate");
            let chunk = self.chunks.next().expect("missing trailing surrogate");
            buf.copy_from_slice(chunk);
            let u2 = u16::from_le_bytes(buf);
            debug_assert!(
                0xDC00 <= u2 && u2 <= 0xDFFF,
                "u16 is not a trailing surrogate"
            );
            let c = (((u - 0xD800) as u32) << 10 | (u2 - 0xDC00) as u32) + 0x1_0000;
            // SAFETY: This is now guaranteed a valid Unicode code point.
            Some(unsafe { std::char::from_u32_unchecked(c) })
        }
    }
}

impl<'a> FusedIterator for WStrChars<'a> {}

/// Iterator yielding `(index, char)` tuples from a UTF-16 little-endian encoded byte slice.
///
/// The slice must contain valid UTF-16, otherwise this may panic or cause undefined
/// behaviour.
#[derive(Debug)]
pub struct WStrCharIndices<'a> {
    chars: WStrChars<'a>,
    index: usize,
}

impl<'a> Iterator for WStrCharIndices<'a> {
    type Item = (usize, char);

    fn next(&mut self) -> Option<Self::Item> {
        let pos = self.index;
        let c = self.chars.next()?;
        self.index += c.len_utf16() * std::mem::size_of::<u16>();
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

impl AsMut<[u8]> for WStr {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_bytes_mut()
    }
}

/// Check that the raw bytes are valid UTF-16LE.
fn validate_raw_utf16le(raw: &[u8]) -> Result<(), Utf16Error> {
    // This could be optimised as it does not need to be actually decoded, just needs to
    // be a valid byte sequence.
    if raw.len() % 2 != 0 {
        return Err(Utf16Error::new());
    }
    let u16iter = raw.chunks_exact(2).map(|chunk| {
        let mut buf = [0u8; 2];
        buf.copy_from_slice(chunk);
        u16::from_le_bytes(buf)
    });
    for c in std::char::decode_utf16(u16iter) {
        match c {
            Ok(_) => (),
            Err(_) => return Err(Utf16Error::new()),
        }
    }
    Ok(())
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
        let buf = s.as_bytes_mut();
        let world = b"w\x00o\x00r\x00l\x00d\x00";
        buf.copy_from_slice(world);
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
        let buf = t.as_bytes_mut();
        buf.copy_from_slice(b"x\x00");

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

        let b = b"\x00\xd8\x00\xdcx\x00";
        let s = WStr::from_utf16le(b).unwrap();
        let chars: Vec<char> = s.chars().collect();
        assert_eq!(chars, vec!['\u{10000}', 'x']);
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
    fn test_wstr_as_mut() {
        let mut b = Vec::from(&b"h\x00e\x00l\x00l\x00o\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();
        let m: &mut [u8] = s.as_mut();
        let world = b"w\x00o\x00r\x00l\x00d\x00";
        m.copy_from_slice(world);
        assert_eq!(b.as_slice(), world);
    }
}
