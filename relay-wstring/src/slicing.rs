//! The [SliceIndex] trait and implementations.
//!
//! This supports all slicing for [WStr].

use std::ops::{
    Index, IndexMut, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive,
};

use crate::WStr;

mod private {
    use super::*;

    pub trait SealedSliceIndex {}

    impl SealedSliceIndex for RangeFull {}
    impl SealedSliceIndex for Range<usize> {}
    impl SealedSliceIndex for RangeFrom<usize> {}
    impl SealedSliceIndex for RangeTo<usize> {}
    impl SealedSliceIndex for RangeInclusive<usize> {}
    impl SealedSliceIndex for RangeToInclusive<usize> {}
}
/// Our own version of [std::slice::SliceIndex].
///
/// Since this is a sealed trait, we need to re-define this trait.  This trait itself is
/// sealed as well.
pub trait SliceIndex<T>: private::SealedSliceIndex
where
    T: ?Sized,
{
    /// The result of slicing, another slice of the same type as you started with normally.
    type Output: ?Sized;

    /// Returns a shared reference to the output at this location, if in bounds.
    fn get(self, slice: &T) -> Option<&Self::Output>;

    /// Returns a mutable reference to the output at this location, if in bounds.
    fn get_mut(self, slice: &mut T) -> Option<&mut Self::Output>;

    /// Like [Self::get] but without bounds checking.
    ///
    /// # Safety
    ///
    /// You must guarantee the resulting slice is valid UTF-16LE, otherwise you will get
    /// undefined behavour.
    unsafe fn get_unchecked(self, slice: &T) -> &Self::Output;

    /// Like [Self::get_mut] but without bounds checking.
    ///
    /// # Safety
    ///
    /// You must guarantee the resulting slice is valid UTF-16LE, otherwise you will get
    /// undefined behavour.
    unsafe fn get_unchecked_mut(self, slice: &mut T) -> &mut Self::Output;

    /// Returns a shared reference to the output at this location, panicking if out of bounds.
    fn index(self, slice: &T) -> &Self::Output;

    /// Returns a mutable reference to the output at this location, panicking if out of bounds.
    fn index_mut(self, slice: &mut T) -> &mut Self::Output;
}

/// Implments substring slicing with syntax `&self[..]` or `&mut self[..]`.\
///
/// Unlike other implementations this can never panic.
impl SliceIndex<WStr> for RangeFull {
    type Output = WStr;

    #[inline]
    fn get(self, slice: &WStr) -> Option<&Self::Output> {
        Some(slice)
    }

    #[inline]
    fn get_mut(self, slice: &mut WStr) -> Option<&mut Self::Output> {
        Some(slice)
    }

    #[inline]
    unsafe fn get_unchecked(self, slice: &WStr) -> &Self::Output {
        slice
    }

    #[inline]
    unsafe fn get_unchecked_mut(self, slice: &mut WStr) -> &mut Self::Output {
        slice
    }

    #[inline]
    fn index(self, slice: &WStr) -> &Self::Output {
        slice
    }

    #[inline]
    fn index_mut(self, slice: &mut WStr) -> &mut Self::Output {
        slice
    }
}

/// Implements substring slicing with syntax `&self[begin .. end]` or `&mut self[begin .. end]`.
impl SliceIndex<WStr> for Range<usize> {
    type Output = WStr;

    #[inline]
    fn get(self, slice: &WStr) -> Option<&Self::Output> {
        if self.start <= self.end
            && slice.is_char_boundary(self.start)
            && slice.is_char_boundary(self.end)
        {
            Some(unsafe { self.get_unchecked(slice) })
        } else {
            None
        }
    }

    #[inline]
    fn get_mut(self, slice: &mut WStr) -> Option<&mut Self::Output> {
        if self.start <= self.end
            && slice.is_char_boundary(self.start)
            && slice.is_char_boundary(self.end)
        {
            Some(unsafe { self.get_unchecked_mut(slice) })
        } else {
            None
        }
    }

    #[inline]
    unsafe fn get_unchecked(self, slice: &WStr) -> &Self::Output {
        let ptr = slice.as_ptr().add(self.start);
        let len = self.end - self.start;
        WStr::from_utf16le_unchecked(std::slice::from_raw_parts(ptr, len))
    }

    #[inline]
    unsafe fn get_unchecked_mut(self, slice: &mut WStr) -> &mut Self::Output {
        let ptr = slice.as_mut_ptr().add(self.start);
        let len = self.end - self.start;
        WStr::from_utf16le_unchecked_mut(std::slice::from_raw_parts_mut(ptr, len))
    }

    #[inline]
    fn index(self, slice: &WStr) -> &Self::Output {
        self.get(slice).expect("slice index out of bounds")
    }

    #[inline]
    fn index_mut(self, slice: &mut WStr) -> &mut Self::Output {
        self.get_mut(slice).expect("slice index out of bounds")
    }
}

/// Implements substring slicing with syntax `&self[.. end]` or `&mut self[.. end]`.
impl SliceIndex<WStr> for RangeTo<usize> {
    type Output = WStr;

    #[inline]
    fn get(self, slice: &WStr) -> Option<&Self::Output> {
        if slice.is_char_boundary(self.end) {
            Some(unsafe { self.get_unchecked(slice) })
        } else {
            None
        }
    }

    #[inline]
    fn get_mut(self, slice: &mut WStr) -> Option<&mut Self::Output> {
        if slice.is_char_boundary(self.end) {
            Some(unsafe { self.get_unchecked_mut(slice) })
        } else {
            None
        }
    }

    #[inline]
    unsafe fn get_unchecked(self, slice: &WStr) -> &Self::Output {
        let ptr = slice.as_ptr();
        WStr::from_utf16le_unchecked(std::slice::from_raw_parts(ptr, self.end))
    }

    #[inline]
    unsafe fn get_unchecked_mut(self, slice: &mut WStr) -> &mut Self::Output {
        let ptr = slice.as_mut_ptr();
        WStr::from_utf16le_unchecked_mut(std::slice::from_raw_parts_mut(ptr, self.end))
    }

    #[inline]
    fn index(self, slice: &WStr) -> &Self::Output {
        self.get(slice).expect("slice index out of bounds")
    }

    #[inline]
    fn index_mut(self, slice: &mut WStr) -> &mut Self::Output {
        self.get_mut(slice).expect("slice index out of bounds")
    }
}

/// Implements substring slicing with syntax `&self[begin ..]` or `&mut self[begin ..]`.
impl SliceIndex<WStr> for RangeFrom<usize> {
    type Output = WStr;

    #[inline]
    fn get(self, slice: &WStr) -> Option<&Self::Output> {
        if slice.is_char_boundary(self.start) {
            Some(unsafe { self.get_unchecked(slice) })
        } else {
            None
        }
    }

    #[inline]
    fn get_mut(self, slice: &mut WStr) -> Option<&mut Self::Output> {
        if slice.is_char_boundary(self.start) {
            Some(unsafe { self.get_unchecked_mut(slice) })
        } else {
            None
        }
    }

    #[inline]
    unsafe fn get_unchecked(self, slice: &WStr) -> &Self::Output {
        let ptr = slice.as_ptr();
        let len = slice.len() - self.start;
        WStr::from_utf16le_unchecked(std::slice::from_raw_parts(ptr, len))
    }

    #[inline]
    unsafe fn get_unchecked_mut(self, slice: &mut WStr) -> &mut Self::Output {
        let ptr = slice.as_mut_ptr();
        let len = slice.len() - self.start;
        WStr::from_utf16le_unchecked_mut(std::slice::from_raw_parts_mut(ptr, len))
    }

    #[inline]
    fn index(self, slice: &WStr) -> &Self::Output {
        self.get(slice).expect("slice index out of bounds")
    }

    #[inline]
    fn index_mut(self, slice: &mut WStr) -> &mut Self::Output {
        self.get_mut(slice).expect("slice index out of bounds")
    }
}

/// Implements substring slicing with syntax `&self[begin ..= end]` or `&mut self[begin ..= end]`.
impl SliceIndex<WStr> for RangeInclusive<usize> {
    type Output = WStr;

    #[inline]
    fn get(self, slice: &WStr) -> Option<&Self::Output> {
        if *self.end() == usize::MAX {
            None
        } else {
            (*self.start()..self.end() + 1).get(slice)
        }
    }

    #[inline]
    fn get_mut(self, slice: &mut WStr) -> Option<&mut Self::Output> {
        if *self.end() == usize::MAX {
            None
        } else {
            (*self.start()..self.end() + 1).get_mut(slice)
        }
    }

    #[inline]
    unsafe fn get_unchecked(self, slice: &WStr) -> &Self::Output {
        (*self.start()..self.end() + 1).get_unchecked(slice)
    }

    #[inline]
    unsafe fn get_unchecked_mut(self, slice: &mut WStr) -> &mut Self::Output {
        (*self.start()..self.end() + 1).get_unchecked_mut(slice)
    }

    #[inline]
    fn index(self, slice: &WStr) -> &Self::Output {
        if *self.end() == usize::MAX {
            panic!("index overflow");
        }
        (*self.start()..self.end() + 1).index(slice)
    }

    #[inline]
    fn index_mut(self, slice: &mut WStr) -> &mut Self::Output {
        if *self.end() == usize::MAX {
            panic!("index overflow");
        }
        (*self.start()..self.end() + 1).index_mut(slice)
    }
}

/// Implements substring slicing with syntax `&self[..= end]` or `&mut self[..= end]`.
impl SliceIndex<WStr> for RangeToInclusive<usize> {
    type Output = WStr;

    #[inline]
    fn get(self, slice: &WStr) -> Option<&Self::Output> {
        if self.end == usize::MAX {
            None
        } else {
            (..self.end + 1).get(slice)
        }
    }

    #[inline]
    fn get_mut(self, slice: &mut WStr) -> Option<&mut Self::Output> {
        if self.end == usize::MAX {
            None
        } else {
            (..self.end + 1).get_mut(slice)
        }
    }

    #[inline]
    unsafe fn get_unchecked(self, slice: &WStr) -> &Self::Output {
        (..self.end + 1).get_unchecked(slice)
    }

    #[inline]
    unsafe fn get_unchecked_mut(self, slice: &mut WStr) -> &mut Self::Output {
        (..self.end + 1).get_unchecked_mut(slice)
    }

    #[inline]
    fn index(self, slice: &WStr) -> &Self::Output {
        if self.end == usize::MAX {
            panic!("index overflow");
        }
        (..self.end + 1).index(slice)
    }

    #[inline]
    fn index_mut(self, slice: &mut WStr) -> &mut Self::Output {
        if self.end == usize::MAX {
            panic!("index overflow");
        }
        (..self.end + 1).index_mut(slice)
    }
}

impl<I> Index<I> for WStr
where
    I: SliceIndex<WStr>,
{
    type Output = I::Output;

    #[inline]
    fn index(&self, index: I) -> &I::Output {
        index.index(self)
    }
}

impl<I> IndexMut<I> for WStr
where
    I: SliceIndex<WStr>,
{
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut I::Output {
        index.index_mut(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range() {
        let b = b"h\x00e\x00l\x00l\x00o\x00\x00\xd8\x00\xdc"; // "hello\u{10000}"
        let s = WStr::from_utf16le(b).unwrap();

        assert_eq!(s.get(0..14).unwrap(), s);

        // not char boundaries
        assert!(s.get(1..14).is_none());
        assert!(s.get(0..12).is_none());

        // out of range
        assert!(s.get(0..16).is_none());
    }

    #[test]
    fn test_range_to() {
        let b = b"h\x00e\x00l\x00l\x00o\x00\x00\xd8\x00\xdc"; // "hello\u{10000}"
        let s = WStr::from_utf16le(b).unwrap();

        assert_eq!(s.get(..14).unwrap(), s);

        // not char boundaries
        assert!(s.get(..9).is_none());
        assert!(s.get(..12).is_none());

        // out of range
        assert!(s.get(..16).is_none());
    }

    #[test]
    fn test_range_from() {
        let b = b"h\x00e\x00l\x00l\x00o\x00\x00\xd8\x00\xdc"; // "hello\u{10000}"
        let s = WStr::from_utf16le(b).unwrap();

        assert_eq!(s.get(0..).unwrap(), s);

        // not char boundaries
        assert!(s.get(1..).is_none());
        assert!(s.get(12..).is_none());

        // out of range
        assert!(s.get(16..).is_none());
    }

    #[test]
    fn test_range_inclusive() {
        let b = b"h\x00e\x00l\x00l\x00o\x00\x00\xd8\x00\xdc"; // "hello\u{10000}"
        let s = WStr::from_utf16le(b).unwrap();

        assert_eq!(s.get(0..=13).unwrap(), s);

        // not char boundaries
        assert!(s.get(0..=8).is_none());
        assert!(s.get(0..=11).is_none());

        // out of range
        assert!(s.get(0..=15).is_none());
    }

    #[test]
    fn test_range_to_inclusive() {
        let b = b"h\x00e\x00l\x00l\x00o\x00\x00\xd8\x00\xdc"; // "hello\u{10000}"
        let s = WStr::from_utf16le(b).unwrap();

        assert_eq!(s.get(..=13).unwrap(), s);

        // not char boundaries
        assert!(s.get(..=8).is_none());
        assert!(s.get(..=11).is_none());

        // out of range
        assert!(s.get(..=15).is_none());
    }

    #[test]
    fn test_range_full() {
        let b = b"h\x00e\x00l\x00l\x00o\x00\x00\xd8\x00\xdc"; // "hello\u{10000}"
        let s = WStr::from_utf16le(b).unwrap();

        assert_eq!(s.get(..).unwrap(), s);
    }
}
