use std::fmt;

/// Maximum size of bytes printed with [`DebugBytes`].
const DEBUG_BYTES_MAX_LEN: usize = 200;

/// Utility to print bytes with a maximum length.
pub struct DebugBytes<'a>(pub &'a [u8]);

impl<'a> fmt::Debug for DebugBytes<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "b\"")?;

        const ELLIPSIS: &str = "...";

        let (ellipsis, num_bytes) = match self.0.len() {
            // Account for the additional `...`
            len if len <= DEBUG_BYTES_MAX_LEN + ELLIPSIS.len() => (false, len),
            _ => (true, DEBUG_BYTES_MAX_LEN),
        };

        for &b in self.0.iter().take(num_bytes) {
            // https://doc.rust-lang.org/reference/tokens.html#byte-escapes
            if b == b'\n' {
                write!(f, "\\n")?;
            } else if b == b'\r' {
                write!(f, "\\r")?;
            } else if b == b'\t' {
                write!(f, "\\t")?;
            } else if b == b'\\' || b == b'"' {
                write!(f, "\\{}", b as char)?;
            } else if b == b'\0' {
                write!(f, "\\0")?;
            // ASCII printable
            } else if (0x20..0x7f).contains(&b) {
                write!(f, "{}", b as char)?;
            } else {
                write!(f, "\\x{:02x}", b)?;
            }
        }

        if ellipsis {
            write!(f, "...")?;
        }

        write!(f, "\"")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debug_bytes_max_len_fits_in_ellipsis() {
        // No need to print the ellipsis if we can print the data instead.
        let bytes = [b'x'; DEBUG_BYTES_MAX_LEN + 3];

        assert_eq!(
            format!("{:?}", DebugBytes(&bytes)),
            format!("b\"{}\"", "x".repeat(DEBUG_BYTES_MAX_LEN + 3)),
        );
    }

    #[test]
    fn test_debug_bytes_max_len_with_ellipsis() {
        // Data doesn't fit into the ellipsis.
        let bytes = [b'x'; DEBUG_BYTES_MAX_LEN + 4];

        assert_eq!(
            format!("{:?}", DebugBytes(&bytes)),
            format!("b\"{}...\"", "x".repeat(DEBUG_BYTES_MAX_LEN)),
        );
    }
}
