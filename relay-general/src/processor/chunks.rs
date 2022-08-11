//! Utilities for dealing with annotated strings.
//!
//! This module contains the `split` and `join` function to destructure and recombine strings by
//! redaction remarks. This allows to quickly inspect modified sections of a string.
//!
//! ### Example
//!
//! ```
//! use relay_general::processor;
//! use relay_general::types::{Meta, Remark, RemarkType};
//!
//! let remarks = vec![Remark::with_range(
//!     RemarkType::Substituted,
//!     "myrule",
//!     (7, 17),
//! )];
//!
//! let chunks = processor::split_chunks("Hello, [redacted]!", &remarks);
//! let (joined, join_remarks) = processor::join_chunks(chunks);
//!
//! assert_eq!(joined, "Hello, [redacted]!");
//! assert_eq!(join_remarks, remarks);
//! ```

use std::borrow::Cow;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::types::{Meta, Remark, RemarkType};

/// A type for dealing with chunks of annotated text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Chunk<'a> {
    /// Unmodified text chunk.
    Text {
        /// The text value of the chunk
        text: Cow<'a, str>,
    },
    /// Redacted text chunk with a note.
    Redaction {
        /// The redacted text value
        text: Cow<'a, str>,
        /// The rule that crated this redaction
        rule_id: Cow<'a, str>,
        /// Type type of remark for this redaction
        #[serde(rename = "remark")]
        ty: RemarkType,
    },
}

impl<'a> Chunk<'a> {
    /// The text of this chunk.
    pub fn as_str(&self) -> &str {
        match self {
            Chunk::Text { text } => text,
            Chunk::Redaction { text, .. } => text,
        }
    }

    /// Effective length of the text in this chunk.
    pub fn len(&self) -> usize {
        self.as_str().len()
    }

    /// The number of UTF-8 encoded Unicode codepoints in this chunk.
    pub fn count(&self) -> usize {
        bytecount::num_chars(self.as_str().as_bytes())
    }

    /// Determines whether this chunk is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Display for Chunk<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Chunks the given text based on remarks.
pub fn split_chunks<'a, I>(text: &'a str, remarks: I) -> Vec<Chunk<'a>>
where
    I: IntoIterator<Item = &'a Remark>,
{
    let mut rv = vec![];
    let mut pos = 0;

    for remark in remarks {
        let (from, to) = match remark.range() {
            Some(range) => *range,
            None => continue,
        };

        if from > pos {
            if let Some(piece) = text.get(pos..from) {
                rv.push(Chunk::Text {
                    text: Cow::Borrowed(piece),
                });
            } else {
                break;
            }
        }
        if let Some(piece) = text.get(from..to) {
            rv.push(Chunk::Redaction {
                text: Cow::Borrowed(piece),
                rule_id: remark.rule_id().into(),
                ty: remark.ty(),
            });
        } else {
            break;
        }
        pos = to;
    }

    if pos < text.len() {
        if let Some(piece) = text.get(pos..) {
            rv.push(Chunk::Text {
                text: Cow::Borrowed(piece),
            });
        }
    }

    rv
}

/// Concatenates chunks into a string and emits remarks for redacted sections.
pub fn join_chunks<'a, I>(chunks: I) -> (String, Vec<Remark>)
where
    I: IntoIterator<Item = Chunk<'a>>,
{
    let mut rv = String::new();
    let mut remarks = vec![];
    let mut pos = 0;

    for chunk in chunks {
        let new_pos = pos + chunk.len();
        rv.push_str(chunk.as_str());

        match chunk {
            Chunk::Redaction { rule_id, ty, .. } => {
                remarks.push(Remark::with_range(ty, rule_id.clone(), (pos, new_pos)))
            }
            Chunk::Text { .. } => {
                // Plain text segments do not need remarks
            }
        }

        pos = new_pos;
    }

    (rv, remarks)
}

/// Splits the string into chunks, maps each chunk and then joins chunks again, emitting
/// remarks along the process.
pub fn process_chunked_value<F>(value: &mut String, meta: &mut Meta, f: F)
where
    F: FnOnce(Vec<Chunk>) -> Vec<Chunk>,
{
    let chunks = split_chunks(value, meta.iter_remarks());
    let (new_value, remarks) = join_chunks(f(chunks));

    if new_value != *value {
        meta.clear_remarks();
        for remark in remarks.into_iter() {
            meta.add_remark(remark);
        }
        meta.set_original_length(Some(bytecount::num_chars(value.as_bytes())));
        *value = new_value;
    }
}

#[cfg(test)]
use crate::testutils::assert_eq_dbg;

#[test]
fn test_chunk_split() {
    let remarks = vec![Remark::with_range(
        RemarkType::Masked,
        "@email:strip",
        (33, 47),
    )];

    let chunks = vec![
        Chunk::Text {
            text: "Hello Peter, my email address is ".into(),
        },
        Chunk::Redaction {
            ty: RemarkType::Masked,
            text: "****@*****.com".into(),
            rule_id: "@email:strip".into(),
        },
        Chunk::Text {
            text: ". See you".into(),
        },
    ];

    assert_eq_dbg!(
        split_chunks(
            "Hello Peter, my email address is ****@*****.com. See you",
            &remarks,
        ),
        chunks
    );

    assert_eq_dbg!(
        join_chunks(chunks),
        (
            "Hello Peter, my email address is ****@*****.com. See you".into(),
            remarks
        )
    );
}
