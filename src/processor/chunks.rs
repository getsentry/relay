//! Utilities for dealing with annotated strings.
//!
//! This module contains the `split` and `join` function to destructure and recombine strings by
//! redaction remarks. This allows to quickly inspect modified sections of a string.
//!
//! ### Example
//!
//! ```
//! use general::meta::{Meta, Remark, RemarkType};
//! use general::chunks;
//!
//! let remarks = vec![Remark::with_range(
//!     RemarkType::Substituted,
//!     "myrule",
//!     (7, 17),
//! )];
//!
//! let chunks = chunks::split("Hello, [redacted]!", &remarks);
//! let (joined, join_remarks) = chunks::join(chunks);
//!
//! assert_eq!(joined, "Hello, [redacted]!");
//! assert_eq!(join_remarks, remarks);
//! ```

use std::fmt;

use super::*;
use crate::types::*;

/// A type for dealing with chunks of annotated text.
#[derive(Clone, Debug, PartialEq)]
pub enum Chunk {
    /// Unmodified text chunk.
    Text {
        /// The text value of the chunk
        text: String,
    },
    /// Redacted text chunk with a note.
    Redaction {
        /// The redacted text value
        text: String,
        /// The rule that crated this redaction
        rule_id: String,
        /// Type type of remark for this redaction
        ty: RemarkType,
    },
}

impl Chunk {
    /// The text of this chunk.
    pub fn as_str(&self) -> &str {
        match *self {
            Chunk::Text { ref text } => &text,
            Chunk::Redaction { ref text, .. } => &text,
        }
    }

    /// Effective length of the text in this chunk.
    pub fn len(&self) -> usize {
        self.as_str().len()
    }

    /// The number of chars in this chunk.
    pub fn chars(&self) -> usize {
        self.as_str().chars().count()
    }

    /// Determines whether this chunk is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Display for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Chunks the given text based on remarks.
pub fn split_chunks<'a, S, I>(text: S, remarks: I) -> Vec<Chunk>
where
    S: AsRef<str>,
    I: IntoIterator<Item = &'a Remark>,
{
    let text = text.as_ref();

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
                    text: piece.to_string(),
                });
            } else {
                break;
            }
        }
        if let Some(piece) = text.get(from..to) {
            rv.push(Chunk::Redaction {
                text: piece.to_string(),
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
                text: piece.to_string(),
            });
        }
    }

    rv
}

/// Concatenates chunks into a string and emits remarks for redacted sections.
pub fn join_chunks<I>(chunks: I) -> (String, Vec<Remark>)
where
    I: IntoIterator<Item = Chunk>,
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

impl Annotated<String> {
    pub fn map_value_chunked<F>(self, f: F) -> Annotated<String>
    where
        F: FnOnce(Vec<Chunk>) -> Vec<Chunk>,
    {
        let Annotated(old_value, mut meta) = self;
        let new_value = old_value.map(|value| {
            let old_chunks = split_chunks(&value, meta.iter_remarks());
            let new_chunks = f(old_chunks);
            let (new_value, remarks) = join_chunks(new_chunks);
            *meta.remarks_mut() = remarks.into_iter().collect();
            if new_value != value {
                meta.set_original_length(Some(value.chars().count() as u32));
            }
            new_value
        });
        Annotated(new_value, meta)
    }

    pub fn trim_string(self, cap_size: CapSize) -> Annotated<String> {
        let limit = cap_size.max_chars();
        let grace_limit = limit + cap_size.grace_chars();

        if self.0.is_none() || self.0.as_ref().unwrap().chars().count() < grace_limit {
            return self;
        }

        // otherwise we trim down to max chars
        self.map_value_chunked(|chunks| {
            let mut length = 0;
            let mut rv = vec![];

            for chunk in chunks {
                let chunk_chars = chunk.chars();

                // if the entire chunk fits, just put it in
                if length + chunk_chars < limit {
                    rv.push(chunk);
                    length += chunk_chars;
                    continue;
                }

                match chunk {
                    // if there is enough space for this chunk and the 3 character
                    // ellipsis marker we can push the remaining chunk
                    Chunk::Redaction { .. } => {
                        if length + chunk_chars + 3 < grace_limit {
                            rv.push(chunk);
                        }
                    }

                    // if this is a text chunk, we can put the remaining characters in.
                    Chunk::Text { text } => {
                        let mut remaining = String::new();
                        for c in text.chars() {
                            if length < limit - 3 {
                                remaining.push(c);
                            } else {
                                break;
                            }
                            length += 1;
                        }
                        rv.push(Chunk::Text { text: remaining });
                    }
                }

                rv.push(Chunk::Redaction {
                    text: "...".to_string(),
                    rule_id: "!len".to_string(),
                    ty: RemarkType::Substituted,
                });
                break;
            }

            rv
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

}
