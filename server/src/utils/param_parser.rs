/// Defines the type of indexes in a string param
#[derive(Debug, PartialEq)]
pub enum ParamIndex<'a> {
    /// a number index e.g. [33]
    Number(usize),
    /// a string index e.g. [some_text]
    String(&'a str),
    /// an empty index e.g. []
    Empty,
}

enum IndexingState {
    LookingForLeftParenthesis,
    Accumulating(usize),
}

/// Extracts indexes from a param string e.g. extracts `[String(abc),String(xyz)]` from `"sentry[abc][xyz]"`
pub fn get_indexes(full_string: &str) -> Vec<ParamIndex> {
    let mut ret_vals: Vec<ParamIndex> = vec![];
    let mut state = IndexingState::LookingForLeftParenthesis;
    // iterate byte by byte (so we can get correct offsets)
    for (idx, by) in full_string.as_bytes().iter().enumerate() {
        match state {
            IndexingState::LookingForLeftParenthesis => {
                if by == &b'[' {
                    state = IndexingState::Accumulating(idx + 1);
                }
            }
            IndexingState::Accumulating(start_idx) => {
                if by == &b']' {
                    if idx == start_idx {
                        ret_vals.push(ParamIndex::Empty);
                    } else {
                        let slice = &full_string[start_idx..idx];
                        let digit = slice.parse::<usize>();

                        match digit {
                            Ok(digit) => ret_vals.push(ParamIndex::Number(digit)),
                            Err(_) => ret_vals.push(ParamIndex::String(&slice)),
                        }
                    }
                    state = IndexingState::LookingForLeftParenthesis;
                }
            }
        }
    }
    ret_vals
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_parser() {
        let examples: &[(&str, &[ParamIndex])] = &[
            (
                "fafdasd[a][b][33]",
                &[
                    ParamIndex::String("a"),
                    ParamIndex::String("b"),
                    ParamIndex::Number(33),
                ],
            ),
            (
                "[23a][234]",
                &[ParamIndex::String("23a"), ParamIndex::Number(234)],
            ),
            (
                "sentry[abc][123][]=SomeVal",
                &[
                    ParamIndex::String("abc"),
                    ParamIndex::Number(123),
                    ParamIndex::Empty,
                ],
            ),
            (
                "sentry[Grüße][Jürgen❤]",
                &[ParamIndex::String("Grüße"), ParamIndex::String("Jürgen❤")],
            ),
            (
                "[农22历][新年]",
                &[ParamIndex::String("农22历"), ParamIndex::String("新年")],
            ),
            (
                "[ὈΔΥΣΣΕΎΣ][abc]",
                &[ParamIndex::String("ὈΔΥΣΣΕΎΣ"), ParamIndex::String("abc")],
            ),
        ];

        for &(example, expected_result) in examples {
            let indexes = get_indexes(example);
            assert_eq!(&indexes[..], expected_result)
        }
    }
}
