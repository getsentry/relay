use crate::{Options, Token, Tokens};

use memchr::arch::all::is_prefix;
use memchr::memmem;

/// Matches [`Tokens`] against a `haystack` with the provided [`Options`].
///
/// This implementation is largely based on the algorithm described by [Kirk J Krauss](jkrauss)
/// and combining the two loops into a single one and other small modifications to take advantage
/// of the already pre-processed [`Tokens`] structure and its invariants.
///
/// [jkrauss]: http://developforperformance.com/MatchingWildcards_AnImprovedAlgorithmForBigData.html
pub fn is_match(tokens: &Tokens, haystack: &str, options: Options) -> bool {
    is_match_inner(tokens.as_slice(), haystack, options)
}

#[inline(always)]
fn is_match_inner(tokens: &[Token], haystack: &str, options: Options) -> bool {
    // Remainder of the haystack which still needs to be matched.
    let mut h_current = haystack;
    // Saved haystack position for backtracking.
    let mut h_saved = haystack;
    // Revert index for `tokens`. In case of backtracking we backtrack to this index.
    //
    // If `t_revert` is zero, it means there is no currently saved backtracking position.
    let mut t_revert = 0;
    // The next token position which needs to be evaluted.
    let mut t_next = 0;

    macro_rules! advance {
        ($len:expr) => {{
            h_current = &h_current[$len..];
            true
        }};
    }

    // Empty glob never matches.
    if tokens.is_empty() {
        return false;
    }

    loop {
        if t_next >= tokens.len() {
            return h_current.is_empty();
        }

        let token = &tokens[t_next];
        t_next += 1;

        // println!("CURRENT: {h_current:?} | TOKEN: {token:?}");

        let matched = match token {
            Token::Literal(literal) => {
                is_prefix(h_current.as_bytes(), literal.as_bytes()) && advance!(literal.len())
            }
            Token::Any(n) => {
                advance!(match n_chars_to_bytes(*n, h_current) {
                    Some(n) => n,
                    None => return false,
                });
                true
            }
            Token::Wildcard => {
                // `ab*c*` matches `abcd`.
                if t_next == tokens.len() {
                    break true;
                }

                // TODO: efficiently skip ahead here with `memmem::find`.
                // With the potential for future optimizations like `[Any(N), Literal(_)]`.

                h_saved = h_current;
                t_revert = t_next;

                true
            }
            Token::Class { negated, ranges } => match h_current.chars().next() {
                Some(next) => (ranges.contains(next) ^ negated) && advance!(next.len_utf8()),
                None => false,
            },
            Token::Alternates(alternates) => {
                // TODO: eliminate allocation here by using an enum base concatenation with a
                // special case for alternatives (we also know alternatives cannot be nested).
                let matches = alternates.iter().any(|alternate| {
                    let alternate = alternate.as_slice();
                    let mut alt_tokens =
                        Vec::with_capacity(tokens.len() - t_next + alternate.len());
                    alt_tokens.extend(alternate.iter().cloned());
                    alt_tokens.extend(tokens[t_next..].iter().cloned()); // TODO if tokens is at the end
                    is_match_inner(&alt_tokens, h_current, options)
                });

                // The brace match already matches to the end, if it is successful we can end right here.
                if matches {
                    return true;
                }
                false
            }
        };

        if t_revert == 0 {
            // No backtracking necessary, no star encountered.
            // Didn't match and no backtracking -> no match.
            if !matched {
                return false;
            }
        } else if !matched || (t_next == tokens.len() && !h_current.is_empty()) {
            // If there is either a failed match *or* no more tokens while there is still data in the
            // haystack remaining, backtrack and try again.
            h_current = h_saved;
            advance!(match n_chars_to_bytes(1, h_saved) {
                Some(n) => n,
                None => return false,
            });

            h_saved = h_current;
            t_next = t_revert;
        }
    }
}

// // Use a trait here to allow monomorphization and inlining for the [`AltAndTokens`] implementation.
// trait TokenIndex: std::ops::Index<usize, Output = Token> {
//     fn len(&self) -> usize;
//
//     fn is_empty(&self) -> bool {
//         self.len() == 0
//     }
// }
//
// impl TokenIndex for [Token] {
//     fn len(&self) -> usize {
//         self.len()
//     }
// }
//
// struct AltAndTokens<'a, T: ?Sized> {
//     alternate: &'a [Token],
//     offset: usize,
//     tokens: &'a T,
// }
//
// impl<'a, T> TokenIndex for AltAndTokens<'a, T>
// where
//     T: TokenIndex + ?Sized,
// {
//     fn len(&self) -> usize {
//         self.alternate.len() + self.tokens.len() - self.offset
//     }
// }
//
// impl<'a, T> std::ops::Index<usize> for AltAndTokens<'a, T>
// where
//     T: TokenIndex + ?Sized,
// {
//     type Output = Token;
//
//     fn index(&self, index: usize) -> &Self::Output {
//         if index < self.alternate.len() {
//             &self.alternate[index]
//         } else {
//             &self.tokens[self.offset + index - self.alternate.len()]
//         }
//     }
// }

#[inline(always)]
fn n_chars_to_bytes(n: usize, s: &str) -> Option<usize> {
    if n == 0 {
        return Some(0);
    }
    s.char_indices()
        .skip(n - 1)
        .next()
        .map(|(i, c)| i + c.len_utf8())
}

// Just some tests, full test suite is run on globs not tokens.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Range, Ranges};

    #[test]
    fn test_literal() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal("abc".to_owned()));
        assert!(is_match(&tokens, "abc", Default::default()));
        assert!(!is_match(&tokens, "abcd", Default::default()));
        assert!(!is_match(&tokens, "bc", Default::default()));
    }

    #[test]
    fn test_class() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal("a".to_owned()));
        tokens.push(Token::Class {
            negated: false,
            ranges: Ranges::Single(Range::single('b')),
        });
        tokens.push(Token::Literal("c".to_owned()));
        assert!(is_match(&tokens, "abc", Default::default()));
        assert!(!is_match(&tokens, "aac", Default::default()));
        assert!(!is_match(&tokens, "abbc", Default::default()));
    }

    #[test]
    fn test_class_negated() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal("a".to_owned()));
        tokens.push(Token::Class {
            negated: true,
            ranges: Ranges::Single(Range::single('b')),
        });
        tokens.push(Token::Literal("c".to_owned()));
        assert!(!is_match(&tokens, "abc", Default::default()));
        assert!(is_match(&tokens, "aac", Default::default()));
        assert!(!is_match(&tokens, "abbc", Default::default()));
    }

    #[test]
    fn test_any_one() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal("a".to_owned()));
        tokens.push(Token::Any(1));
        tokens.push(Token::Literal("c".to_owned()));
        assert!(is_match(&tokens, "abc", Default::default()));
        assert!(is_match(&tokens, "aඞc", Default::default()));
        assert!(!is_match(&tokens, "abbc", Default::default()));
    }

    #[test]
    fn test_any_many() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal("a".to_owned()));
        tokens.push(Token::Any(2));
        tokens.push(Token::Literal("d".to_owned()));
        assert!(is_match(&tokens, "abcd", Default::default()));
        assert!(is_match(&tokens, "aඞ_d", Default::default()));
        assert!(!is_match(&tokens, "abbc", Default::default()));
        assert!(!is_match(&tokens, "abcde", Default::default()));
        assert!(!is_match(&tokens, "abc", Default::default()));
        assert!(!is_match(&tokens, "bcd", Default::default()));
    }

    #[test]
    fn test_wildcard_start() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Wildcard);
        tokens.push(Token::Literal("b".to_owned()));
        assert!(is_match(&tokens, "b", Default::default()));
        assert!(is_match(&tokens, "aaaab", Default::default()));
        assert!(is_match(&tokens, "ඞb", Default::default()));
        assert!(!is_match(&tokens, "", Default::default()));
        assert!(!is_match(&tokens, "a", Default::default()));
        assert!(!is_match(&tokens, "aa", Default::default()));
        assert!(!is_match(&tokens, "aaa", Default::default()));
        assert!(!is_match(&tokens, "ba", Default::default()));
    }

    #[test]
    fn test_wildcard_end() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal("a".to_owned()));
        tokens.push(Token::Wildcard);
        assert!(is_match(&tokens, "a", Default::default()));
        assert!(is_match(&tokens, "aaaab", Default::default()));
        assert!(is_match(&tokens, "aඞ", Default::default()));
        assert!(!is_match(&tokens, "", Default::default()));
        assert!(!is_match(&tokens, "b", Default::default()));
        assert!(!is_match(&tokens, "bb", Default::default()));
        assert!(!is_match(&tokens, "bbb", Default::default()));
        assert!(!is_match(&tokens, "ba", Default::default()));
    }

    #[test]
    fn test_alternate() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal("a".to_owned()));
        tokens.push(Token::Alternates(vec![
            {
                let mut tokens = Tokens::default();
                tokens.push(Token::Literal("b".to_owned()));
                tokens
            },
            {
                let mut tokens = Tokens::default();
                tokens.push(Token::Literal("c".to_owned()));
                tokens
            },
        ]));
        tokens.push(Token::Literal("a".to_owned()));
        assert!(is_match(&tokens, "aba", Default::default()));
        assert!(is_match(&tokens, "aca", Default::default()));
        assert!(!is_match(&tokens, "ada", Default::default()));
    }
}
