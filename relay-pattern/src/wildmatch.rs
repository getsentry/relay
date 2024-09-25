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
fn is_match_inner<T>(tokens: &T, haystack: &str, options: Options) -> bool
where
    T: TokenIndex + ?Sized,
{
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
        if t_next == tokens.len() {
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
                    return true;
                }

                t_revert = t_next;

                match skip_to_token(tokens, t_next, h_current) {
                    Some((tokens, saved, remaining)) => {
                        t_next += tokens;
                        h_saved = saved;
                        h_current = remaining;
                    }
                    None => return false,
                };

                true
            }
            Token::Class { negated, ranges } => match h_current.chars().next() {
                Some(next) => (ranges.contains(next) ^ negated) && advance!(next.len_utf8()),
                None => false,
            },
            Token::Alternates(alternates) => {
                let matches = alternates.iter().any(|alternate| {
                    let tokens = tokens.with_alternate(t_next, alternate.as_slice());
                    is_match_inner(&tokens, h_current, options)
                });

                // The brace match already matches to the end, if it is successful we can end right here.
                if matches {
                    return true;
                }
                // No match, allow for backtracking.
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
            h_current = h_saved;
            t_next = t_revert;

            // Backtrack to the previous location +1 character.
            advance!(match n_chars_to_bytes(1, h_current) {
                Some(n) => n,
                None => return false,
            });

            match skip_to_token(tokens, t_next, h_current) {
                Some((tokens, saved, remaining)) => {
                    t_next += tokens;
                    h_saved = saved;
                    h_current = remaining;
                }
                None => return false,
            };
        }
    }
}

/// Efficiently skips to the next matching possible match after a wildcard.
///
/// Returns `None` if there is no match and the matching can be aborted.
/// Otherwise returns the amount of tokens consumed, the new save point to backtrack to
/// and the remaining haystack.
#[inline(always)]
fn skip_to_token<'a, T>(
    tokens: &T,
    t_next: usize,
    haystack: &'a str,
) -> Option<(usize, &'a str, &'a str)>
where
    T: TokenIndex + ?Sized,
{
    println!("SKIP {tokens:?} | {t_next}");
    let next = &tokens[t_next];

    // TODO: optimize other cases like:
    //  - `[Any(n), Literal(_), ..]` (skip + literal find)
    //  - `[Any(n)]` (minimum remaining length)
    Some(match next {
        Token::Literal(literal) => {
            match memmem::find(haystack.as_bytes(), literal.as_bytes()) {
                // We cannot use `offset + literal.len()` as the saved position
                // to not discard overlapping matches.
                Some(offset) => (1, &haystack[offset..], &haystack[offset + literal.len()..]),
                // The literal does not exist in the remaining slice.
                // No backtracking necessary, we won't ever find it.
                None => return None,
            }
        }
        Token::Class { negated, ranges } => {
            match haystack
                .char_indices()
                .find(|&(_, c)| ranges.contains(c) ^ negated)
            {
                Some((offset, c)) => (1, &haystack[offset..], &haystack[offset + c.len_utf8()..]),
                // None of the remaining characters matches this class.
                // No backtracking necessary, we won't ever find it.
                None => return None,
            }
        }
        _ => {
            // We didn't consume and match the token, revert to the previous state and
            // let the generic matching with slower backtracking handle the token.
            (0, haystack, haystack)
        }
    })
}

/// Minimum requirements to process tokens during matching.
///
/// This is very closely coupled to [`is_match_inner`] and the process
/// of also matching alternate branches of a pattern. We use a trait here
/// to make use of monomorphization to make sure the alternate matching
/// can be inlined.
trait TokenIndex: std::ops::Index<usize, Output = Token> + std::fmt::Debug {
    /// The type returned from [`Self::with_alternate`].
    ///
    /// We need an associated type here to not produce and endlessly recursive
    /// type. Alternates are only allowed for the first 'level' (can't be nested),
    /// which allows us to break through the
    type WithAlternates<'a>: TokenIndex
    where
        Self: 'a;

    fn len(&self) -> usize;

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Merges the current instance with alternate tokens and returns [`Self::WithAlternates`].
    fn with_alternate<'a>(
        &'a self,
        offset: usize,
        alternate: &'a [Token],
    ) -> Self::WithAlternates<'_>;
}

impl TokenIndex for [Token] {
    type WithAlternates<'a> = AltAndTokens<'a>;

    #[inline(always)]
    fn len(&self) -> usize {
        self.len()
    }

    #[inline(always)]
    fn with_alternate<'a>(
        &'a self,
        offset: usize,
        alternate: &'a [Token],
    ) -> Self::WithAlternates<'_> {
        AltAndTokens {
            alternate,
            tokens: &self[offset..],
        }
    }
}

/// A [`TokenIndex`] implementation which has been combined with tokens from an alternation.
///
/// Each alternate in the pattern creates a new individual matching branch with the alternate
/// currently being matched, the remaining tokens of the original pattern and the remaining
/// haystack which is yet to be matched.
///
/// If the resulting submatch matches the total pattern matches, if it does not match
/// another branch is tested.
#[derive(Debug)]
struct AltAndTokens<'a> {
    /// The alternation tokens.
    alternate: &'a [Token],
    /// The remaining tokens of the original pattern.
    tokens: &'a [Token],
}

impl<'a> TokenIndex for AltAndTokens<'a> {
    // Type here does not matter, we implement `with_alternate` by returning the never type.
    // It just needs to satisfy the `TokenIndex` trait bound.
    type WithAlternates<'b> = AltAndTokens<'b> where Self: 'b;

    #[inline(always)]
    fn len(&self) -> usize {
        self.alternate.len() + self.tokens.len()
    }

    fn with_alternate<'b>(&'b self, _: usize, _: &'b [Token]) -> Self::WithAlternates<'b> {
        unreachable!("No nested alternates")
    }
}

impl<'a> std::ops::Index<usize> for AltAndTokens<'a> {
    type Output = Token;

    fn index(&self, index: usize) -> &Self::Output {
        if index < self.alternate.len() {
            &self.alternate[index]
        } else {
            &self.tokens[index - self.alternate.len()]
        }
    }
}

#[inline(always)]
fn n_chars_to_bytes(n: usize, s: &str) -> Option<usize> {
    if n == 0 {
        return Some(0);
    }
    // Fast path check, if there are less bytes than characters.
    if n > s.len() {
        return None;
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
        // assert!(is_match(&tokens, "b", Default::default()));
        // assert!(is_match(&tokens, "aaaab", Default::default()));
        // assert!(is_match(&tokens, "ඞb", Default::default()));
        // assert!(!is_match(&tokens, "", Default::default()));
        // assert!(!is_match(&tokens, "a", Default::default()));
        // assert!(!is_match(&tokens, "aa", Default::default()));
        // assert!(!is_match(&tokens, "aaa", Default::default()));
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
