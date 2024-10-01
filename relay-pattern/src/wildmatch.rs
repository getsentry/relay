use std::num::NonZeroUsize;

use crate::{Literal, Options, Ranges, Token, Tokens};

/// Matches [`Tokens`] against a `haystack` with the provided [`Options`].
///
/// This implementation is largely based on the algorithm described by [Kirk J Krauss]
/// and combining the two loops into a single one and other small modifications to take advantage
/// of the already pre-processed [`Tokens`] structure and its invariants.
///
/// [Kirk J Krauss]: http://developforperformance.com/MatchingWildcards_AnImprovedAlgorithmForBigData.html
pub fn is_match(haystack: &str, tokens: &Tokens, options: Options) -> bool {
    match options.case_insensitive {
        false => is_match_impl::<_, CaseSensitive>(haystack, tokens.as_slice()),
        true => is_match_impl::<_, CaseInsensitive>(haystack, tokens.as_slice()),
    }
}

#[inline(always)]
fn is_match_impl<T, M>(haystack: &str, tokens: &T) -> bool
where
    T: TokenIndex + ?Sized,
    M: Matcher,
{
    // Remainder of the haystack which still needs to be matched.
    let mut h_current = haystack;
    // Saved haystack position for backtracking.
    let mut h_revert = haystack;
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

    while t_next != tokens.len() || !h_current.is_empty() {
        let matched = if t_next == tokens.len() {
            false
        } else {
            let token = &tokens[t_next];
            t_next += 1;

            match token {
                Token::Literal(literal) => match M::is_prefix(h_current, literal) {
                    Some(n) => advance!(n),
                    // The literal does not match, but it may match after backtracking.
                    // TODO: possible optimization: if the literal cannot possibly match anymore
                    // because it is too long for the remaining haystack, we can immediately return
                    // no match here.
                    None => false,
                },
                Token::Any(n) => {
                    advance!(match n_chars_to_bytes(*n, h_current) {
                        Some(n) => n,
                        // Not enough characters in the haystack remaining,
                        // there cannot be any other possible match.
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

                    match skip_to_token::<_, M>(tokens, t_next, h_current) {
                        Some((tokens, revert, remaining)) => {
                            t_next += tokens;
                            h_revert = revert;
                            h_current = remaining;
                        }
                        None => return false,
                    };

                    true
                }
                Token::Class { negated, ranges } => match h_current.chars().next() {
                    Some(next) => {
                        M::ranges_match(next, *negated, ranges) && advance!(next.len_utf8())
                    }
                    None => false,
                },
                Token::Alternates(alternates) => {
                    // TODO: should we make this iterative instead of recursive?
                    let matches = alternates.iter().any(|alternate| {
                        let tokens = tokens.with_alternate(t_next, alternate.as_slice());
                        is_match_impl::<_, M>(h_current, &tokens)
                    });

                    // The brace match already matches to the end, if it is successful we can end right here.
                    if matches {
                        return true;
                    }
                    // No match, allow for backtracking.
                    false
                }
            }
        };

        if !matched {
            if t_revert == 0 {
                // No backtracking necessary, no star encountered.
                // Didn't match and no backtracking -> no match.
                return false;
            }
            h_current = h_revert;
            t_next = t_revert;

            // Backtrack to the previous location +1 character.
            advance!(match n_chars_to_bytes(NonZeroUsize::MIN, h_current) {
                Some(n) => n,
                None => return false,
            });

            match skip_to_token::<_, M>(tokens, t_next, h_current) {
                Some((tokens, revert, remaining)) => {
                    t_next += tokens;
                    h_revert = revert;
                    h_current = remaining;
                }
                None => return false,
            };
        }
    }

    true
}

/// Bundles necessary matchers for [`is_match_impl`].
trait Matcher {
    /// Returns the length of the `needle` in the `haystack` if the `needle` is a prefix of `haystack`.
    fn is_prefix(haystack: &str, needle: &Literal) -> Option<usize>;
    /// Searches for the `needle` in the `haystack` and returns the index of the start of the match
    /// and the length of match or `None` if the `needle` is not contained in the `haystack`.
    fn find(haystack: &str, needle: &Literal) -> Option<(usize, usize)>;
    /// Returns `true` if the char `c` is contained within `ranges`.
    fn ranges_match(c: char, negated: bool, ranges: &Ranges) -> bool;
    /// Searches for the first char in the `haystack` that is contained in one of the `ranges`.
    ///
    /// Returns the offset in bytes and matching `char` if the range is contained within the
    /// `haystack`.
    #[inline(always)]
    fn ranges_find(haystack: &str, negated: bool, ranges: &Ranges) -> Option<(usize, char)> {
        // TODO: possibly optimize range finding.
        // TODO: possibly optimize with `memchr{1,2,3}` for short ranges.
        haystack
            .char_indices()
            .find(|&(_, c)| Self::ranges_match(c, negated, ranges))
    }
}

/// A case sensitive [`Matcher`].
struct CaseSensitive;

impl Matcher for CaseSensitive {
    #[inline(always)]
    fn is_prefix(haystack: &str, needle: &Literal) -> Option<usize> {
        let needle = needle.as_case_converted_bytes();
        memchr::arch::all::is_prefix(haystack.as_bytes(), needle).then_some(needle.len())
    }

    #[inline(always)]
    fn find(haystack: &str, needle: &Literal) -> Option<(usize, usize)> {
        let needle = needle.as_case_converted_bytes();
        memchr::memmem::find(haystack.as_bytes(), needle).map(|offset| (offset, needle.len()))
    }

    #[inline(always)]
    fn ranges_match(c: char, negated: bool, ranges: &Ranges) -> bool {
        ranges.contains(c) ^ negated
    }
}

/// A case insensitive [`Matcher`].
struct CaseInsensitive;

impl Matcher for CaseInsensitive {
    #[inline(always)]
    fn is_prefix(haystack: &str, needle: &Literal) -> Option<usize> {
        // We can safely assume `needle` is already full lowercase. This transformation is done on
        // token creation based on the options.
        //
        // The haystack cannot be converted to full lowercase to not break class matches on
        // uppercase unicode characters which would produce multiple lowercase characters.
        //
        // TODO: benchmark if allocation free is better/faster.
        let needle = needle.as_case_converted_bytes();
        let lower_haystack = haystack.to_lowercase();

        memchr::arch::all::is_prefix(lower_haystack.as_bytes(), needle)
            .then(|| recover_offset_len(haystack, 0, needle.len()).1)
    }

    #[inline(always)]
    fn find(haystack: &str, needle: &Literal) -> Option<(usize, usize)> {
        // TODO: implement manual lowercase which remembers if there were 'special' unicode
        // conversion involved, if not, there is no recovery necessary.
        // TODO: benchmark if a lut from offset -> original offset makes sense.
        // TODO: benchmark allocation free and search with proper case insensitive search.
        let needle = needle.as_case_converted_bytes();
        let lower_haystack = haystack.to_lowercase();

        let offset = memchr::memmem::find(lower_haystack.as_bytes(), needle)?;

        // `offset` now points into the lowercase converted string, but this may not match the
        // offset in the original string. Time to recover the index.
        Some(recover_offset_len(haystack, offset, offset + needle.len()))
    }

    #[inline(always)]
    fn ranges_match(c: char, negated: bool, ranges: &Ranges) -> bool {
        let matches = exactly_one(c.to_lowercase()).is_some_and(|c| ranges.contains(c))
            || exactly_one(c.to_uppercase()).is_some_and(|c| ranges.contains(c));
        matches ^ negated
    }
}

/// Efficiently skips to the next matching possible match after a wildcard.
///
/// Tokens must be indexable with `t_next`.
///
/// Returns `None` if there is no match and the matching can be aborted.
/// Otherwise returns the amount of tokens consumed, the new save point to backtrack to
/// and the remaining haystack.
#[inline(always)]
fn skip_to_token<'a, T, M>(
    tokens: &T,
    t_next: usize,
    haystack: &'a str,
) -> Option<(usize, &'a str, &'a str)>
where
    T: TokenIndex + ?Sized,
    M: Matcher,
{
    let next = &tokens[t_next];

    // TODO: optimize other cases like:
    //  - `[Any(n), Literal(_), ..]` (skip + literal find)
    //  - `[Any(n)]` (minimum remaining length)
    Some(match next {
        Token::Literal(literal) => {
            match M::find(haystack, literal) {
                // We cannot use `offset + literal.len()` as the revert position
                // to not discard overlapping matches.
                Some((offset, len)) => (1, &haystack[offset..], &haystack[offset + len..]),
                // The literal does not exist in the remaining slice.
                // No backtracking necessary, we won't ever find it.
                None => return None,
            }
        }
        Token::Class { negated, ranges } => {
            match M::ranges_find(haystack, *negated, ranges) {
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

/// Calculates a byte offset of the next `n` chars in the string `s`.
///
/// Returns `None` if the string is too short.
#[inline(always)]
fn n_chars_to_bytes(n: NonZeroUsize, s: &str) -> Option<usize> {
    // Fast path check, if there are less bytes than characters.
    if n.get() > s.len() {
        return None;
    }
    s.char_indices()
        .nth(n.get() - 1)
        .map(|(i, c)| i + c.len_utf8())
}

/// Returns `Some` if `iter` contains exactly one element.
#[inline(always)]
fn exactly_one<T>(mut iter: impl Iterator<Item = T>) -> Option<T> {
    let item = iter.next()?;
    match iter.next() {
        Some(_) => None,
        None => Some(item),
    }
}

/// Recovers offset and length from a case insensitive search in `haystack` using a lowecase
/// haystack and a lowercase needle.
///
/// `lower_offset` is the offset of the match in the lowercase haystack.
/// `lower_end` is the end offset of the match in the lowercase haystack.
///
/// Returns the recovered offset and length.
#[inline(always)]
fn recover_offset_len(
    haystack: &str,
    lower_offset: usize,
    lower_offset_end: usize,
) -> (usize, usize) {
    haystack
        .chars()
        .try_fold((0, 0, 0), |(lower, h_offset, h_len), c| {
            let lower = lower + c.to_lowercase().map(|c| c.len_utf8()).sum::<usize>();

            if lower < lower_offset {
                Ok((lower, h_offset + c.len_utf8(), 0))
            } else if lower == lower_offset {
                Ok((lower, h_offset, c.len_utf8()))
            } else if lower <= lower_offset_end {
                Ok((lower, h_offset, h_len + c.len_utf8()))
            } else {
                Err((h_offset, h_len))
            }
        })
        .map_or_else(|e| e, |(_, offset, len)| (offset, len))
}

/// Minimum requirements to process tokens during matching.
///
/// This is very closely coupled to [`is_match_impl`] and the process
/// of also matching alternate branches of a pattern. We use a trait here
/// to make use of monomorphization to make sure the alternate matching
/// can be inlined.
trait TokenIndex: std::ops::Index<usize, Output = Token> + std::fmt::Debug {
    /// The type returned from [`Self::with_alternate`].
    ///
    /// We need an associated type here to not produce and endlessly recursive
    /// type. Alternates are only allowed for the first 'level' (can't be nested),
    /// which allows us to avoid the recursion.
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
    ) -> Self::WithAlternates<'a>;
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
    ) -> Self::WithAlternates<'a> {
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

    fn with_alternate<'b>(
        &'b self,
        offset: usize,
        alternate: &'b [Token],
    ) -> Self::WithAlternates<'b> {
        if offset < self.alternate.len() {
            unreachable!("No nested alternates")
        }
        AltAndTokens {
            alternate,
            tokens: &self.tokens[offset - self.alternate.len()..],
        }
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

// Just some tests, full test suite is run on globs not tokens.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Range, Ranges};

    fn literal(s: &str) -> Literal {
        Literal::new(s.to_owned(), Default::default())
    }

    #[test]
    fn test_exactly_one() {
        assert_eq!(exactly_one([].into_iter()), None::<i32>);
        assert_eq!(exactly_one([1].into_iter()), Some(1));
        assert_eq!(exactly_one([1, 2].into_iter()), None);
        assert_eq!(exactly_one([1, 2, 3].into_iter()), None);
    }

    #[test]
    fn test_literal() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal(literal("abc")));
        assert!(is_match("abc", &tokens, Default::default()));
        assert!(!is_match("abcd", &tokens, Default::default()));
        assert!(!is_match("bc", &tokens, Default::default()));
    }

    #[test]
    fn test_class() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal(literal("a")));
        tokens.push(Token::Class {
            negated: false,
            ranges: Ranges::Single(Range::single('b')),
        });
        tokens.push(Token::Literal(literal("c")));
        assert!(is_match("abc", &tokens, Default::default()));
        assert!(!is_match("aac", &tokens, Default::default()));
        assert!(!is_match("abbc", &tokens, Default::default()));
    }

    #[test]
    fn test_class_negated() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal(literal("a")));
        tokens.push(Token::Class {
            negated: true,
            ranges: Ranges::Single(Range::single('b')),
        });
        tokens.push(Token::Literal(literal("c")));
        assert!(!is_match("abc", &tokens, Default::default()));
        assert!(is_match("aac", &tokens, Default::default()));
        assert!(!is_match("abbc", &tokens, Default::default()));
    }

    #[test]
    fn test_any_one() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal(literal("a")));
        tokens.push(Token::Any(NonZeroUsize::MIN));
        tokens.push(Token::Literal(literal("c")));
        assert!(is_match("abc", &tokens, Default::default()));
        assert!(is_match("aඞc", &tokens, Default::default()));
        assert!(!is_match("abbc", &tokens, Default::default()));
    }

    #[test]
    fn test_any_many() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal(literal("a")));
        tokens.push(Token::Any(NonZeroUsize::new(2).unwrap()));
        tokens.push(Token::Literal(literal("d")));
        assert!(is_match("abcd", &tokens, Default::default()));
        assert!(is_match("aඞ_d", &tokens, Default::default()));
        assert!(!is_match("abbc", &tokens, Default::default()));
        assert!(!is_match("abcde", &tokens, Default::default()));
        assert!(!is_match("abc", &tokens, Default::default()));
        assert!(!is_match("bcd", &tokens, Default::default()));
    }

    #[test]
    fn test_any_unicode() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal(literal("a")));
        tokens.push(Token::Any(NonZeroUsize::new(3).unwrap()));
        tokens.push(Token::Literal(literal("a")));
        assert!(is_match("abbba", &tokens, Default::default()));
        assert!(is_match("aඞbඞa", &tokens, Default::default()));
        // `i̇` is `i\u{307}`
        assert!(is_match("aඞi̇a", &tokens, Default::default()));
        assert!(!is_match("aඞi̇ඞa", &tokens, Default::default()));
    }

    #[test]
    fn test_wildcard_start() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Wildcard);
        tokens.push(Token::Literal(literal("b")));
        assert!(is_match("b", &tokens, Default::default()));
        assert!(is_match("aaaab", &tokens, Default::default()));
        assert!(is_match("ඞb", &tokens, Default::default()));
        assert!(is_match("bbbbbbbbb", &tokens, Default::default()));
        assert!(!is_match("", &tokens, Default::default()));
        assert!(!is_match("a", &tokens, Default::default()));
        assert!(!is_match("aa", &tokens, Default::default()));
        assert!(!is_match("aaa", &tokens, Default::default()));
        assert!(!is_match("ba", &tokens, Default::default()));
    }

    #[test]
    fn test_wildcard_end() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal(literal("a")));
        tokens.push(Token::Wildcard);
        assert!(is_match("a", &tokens, Default::default()));
        assert!(is_match("aaaab", &tokens, Default::default()));
        assert!(is_match("aඞ", &tokens, Default::default()));
        assert!(!is_match("", &tokens, Default::default()));
        assert!(!is_match("b", &tokens, Default::default()));
        assert!(!is_match("bb", &tokens, Default::default()));
        assert!(!is_match("bbb", &tokens, Default::default()));
        assert!(!is_match("ba", &tokens, Default::default()));
    }

    #[test]
    fn test_wildcard_end_unicode_case_insensitive() {
        let options = Options {
            case_insensitive: true,
        };
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal(Literal::new("İ".to_string(), options)));
        tokens.push(Token::Wildcard);

        assert!(is_match("İ___", &tokens, options));
        assert!(is_match("İ", &tokens, options));
        assert!(is_match("i̇", &tokens, options));
        assert!(is_match("i\u{307}___", &tokens, options));
        assert!(!is_match("i____", &tokens, options));
    }

    #[test]
    fn test_alternate() {
        let mut tokens = Tokens::default();
        tokens.push(Token::Literal(literal("a")));
        tokens.push(Token::Alternates(vec![
            {
                let mut tokens = Tokens::default();
                tokens.push(Token::Literal(literal("b")));
                tokens
            },
            {
                let mut tokens = Tokens::default();
                tokens.push(Token::Literal(literal("c")));
                tokens
            },
        ]));
        tokens.push(Token::Literal(literal("a")));
        assert!(is_match("aba", &tokens, Default::default()));
        assert!(is_match("aca", &tokens, Default::default()));
        assert!(!is_match("ada", &tokens, Default::default()));
    }
}
