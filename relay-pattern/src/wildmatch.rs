use std::num::NonZeroUsize;

use smallvec::SmallVec;

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
        false => is_match_impl::<CaseSensitive>(haystack, tokens.as_slice()),
        true => is_match_impl::<CaseInsensitive>(haystack, tokens.as_slice()),
    }
}

#[inline(always)]
fn is_match_impl<'a, M>(haystack: &'a str, tokens: &'a [Token]) -> bool
where
    M: Matcher,
{
    // Empty glob never matches.
    if tokens.is_empty() {
        return false;
    }

    // Stack of matching attempts, the top of the stack is the attempt which is
    // currently being matched.
    //
    // Each alternation pushes a new frame on the stack, containing a list of branches
    // needing to be matched.
    let mut frames: SmallVec<[Frame<'a>; 5]> = smallvec::smallvec![Frame::root(haystack, tokens)];

    // Whether the last evaluated token matched.
    let mut matched = true;

    loop {
        let Some(frame) = frames.last_mut() else {
            // All alternation branches exhausted -> no match.
            return false;
        };

        // Matches the current attempt against the haystack, including wildcard backtracking.
        //
        // The loop:
        //  - Returns `true` if a match was found.
        //  - Breaks with a new frame, when an alternate is found.
        //  - Breaks with `None` if the current frame does not match.
        let new_frame = loop {
            if !matched {
                if frame.t_revert == 0 {
                    // No backtracking possible, no wildcard was encountered
                    // in the current attempt.
                    break None;
                }
                frame.h_current = frame.h_revert;
                frame.t_next = frame.t_revert;

                // Backtrack to the previous location +1 character.
                match n_chars_to_bytes(NonZeroUsize::MIN, frame.h_current) {
                    Some(n) => frame.h_current = &frame.h_current[n..],
                    // The haystack is exhausted.
                    None => break None,
                }

                match skip_to_token::<M>(frame.stream, frame.t_next, frame.h_current) {
                    Some((tokens, revert, remaining)) => {
                        frame.t_next += tokens;
                        frame.h_revert = revert;
                        frame.h_current = remaining;
                    }
                    None => break None,
                }
            }

            if frame.t_next == frame.stream.len() {
                if frame.h_current.is_empty() {
                    // All tokens and the entire haystack are consumed -> match.
                    return true;
                }
                // There is haystack remaining, only backtracking can consume more of it.
                matched = false;
                continue;
            }

            let token = frame.stream.get(frame.t_next);
            frame.t_next += 1;

            matched = match token {
                Token::Literal(literal) => match M::is_prefix(frame.h_current, literal) {
                    Some(n) => {
                        frame.h_current = &frame.h_current[n..];
                        true
                    }
                    // The literal does not match, but it may match after backtracking.
                    // TODO: possible optimization: if the literal cannot possibly match
                    // anymore because it is too long for the remaining haystack, we can
                    // immediately give up on the current attempt here.
                    None => false,
                },
                Token::Any(n) => match n_chars_to_bytes(*n, frame.h_current) {
                    Some(n) => {
                        frame.h_current = &frame.h_current[n..];
                        true
                    }
                    // Not enough characters in the haystack remaining and backtracking
                    // only shrinks the haystack, there cannot be any other possible
                    // match in the current attempt.
                    None => break None,
                },
                Token::Wildcard => {
                    // `ab*c*` matches `abcd`.
                    if frame.t_next == frame.stream.len() {
                        return true;
                    }

                    frame.t_revert = frame.t_next;

                    match skip_to_token::<M>(frame.stream, frame.t_next, frame.h_current) {
                        Some((tokens, revert, remaining)) => {
                            frame.t_next += tokens;
                            frame.h_revert = revert;
                            frame.h_current = remaining;
                            true
                        }
                        None => break None,
                    }
                }
                Token::Class { negated, ranges } => match frame.h_current.chars().next() {
                    Some(next) if M::ranges_match(next, *negated, ranges) => {
                        frame.h_current = &frame.h_current[next.len_utf8()..];
                        true
                    }
                    _ => false,
                },
                // The parent frame is already in the correct state to continue matching
                // after the alternation, all it takes is a new frame for the alternation.
                Token::Alternates(alternates) => {
                    break Some(frame.new_branch(alternates.as_slice(), false));
                }
                Token::Optional(optional) => {
                    break Some(frame.new_branch(std::slice::from_ref(optional), true));
                }
            };
        };

        match new_frame {
            // An alternation was just entered, match its first branch.
            Some(mut new_frame) => {
                if new_frame.enter_next_alternate() {
                    frames.push(new_frame);
                    matched = true;
                } else {
                    // An alternation without any branches, there is nothing to match,
                    // continue in the current frame.
                    //
                    // The parser never produces empty alternations, but they are
                    // gracefully handled here like an alternation which did not match.
                    matched = new_frame.optional;
                }
            }
            // The current alternate can no longer match, continue with the next alternate.
            None if frame.enter_next_alternate() => {
                matched = true;
            }
            // The current frame is exhausted, all alternates did not match.
            None => {
                // All branches failed, continue with the parent and search for alternative
                // matches.
                //
                // If the current frame is marked optional, we did match and the parent does
                // not need to try to match alternates or backtrack.
                matched = frame.optional;
                frames.pop();
            }
        }
    }
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
/// The stream must be indexable with `t_next`.
///
/// Returns `None` if there is no match and the matching can be aborted.
/// Otherwise returns the amount of tokens consumed, the new save point to backtrack to
/// and the remaining haystack.
#[inline(always)]
fn skip_to_token<'a, M>(
    stream: TokenStream<'_>,
    t_next: usize,
    haystack: &'a str,
) -> Option<(usize, &'a str, &'a str)>
where
    M: Matcher,
{
    let next = stream.get(t_next);

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

            if lower <= lower_offset {
                Ok((lower, h_offset + c.len_utf8(), 0))
            } else if lower <= lower_offset_end {
                Ok((lower, h_offset, h_len + c.len_utf8()))
            } else {
                Err((h_offset, h_len))
            }
        })
        .map_or_else(|e| e, |(_, offset, len)| (offset, len))
}

/// The stream of tokens which is currently being matched against the haystack.
///
/// The stream consists of the tokens of the currently active alternation branch
/// (`alt`, empty if there is none) followed by the not yet fully matched suffix
/// of the original pattern (`base`).
///
/// Alternations cannot be nested, which is why two segments are always sufficient
/// to represent every state which can occur during matching.
#[derive(Default, Clone, Copy, Debug)]
struct TokenStream<'a> {
    /// The tokens of the currently active alternation branch.
    alternate: &'a [Token],
    /// The remaining tokens of the original pattern to match against.
    tokens: &'a [Token],
}

impl<'a> TokenStream<'a> {
    /// Total amount of tokens in the stream.
    #[inline(always)]
    fn len(&self) -> usize {
        self.alternate.len() + self.tokens.len()
    }

    /// Returns the token at position `index`.
    ///
    /// Panics if `index` is out of bounds.
    #[inline(always)]
    fn get(&self, index: usize) -> &'a Token {
        match self.alternate.get(index) {
            Some(token) => token,
            None => &self.tokens[index - self.alternate.len()],
        }
    }

    /// Returns the remaining tokens of the original pattern starting at `t_next`.
    fn base_suffix(&self, t_next: usize) -> &'a [Token] {
        match t_next.checked_sub(self.alternate.len()) {
            Some(offset) => &self.tokens[offset..],
            // This would only happen if `t_next` points into the alternates,
            // which is not possible since we do not allow nesting alternates.
            None => unreachable!("No nested alternates"),
        }
    }
}

/// A single matching attempt.
///
/// It consists of two parts:
/// 1. Current matching information
/// 2. Potential alternates to match
///
/// If the current match (1) is exhausted and does not match the haystack,
/// another alternate is queried from (2) and replaces the failed match
/// in (1).
struct Frame<'a> {
    /// The stream of tokens which is currently being matched.
    stream: TokenStream<'a>,
    /// Remainder of the haystack which still needs to be matched.
    h_current: &'a str,
    /// Saved haystack position for backtracking.
    h_revert: &'a str,
    /// The next token position in `stream` which needs to be evaluated.
    t_next: usize,
    /// Revert index for `stream`. In case of backtracking we backtrack to this index.
    ///
    /// If `t_revert` is zero, it means there is no currently saved backtracking position.
    t_revert: usize,

    /// The haystack at the position the alternation token was encountered and needs
    /// to be matched against.
    haystack: &'a str,
    /// The alternations which still need to be tried.
    ///
    /// Empty for the root frame.
    alternates: std::slice::Iter<'a, Tokens>,
    /// Whether the alternates are optional.
    ///
    /// If a frame is optional, none of its alternates need to match for the entire frame
    /// considered matching.
    optional: bool,
    /// The tokens of the original pattern following the alternation token.
    ///
    /// Every alternation branch is followed by these tokens.
    base: &'a [Token],
}

impl<'a> Frame<'a> {
    /// Creates the root frame matching the full pattern against the full haystack.
    fn root(haystack: &'a str, tokens: &'a [Token]) -> Self {
        Self {
            stream: TokenStream {
                alternate: &[],
                tokens,
            },
            h_current: haystack,
            h_revert: haystack,
            t_next: 0,
            t_revert: 0,
            alternates: Default::default(),
            optional: false,
            haystack,
            base: &[],
        }
    }

    /// Creates a child frame starting at the current location with a list of `branches` to evaluate.
    ///
    /// The parent's `t_next` must already point past the alternation token.
    /// The working state is initialized when the first branch is entered.
    fn new_branch(&self, alternates: &'a [Tokens], optional: bool) -> Self {
        Self {
            stream: TokenStream::default(),
            h_current: self.h_current,
            h_revert: self.h_current,
            t_next: 0,
            t_revert: 0,
            alternates: alternates.iter(),
            optional,
            haystack: self.h_current,
            base: self.stream.base_suffix(self.t_next),
        }
    }

    /// Enters the next branch of the alternation and resets the working state.
    ///
    /// Returns `false` if all branches are already exhausted.
    fn enter_next_alternate(&mut self) -> bool {
        let Some(branch) = self.alternates.next() else {
            return false;
        };

        self.stream = TokenStream {
            alternate: branch.as_slice(),
            tokens: self.base,
        };
        self.h_current = self.haystack;
        self.h_revert = self.haystack;
        self.t_next = 0;
        self.t_revert = 0;

        true
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

    fn literal_ci(s: &str) -> Literal {
        Literal::new(
            s.to_owned(),
            Options {
                case_insensitive: true,
            },
        )
    }

    fn range(start: char, end: char) -> Ranges {
        Ranges::Single(Range { start, end })
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
        tokens.push(Token::Literal(Literal::new("İ".to_owned(), options)));
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

    #[test]
    fn test_optional() {
        let mut tokens = Tokens::default();

        tokens.push(Token::Optional(Tokens(vec![Token::Literal(literal(
            "foo",
        ))])));
        assert!(is_match("foo", &tokens, Default::default()));
        assert!(is_match("", &tokens, Default::default()));
    }

    #[test]
    fn test_optional_alternate() {
        let mut tokens = Tokens::default();
        let alternates = Token::Alternates(vec![
            {
                let mut tokens = Tokens::default();
                tokens.push(Token::Literal(literal("foo")));
                tokens
            },
            {
                let mut tokens = Tokens::default();
                tokens.push(Token::Literal(literal("bar")));
                tokens
            },
        ]);

        tokens.push(Token::Optional(Tokens(vec![alternates])));
        assert!(is_match("foo", &tokens, Default::default()));
        assert!(is_match("bar", &tokens, Default::default()));
        assert!(is_match("", &tokens, Default::default()));
    }

    #[test]
    fn test_matcher_case_sensitive_prefix() {
        macro_rules! test {
            ($haystack:expr, $needle:expr, $result:expr) => {
                assert_eq!(
                    CaseSensitive::is_prefix($haystack, &literal($needle)),
                    $result
                );
            };
        }

        test!("foobar", "f", Some(1));
        test!("foobar", "foo", Some(3));
        test!("foobar", "foobar", Some(6));
        test!("foobar", "oobar", None);
        test!("foobar", "foobar2", None);
        test!("İ", "İ", Some(2));
        test!("İ", "i", None);
        test!("i", "İ", None);
        test!("i", "i", Some(1));
        test!("i̇", "i", Some(1));
        test!("i̇", "i\u{307}", Some(3));
        test!("i̇x", "i\u{307}", Some(3));
        test!("i̇x", "i\u{307}x", Some(4));
        test!("i̇x", "i\u{307}_", None);
    }

    #[test]
    fn test_matcher_case_sensitive_find() {
        macro_rules! test {
            ($haystack:expr, $needle:expr, $result:expr) => {
                assert_eq!(CaseSensitive::find($haystack, &literal($needle)), $result);
            };
        }

        test!("foobar", "f", Some((0, 1)));
        test!("foobar", "foo", Some((0, 3)));
        test!("foobar", "foobar", Some((0, 6)));
        test!("foobar", "bar", Some((3, 3)));
        test!("foobar", "oobar", Some((1, 5)));
        test!("foobar", "foobar2", None);
        test!("İ", "İ", Some((0, 2)));
        test!("i", "i", Some((0, 1)));
        test!("i̇", "i\u{307}", Some((0, 3)));
        test!("i̇x", "i\u{307}x", Some((0, 4)));
        test!("i̇x", "i\u{307}_", None);
        test!("xi̇x", "i\u{307}", Some((1, 3)));
        test!("xi̇ඞi̇x", "ඞ", Some((4, 3)));
        test!("xi̇ඞi̇x", "ඞi̇", Some((4, 6)));
    }

    #[test]
    fn test_matcher_case_sensitive_ranges_match() {
        macro_rules! test {
            ($c:expr, $negated:expr, [$start:literal - $end:literal], $result:expr) => {
                assert_eq!(
                    CaseSensitive::ranges_match($c, $negated, &range($start, $end)),
                    $result
                );
            };
        }

        test!('a', false, ['a' - 'a'], true);
        test!('a', true, ['a' - 'a'], false);
        test!('b', false, ['a' - 'a'], false);
        test!('b', true, ['a' - 'a'], true);
        test!('b', false, ['a' - 'c'], true);
        test!('b', false, ['b' - 'c'], true);
        test!('b', false, ['a' - 'b'], true);

        test!('A', false, ['a' - 'a'], false);
        test!('ඞ', false, ['ඞ' - 'ඞ'], true);
    }

    #[test]
    fn test_matcher_case_sensitive_ranges_find() {
        macro_rules! test {
            ($haystack:expr, $negated:expr, [$start:literal - $end:literal], $result:expr) => {
                assert_eq!(
                    CaseSensitive::ranges_find($haystack, $negated, &range($start, $end)),
                    $result
                );
            };
        }

        test!("ඞaඞ", false, ['a' - 'a'], Some((3, 'a')));
        test!("a", true, ['a' - 'a'], None);
        test!("ඞaඞ", true, ['a' - 'a'], Some((0, 'ඞ')));
        test!("aඞaඞ", true, ['a' - 'a'], Some((1, 'ඞ')));
        test!("ඞbඞ", false, ['a' - 'a'], None);
        test!("ඞbඞ", true, ['ඞ' - 'ඞ'], Some((3, 'b')));
        test!("ඞbඞ", false, ['a' - 'c'], Some((3, 'b')));
        test!("ඞbඞ", false, ['b' - 'c'], Some((3, 'b')));
        test!("ඞbඞ", false, ['a' - 'b'], Some((3, 'b')));
        test!("AAAAA", false, ['a' - 'a'], None);
        test!("aaaaaaabb", true, ['a' - 'a'], Some((7, 'b')));
        test!("AaaaaaAbb", false, ['b' - 'b'], Some((7, 'b')));
    }

    #[test]
    fn test_matcher_case_insensitive_prefix() {
        macro_rules! test {
            ($haystack:expr, $needle:expr, $result:expr) => {
                assert_eq!(
                    CaseInsensitive::is_prefix($haystack, &literal_ci($needle)),
                    $result
                );
            };
        }

        test!("foobar", "f", Some(1));
        test!("foobar", "F", Some(1));
        test!("fOobar", "foo", Some(3));
        test!("fooBAR", "foobar", Some(6));
        test!("foobar", "oobar", None);
        test!("FOOBAR", "oobar", None);
        test!("foobar", "foobar2", None);
        test!("İ", "İ", Some(2));
        test!("İ", "i", Some(0));
        test!("İ", "i̇", Some(2));
        test!("i", "İ", None);
        test!("i", "i", Some(1));
        test!("i̇", "i", Some(1));
        test!("i̇", "i\u{307}", Some(3));
        test!("i̇x", "i\u{307}", Some(3));
        test!("i̇x", "i\u{307}x", Some(4));
        test!("i̇x", "i\u{307}_", None);
    }

    #[test]
    fn test_matcher_case_insensitive_find() {
        macro_rules! test {
            ($haystack:expr, $needle:expr, $result:expr) => {
                assert_eq!(
                    CaseInsensitive::find($haystack, &literal_ci($needle)),
                    $result
                );
            };
        }

        test!("Foobar", "f", Some((0, 1)));
        test!("foObar", "FOO", Some((0, 3)));
        test!("foObar", "Foobar", Some((0, 6)));
        test!("foObar", "bar", Some((3, 3)));
        test!("foObarx", "bar", Some((3, 3)));
        test!("foObarbarbar", "bar", Some((3, 3)));
        test!("foObar", "Oobar", Some((1, 5)));
        test!("foObar", "Foobar2", None);
        test!("İ", "İ", Some((0, 2)));
        test!("i", "i", Some((0, 1)));
        test!("i̇", "i\u{307}", Some((0, 3)));
        test!("i̇x", "i\u{307}x", Some((0, 4)));
        test!("i̇x", "i\u{307}_", None);
        test!("xi̇x", "i\u{307}", Some((1, 3)));
        test!("xi̇ඞi̇x", "ඞ", Some((4, 3)));
        test!("xi̇ඞi̇x", "ඞi̇", Some((4, 6)));
        test!("xi̇ඞİx", "ඞi̇", Some((4, 5)));
    }

    #[test]
    fn test_matcher_case_insensitive_ranges_match() {
        macro_rules! test {
            ($c:expr, $negated:expr, [$start:literal - $end:literal], $result:expr) => {
                assert_eq!(
                    CaseInsensitive::ranges_match($c, $negated, &range($start, $end)),
                    $result
                );
            };
        }

        test!('a', false, ['a' - 'a'], true);
        test!('a', true, ['a' - 'a'], false);
        test!('b', false, ['a' - 'a'], false);
        test!('b', true, ['a' - 'a'], true);
        test!('b', false, ['a' - 'c'], true);
        test!('b', false, ['b' - 'c'], true);
        test!('b', false, ['a' - 'b'], true);

        test!('b', false, ['A' - 'A'], false);
        test!('b', true, ['A' - 'A'], true);
        test!('b', false, ['A' - 'C'], true);
        test!('b', false, ['B' - 'C'], true);
        test!('b', false, ['A' - 'B'], true);

        test!('B', false, ['a' - 'a'], false);
        test!('B', true, ['a' - 'a'], true);
        test!('B', false, ['a' - 'c'], true);
        test!('B', false, ['b' - 'c'], true);
        test!('B', false, ['a' - 'b'], true);

        test!('ǧ', false, ['Ǧ' - 'Ǧ'], true);
        test!('Ǧ', false, ['ǧ' - 'ǧ'], true);
        test!('ǧ', true, ['Ǧ' - 'Ǧ'], false);
        test!('Ǧ', true, ['ǧ' - 'ǧ'], false);

        test!('ඞ', false, ['ඞ' - 'ඞ'], true);
    }

    #[test]
    fn test_matcher_case_insensitive_ranges_find() {
        macro_rules! test {
            ($haystack:expr, $negated:expr, [$start:literal - $end:literal], $result:expr) => {
                assert_eq!(
                    CaseInsensitive::ranges_find($haystack, $negated, &range($start, $end)),
                    $result
                );
            };
        }

        test!("ඞaඞ", false, ['a' - 'a'], Some((3, 'a')));
        test!("a", true, ['a' - 'a'], None);
        test!("ඞaඞ", true, ['a' - 'a'], Some((0, 'ඞ')));
        test!("aඞaඞ", true, ['a' - 'a'], Some((1, 'ඞ')));
        test!("ඞbඞ", false, ['a' - 'a'], None);
        test!("ඞbඞ", true, ['ඞ' - 'ඞ'], Some((3, 'b')));
        test!("ඞbඞ", false, ['a' - 'c'], Some((3, 'b')));
        test!("ඞbඞ", false, ['b' - 'c'], Some((3, 'b')));
        test!("ඞbඞ", false, ['a' - 'b'], Some((3, 'b')));
        test!("AAAAA", false, ['a' - 'a'], Some((0, 'A')));
        test!("aaaaaaabb", true, ['a' - 'a'], Some((7, 'b')));
        test!("AaaaaaAbb", false, ['b' - 'b'], Some((7, 'b')));

        test!("ඞBඞ", false, ['a' - 'a'], None);
        test!("ඞBඞ", true, ['ඞ' - 'ඞ'], Some((3, 'B')));
        test!("ඞBඞ", false, ['a' - 'c'], Some((3, 'B')));
        test!("ඞBඞ", false, ['b' - 'c'], Some((3, 'B')));
        test!("ඞBඞ", false, ['a' - 'b'], Some((3, 'B')));

        test!("ඞbඞ", false, ['A' - 'A'], None);
        test!("ඞbඞ", true, ['ඞ' - 'ඞ'], Some((3, 'b')));
        test!("ඞbඞ", false, ['A' - 'C'], Some((3, 'b')));
        test!("ඞbඞ", false, ['B' - 'C'], Some((3, 'b')));
        test!("ඞbඞ", false, ['A' - 'B'], Some((3, 'b')));

        test!("fඞoǧbar", false, ['ǧ' - 'ǧ'], Some((5, 'ǧ')));
        test!("fඞoǧbar", false, ['Ǧ' - 'Ǧ'], Some((5, 'ǧ')));
        test!("fඞoǦbar", false, ['ǧ' - 'ǧ'], Some((5, 'Ǧ')));
    }
}
