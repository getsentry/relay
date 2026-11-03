import pytest

import sentry_relay


@pytest.mark.parametrize(
    "pattern,value,expected",
    [
        ("*.{js,py}", "src/hello.py", True),
        ("*.{js,py}", "src/hello.rs", False),
        ("*.py", "hello.py.bak", False),
        ("h?llo", "héllo", True),
        ("[a-z]*", "hello", True),
        ("[a-z]*", "123", False),
        ("[!a-z]*", "123", True),
        (r"\*", "*", True),
        ("*", "hello\nworld", True),
        ("hello*", "hello\0world", True),
        ("", "", False),
        ("*", "", True),
    ],
)
@pytest.mark.parametrize("gas", [None, 1000])
def test_pattern_matching(pattern, value, expected, gas):
    assert sentry_relay.Pattern(pattern).is_match(value, gas=gas) is expected


@pytest.mark.parametrize("case_insensitive", [False, True])
def test_pattern_case_insensitive(case_insensitive):
    pattern = sentry_relay.Pattern("*.{js,PY}", case_insensitive=case_insensitive)
    assert pattern.is_match("src/hello.js")
    assert pattern.is_match("src/hello.PY")
    assert pattern.is_match("src/hello.JS") is case_insensitive
    assert pattern.is_match("src/hello.py") is case_insensitive


def test_pattern_case_sensitive_by_default():
    pattern = sentry_relay.Pattern("Hello*")
    assert pattern.is_match("Hello world")
    assert not pattern.is_match("hello world")


def test_pattern_unicode_case_insensitive():
    pattern = sentry_relay.Pattern("Äpfel*", case_insensitive=True)
    assert pattern.is_match("äpfel und birnen")


def test_pattern_str():
    pattern = sentry_relay.Pattern("Foo**", case_insensitive=True)
    assert str(pattern) == "foo*"


@pytest.mark.parametrize("pattern", ["[", "[z-a]", "hello}", "\\"])
def test_invalid_pattern(pattern):
    with pytest.raises(sentry_relay.PatternError, match="Error parsing pattern"):
        sentry_relay.Pattern(pattern)


def test_pattern_gas():
    pattern = sentry_relay.Pattern("*{*a}a{*a,b}b")
    with pytest.raises(
        sentry_relay.PatternOutOfGas,
        match="Pattern could not be matched with 5 ops",
    ):
        pattern.is_match("aaaaaaaaaaaaaaaaa", gas=5)

    assert not pattern.is_match("aaaaaaaaaaaaaaaaa", gas=1000)
    assert pattern.is_match("aaaaaaaaaaaaaaaaab", gas=1000)
