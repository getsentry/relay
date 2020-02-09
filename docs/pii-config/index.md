# PII Configuration

The following document explores the syntax and semantics of PII configs for Relay. To get started with PII configs, it's recommended to use [_Piinguin_](https://getsentry.github.io/piinguin) and refer back to this document when needed.

## A basic example

Say you have an exception message which, unfortunately, contains IP addresses which are not supposed to be there. You'd write:

```json
{
  "applications": {
    "$string": ["@ip:replace"]
  }
}
```

It reads as "replace all IP addresses in all strings", or "apply `@ip:replace` to all `$string` fields".

`@ip:replace` is called a rule, and `$string` is a [_selector_](selectors.md).

## Built-in rules

The following rules exist by default:

- `@ip:replace` and `@ip:hash` for replacing IP addresses.
- `@imei:replace` and `@imei:hash` for replacing IMEIs
- `@mac:replace`, `@mac:mask` and `@mac:hash` for matching MAC addresses
- `@email:mask`, `@email:replace` and `@email:hash` for matching email addresses
- `@creditcard:mask`, `@creditcard:replace` and `@creditcard:hash` for matching creditcard numbers
- `@userpath:replace` and `@userpath:hash` for matching local paths (e.g. `C:/Users/foo/`)
- `@password:remove` for removing passwords. In this case we're pattern matching against the field's key, whether it contains `password`, `credentials` or similar strings.
- `@anything:remove`, `@anything:replace` and `@anything:hash` for removing, replacing or hashing any value. It is essentially equivalent to a wildcard-regex, but it will also match much more than strings.

## Writing your own rules

Rules generally consist of two parts:

- *Rule types* describe what to match. See [_PII Rule Types_](types.md) for an exhaustive list.
- *Rule redaction methods* describe what to do with the match. See [_PII Redaction Methods_](methods.md) for a list.

Each page comes with examples. Try those examples out by pasting them into the "PII config" column of [_Piinguin_](https://getsentry.github.io/piinguin) and clicking on fields to get suggestions.
