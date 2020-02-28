# PII Selectors

You have the possibility to select a region of the event using JSON-path-like syntax. To delete a specific key in "Additional Data", you could for example use:

```json
{
  "applications": {
    "extra.foo": ["@anything:remove"]
  }
}
```

## Boolean logic

You can combine selectors using boolean logic.

* Prefix with `~` to invert the selector. `foo` matches the JSON key `foo`, while `(~foo)` matches everything but `foo`.
* Build the conjunction (AND) using `&`, such as: `(foo&(~extra.foo))` to match the key `foo` except when inside of `extra`.
* Build the disjunction (OR) using `|`, such as: `(foo|bar)` to match `foo` or `bar`.

## Wildcards

* `**` matches all subpaths, so that `foo.**` matches all JSON keys within `foo`.
* `*` matches a single path item, so that `foo.*` matches all JSON keys one level below `foo`.

## Value types

The following can be used to select subsections by JSON-type or semantic meaning.

* `$string`
* `$number`
* `$boolean`
* `$datetime`
* `$array`
* `$object`
* `$event`
* `$exception`
* `$stacktrace`
* `$frame`
* `$request`
* `$user`
* `$logentry` (also applies to `event.message`)
* `$thread`
* `$breadcrumb`
* `$span`
* `$sdk`

Examples:

* Delete `event.user`:

  ```json
  {
    "applications": {
      "$user": ["@anything:remove"]
    }
  }
  ```

* Delete all frame-local variables:

  ```json
  {
    "applications": {
      "$frame.vars": ["@anything:remove"]
    }
  }
  ```

## Escaping specal characters

If the object key you want to match contains whitespace or special characters, you can use quotes to escape it:

```json
{
  "applications": {
    "extra.'my special value'": ["@anything:remove"]
  }
}
```

To escape `'` within the quotes, replace it with `''`.
