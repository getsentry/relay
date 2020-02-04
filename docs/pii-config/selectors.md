# PII Selectors

Instead of `text` you have the possibility to select a region of the event
using JSON-path-like syntax. To delete a specific key in "Additional Data", you could for example use:

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

## Shortcuts

* `text` matches all strings. In most cases this can be replaced with `**`, as applying a regex on e.g. an integer or JSON object just does nothing.
