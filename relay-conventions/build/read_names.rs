use std::collections::{BTreeMap, BTreeSet};

pub struct ReadAttribute<'a> {
    pub ty: &'a str,
    pub replacement: Option<&'a str>,
}

pub fn read_names<'a>(
    attributes: &BTreeMap<&'a str, ReadAttribute<'a>>,
) -> BTreeMap<&'a str, Vec<&'a str>> {
    let mut replacements = BTreeMap::new();
    let mut predecessors: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();

    for (&key, attribute) in attributes {
        let Some(replacement) = attribute.replacement else {
            continue;
        };
        let target = attributes
            .get(replacement)
            .unwrap_or_else(|| panic!("Unknown replacement {replacement} for {key}"));
        if attribute.ty != target.ty || key.contains("<key>") != replacement.contains("<key>") {
            continue;
        }
        replacements.insert(key, replacement);
        predecessors.entry(replacement).or_default().insert(key);
    }

    let mut result = BTreeMap::new();
    for &key in attributes.keys() {
        let mut head = key;
        let mut visited = BTreeSet::from([head]);
        while let Some(&replacement) = replacements.get(head) {
            assert!(
                visited.insert(replacement),
                "Deprecation cycle involving {replacement}"
            );
            head = replacement;
        }

        let mut names = Vec::new();
        let mut level = BTreeSet::from([head]);
        while !level.is_empty() {
            let mut next = BTreeSet::new();
            for name in level {
                names.push(name);
                if let Some(older_names) = predecessors.get(name) {
                    next.extend(older_names);
                }
            }
            level = next;
        }
        result.insert(key, names);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_distance_then_name_for_every_member() {
        let attributes = BTreeMap::from([
            (
                "current",
                ReadAttribute {
                    ty: "string",
                    replacement: None,
                },
            ),
            (
                "middle_b",
                ReadAttribute {
                    ty: "string",
                    replacement: Some("current"),
                },
            ),
            (
                "middle_a",
                ReadAttribute {
                    ty: "string",
                    replacement: Some("current"),
                },
            ),
            (
                "oldest_a",
                ReadAttribute {
                    ty: "string",
                    replacement: Some("middle_b"),
                },
            ),
            (
                "oldest_b",
                ReadAttribute {
                    ty: "string",
                    replacement: Some("middle_a"),
                },
            ),
        ]);
        for names in read_names(&attributes).values() {
            assert_eq!(
                names,
                &["current", "middle_a", "middle_b", "oldest_a", "oldest_b"]
            );
        }
    }

    #[test]
    fn excludes_incompatible_types_and_key_shapes() {
        let attributes = BTreeMap::from([
            (
                "current.<key>",
                ReadAttribute {
                    ty: "string",
                    replacement: None,
                },
            ),
            (
                "old.<key>",
                ReadAttribute {
                    ty: "string",
                    replacement: Some("current.<key>"),
                },
            ),
            (
                "array.<key>",
                ReadAttribute {
                    ty: "string[]",
                    replacement: Some("current.<key>"),
                },
            ),
            (
                "scalar",
                ReadAttribute {
                    ty: "string",
                    replacement: Some("current.<key>"),
                },
            ),
            (
                "query.<key>",
                ReadAttribute {
                    ty: "string",
                    replacement: Some("query"),
                },
            ),
            (
                "query",
                ReadAttribute {
                    ty: "string",
                    replacement: None,
                },
            ),
        ]);
        let names = read_names(&attributes);
        assert_eq!(names["current.<key>"], ["current.<key>", "old.<key>"]);
        assert_eq!(names["old.<key>"], names["current.<key>"]);
        for key in ["array.<key>", "scalar", "query.<key>", "query"] {
            assert_eq!(names[key], [key]);
        }
    }

    #[test]
    #[should_panic(expected = "Deprecation cycle")]
    fn rejects_cycles() {
        read_names(&BTreeMap::from([
            (
                "a",
                ReadAttribute {
                    ty: "string",
                    replacement: Some("b"),
                },
            ),
            (
                "b",
                ReadAttribute {
                    ty: "string",
                    replacement: Some("a"),
                },
            ),
        ]));
    }

    #[test]
    #[should_panic(expected = "Unknown replacement")]
    fn rejects_unknown_replacements() {
        read_names(&BTreeMap::from([(
            "a",
            ReadAttribute {
                ty: "string",
                replacement: Some("unknown"),
            },
        )]));
    }
}
