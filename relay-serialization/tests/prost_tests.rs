mod prost {
    use prost::Message;
    use relay_serialization::prost::{decode, ops, scan};
    use relay_serialization_derive::RuntimeDescription as DeriveRuntimeDescription;

    /// A leaf message with one field of every shape the scanner dispatches on.
    #[derive(Clone, PartialEq, Message, DeriveRuntimeDescription)]
    struct Leaf {
        #[prost(string, tag = "1")]
        text: String,
        #[prost(uint64, tag = "2")]
        number: u64,
        #[prost(double, tag = "3")]
        double: f64,
        #[prost(fixed32, tag = "4")]
        fixed: u32,
        #[prost(bytes = "vec", tag = "5")]
        blob: Vec<u8>,
        #[prost(oneof = "Var", tags = "6, 7")]
        one_of: ::core::option::Option<Var>,
    }

    /// A message which nests, so the scanner has to recurse to see the leaves.
    #[derive(Clone, PartialEq, Message, DeriveRuntimeDescription)]
    struct Branch {
        #[prost(message, repeated, tag = "1")]
        leaves: Vec<Leaf>,
        #[prost(message, optional, boxed, tag = "2")]
        branch: Option<Box<Branch>>,
        #[prost(string, tag = "3")]
        label: String,
    }

    /// A message which nests, so the scanner has to recurse to see the leaves.
    #[derive(Clone, PartialEq, prost::Oneof, DeriveRuntimeDescription)]
    enum Var {
        #[prost(string, tag = "6")]
        Name(String),
        // /// Recursion through a `oneof`.
        #[prost(message, tag = "7")]
        Leaf(Box<Branch>),
    }

    #[test]
    fn test_scan_charges_each_empty_element() {
        // The case a size bound cannot see: every element is tiny, but there are a great many.
        for count in [0, 1, 2, 512] {
            let branch = Branch {
                leaves: vec![Leaf::default(); count],
                ..Default::default()
            };

            // One op for each `leaves` occurrence. An empty `Leaf` has no fields of its own, and
            // default scalars are not encoded at all in proto3.
            assert_eq!(ops(&branch), count);
        }
    }

    #[test]
    fn test_scan_charges_opaque_payload_once() {
        // A megabyte in one field costs one op, where a byte meter would charge a megabyte.
        let branch = Branch {
            label: "a".repeat(1 << 20),
            ..Default::default()
        };

        assert_eq!(ops(&branch), 1);
    }

    #[test]
    fn test_scan_charges_every_depth() {
        let branch = Branch {
            leaves: vec![
                Leaf {
                    text: "one".to_owned(),
                    number: 1,
                    double: 1.5,
                    fixed: 2,
                    blob: vec![1, 2, 3],
                    one_of: Some(Var::Leaf(Box::new(Branch {
                        leaves: vec![],
                        branch: None,
                        label: "done".to_owned(),
                    }))),
                },
                Leaf {
                    number: 7,
                    ..Default::default()
                },
            ],
            branch: Some(Box::new(Branch {
                label: "inner".to_owned(),
                ..Default::default()
            })),
            label: "outer".to_owned(),
        };

        // Two `leaves` occurrences carrying five and one field, one `branch` carrying one field,
        // the outer `label`, and then finally the one_of field + the child branch's label.
        let expected = (1 + 5) + (1 + 1) + (1 + 1) + 1 + (1 + 1);
        assert_eq!(ops(&branch), expected);
    }

    #[test]
    fn test_scan_exceeds_budget() {
        let branch = Branch {
            leaves: vec![Leaf::default(); 4096],
            ..Default::default()
        };

        let buf = branch.encode_to_vec();
        let error = scan::<Branch>(&buf, 128).unwrap_err();
        assert!(error.is_limit_exceeded());
        assert_eq!(error.to_string(), "message exceeds the operation limit");
    }

    #[test]
    fn test_scan_exceeds_budget_when_nested() {
        // The budget has to survive recursion: the fields are all four levels down.
        let leaves = vec![Leaf::default(); 4096];
        let branch = Branch {
            branch: Some(Box::new(Branch {
                branch: Some(Box::new(Branch {
                    leaves,
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };

        let buf = branch.encode_to_vec();
        let error = scan::<Branch>(&buf, 256).unwrap_err();
        assert!(error.is_limit_exceeded());
    }

    #[test]
    fn test_decode_rejects_before_decoding() {
        let branch = Branch {
            leaves: vec![Leaf::default(); 4096],
            ..Default::default()
        };
        let payload = branch.encode_to_vec();

        assert!(
            decode::<Branch>(&payload, 128)
                .unwrap_err()
                .is_limit_exceeded()
        );
        // The same payload decodes once the budget accommodates it.
        let decoded = decode::<Branch>(&payload, 1 << 20).unwrap();
        assert_eq!(decoded, branch);
    }

    #[test]
    fn test_scan_accepts_what_prost_accepts() {
        // An unknown field prost would skip: tag 9, length delimited, holding a nested message the
        // scanner has no descriptor for. It is charged once and walked over.
        let payload = [0x4a, 0x04, 0x08, 0x01, 0x10, 0x02];

        assert!(Branch::decode(payload.as_slice()).is_ok());
        assert!(scan::<Branch>(&payload, 1).is_ok());
    }

    #[test]
    fn test_scan_rejects_malformed_payloads() {
        // A truncated length delimiter, a tag of zero, and a varint with no terminator.
        for payload in [
            [0x1a, 0x08, 0x61].as_slice(),
            [0x00, 0x01].as_slice(),
            [0x08, 0xff].as_slice(),
        ] {
            let error = decode::<Branch>(payload, 1 << 20).unwrap_err();
            assert!(error.is_scan_error(), "{payload:?}");
            // Whatever the scanner rejects, prost rejects too.
            assert!(Branch::decode(payload).is_err(), "{payload:?}");
        }
    }

    #[test]
    fn test_scan_bounds_its_own_recursion() {
        // Deeper than the recursion limit, so the scanner must not run out of stack walking it.
        let mut payload = Vec::new();
        for _ in 0..101 {
            let mut framed = vec![0x12, payload.len() as u8];
            framed.extend_from_slice(&payload);
            payload = framed;
        }

        let error = scan::<Branch>(&payload, 1 << 20).unwrap_err();
        assert!(!error.is_limit_exceeded());
        assert!(Branch::decode(payload.as_slice()).is_err());
    }

    #[test]
    fn test_map_fails_build() {
        let t = trybuild::TestCases::new();
        t.compile_fail("tests/build_failures/map_fails.rs");
    }
}
