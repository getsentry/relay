use std::borrow::Cow;
use std::mem;

use once_cell::sync::OnceCell;
use regex::Regex;

use crate::pii::compiledconfig::RuleRef;
use crate::pii::regexes::{get_regex_for_rule_type, PatternType, ReplaceBehavior, ANYTHING_REGEX};
use crate::pii::utils::{hash_value, process_pairlist};
use crate::pii::{CompiledPiiConfig, Redaction, RuleType};
use crate::processor::{
    process_chunked_value, Chunk, Pii, ProcessValue, ProcessingState, Processor, ValueType,
};
use crate::protocol::{AsPair, IpAddr, NativeImagePath, PairList, Replay, User};
use crate::types::{Meta, ProcessingAction, ProcessingResult, Remark, RemarkType};

/// A processor that performs PII stripping.
pub struct PiiProcessor<'a> {
    compiled_config: &'a CompiledPiiConfig,
}

impl<'a> PiiProcessor<'a> {
    /// Creates a new processor based on a config.
    pub fn new(compiled_config: &'a CompiledPiiConfig) -> PiiProcessor<'a> {
        // this constructor needs to be cheap... a new PiiProcessor is created for each event. Move
        // any init logic into CompiledPiiConfig::new.
        PiiProcessor { compiled_config }
    }

    fn apply_all_rules(
        &self,
        mut meta: Option<&mut Meta>,
        state: &ProcessingState<'_>,
        mut value: Option<&mut String>,
    ) -> ProcessingResult {
        let pii = state.attrs().pii;
        if pii == Pii::False {
            return Ok(());
        }

        for (selector, rules) in self.compiled_config.applications.iter() {
            if state.path().matches_selector(selector) {
                #[allow(clippy::needless_option_as_deref)]
                for rule in rules {
                    dbg!(&rule);
                    let reborrowed_value = value.as_deref_mut();
                    dbg!(&reborrowed_value);
                    apply_rule_to_value(&mut meta, rule, state.path().key(), reborrowed_value)?;
                }
            }
        }

        Ok(())
    }
}

impl<'a> Processor for PiiProcessor<'a> {
    fn before_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        let mut foo = false;
        if let Some(crate::types::Value::String(original_value)) = meta.original_value_as_mut() {
            //dbg!(&original_value);
            if self
                .apply_all_rules(None, state, Some(original_value))
                .is_err()
            {
                foo = true;
            }
            dbg!(&original_value);
        }

        if foo {
            meta.set_original_value(Option::<String>::None);
        }

        // booleans cannot be PII, and strings are handled in process_string
        if state.value_type().contains(ValueType::Boolean)
            || state.value_type().contains(ValueType::String)
        {
            return Ok(());
        }

        if value.is_none() {
            return Ok(());
        }

        // apply rules based on key/path
        self.apply_all_rules(Some(meta), state, None)
    }

    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if let "" | "true" | "false" | "null" | "undefined" = value.as_str() {
            return Ok(());
        }

        // same as before_process. duplicated here because we can only check for "true",
        // "false" etc in process_string.
        self.apply_all_rules(Some(meta), state, Some(value))
    }

    fn process_native_image_path(
        &mut self,
        NativeImagePath(ref mut value): &mut NativeImagePath,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // In NativeImagePath we must not strip the file's basename because that would break
        // processing.
        //
        // We pop the basename from the end of the string, call process_string and push the
        // basename again.
        //
        // The ranges in Meta should still be right as long as we only pop/push from the end of the
        // string. If we decide that we need to preserve anything other than suffixes all PII
        // tooltips/annotations are potentially wrong.

        if let Some(index) = value.rfind(|c| c == '/' || c == '\\') {
            let basename = value.split_off(index);
            match self.process_string(value, meta, state) {
                Ok(()) => value.push_str(&basename),
                Err(ProcessingAction::DeleteValueHard) | Err(ProcessingAction::DeleteValueSoft) => {
                    *value = basename[1..].to_owned();
                }
                Err(ProcessingAction::InvalidTransaction(x)) => {
                    return Err(ProcessingAction::InvalidTransaction(x))
                }
            }
        }

        Ok(())
    }

    fn process_pairlist<T: ProcessValue + AsPair>(
        &mut self,
        value: &mut PairList<T>,
        _meta: &mut Meta,
        state: &ProcessingState,
    ) -> ProcessingResult {
        process_pairlist(self, value, state)
    }

    fn process_user(
        &mut self,
        user: &mut User,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        let ip_was_valid = user.ip_address.value().map_or(true, IpAddr::is_valid);

        // Recurse into the user and does PII processing on fields.
        user.process_child_values(self, state)?;

        let has_other_fields = user.id.value().is_some()
            || user.username.value().is_some()
            || user.email.value().is_some();

        let ip_is_still_valid = user.ip_address.value().map_or(true, IpAddr::is_valid);

        // If the IP address has become invalid as part of PII processing, we move it into the user
        // ID. That ensures people can do IP hashing and still have a correct users-affected count.
        //
        // Right now both Snuba and EventUser discard unparseable IPs for indexing, and we assume
        // we want to keep it that way.
        //
        // If there are any other fields set that take priority over the IP for uniquely
        // identifying a user (has_other_fields), we do not want to do anything. The value will be
        // wiped out in renormalization anyway.
        if ip_was_valid && !has_other_fields && !ip_is_still_valid {
            user.id = mem::take(&mut user.ip_address).map_value(|ip| ip.into_inner().into());
        }

        Ok(())
    }

    // Replay PII processor entry point.
    fn process_replay(
        &mut self,
        replay: &mut Replay,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        replay.process_child_values(self, state)?;
        Ok(())
    }
}

fn apply_rule_to_value(
    meta: &mut Option<&mut Meta>,
    rule: &RuleRef,
    key: Option<&str>,
    mut value: Option<&mut String>,
) -> ProcessingResult {
    // The rule might specify to remove or to redact. If redaction is chosen, we need to
    // chunk up the value, otherwise we need to simply mark the value for deletion.
    let should_redact_chunks = !matches!(rule.redaction, Redaction::Default | Redaction::Remove);

    // In case the value is not a string (but a container, bool or number) and the rule matches on
    // anything, we can only remove the value (not replace, hash, etc).
    if rule.ty == RuleType::Anything && (value.is_none() || !should_redact_chunks) {
        // The value is a container, @anything on a container can do nothing but delete.
        if let Some(meta) = meta {
            meta.add_remark(Remark::new(RemarkType::Removed, rule.origin.clone()));
        }
        return Err(ProcessingAction::DeleteValueHard);
    }

    macro_rules! apply_regex {
        ($regex:expr, $replace_behavior:expr) => {
            if let Some(ref mut value) = value {
                process_chunked_value(value, meta, |chunks| {
                    apply_regex_to_chunks(chunks, rule, $regex, $replace_behavior)
                });
            }
        };
    }

    for (pattern_type, regex, replace_behavior) in get_regex_for_rule_type(&rule.ty) {
        match pattern_type {
            PatternType::KeyValue => {
                if regex.is_match(key.unwrap_or("")) {
                    if value.is_some() && should_redact_chunks {
                        // If we're given a string value here, redact the value like we would with
                        // @anything.
                        apply_regex!(&ANYTHING_REGEX, replace_behavior);
                    } else {
                        if let Some(meta) = meta {
                            meta.add_remark(Remark::new(RemarkType::Removed, rule.origin.clone()))
                        }
                        return Err(ProcessingAction::DeleteValueHard);
                    }
                } else {
                    // If we did not redact using the key, we will redact the entire value if the key
                    // appears in it.
                    apply_regex!(regex, replace_behavior);
                }
            }
            PatternType::Value => {
                apply_regex!(regex, replace_behavior);
            }
        }
    }

    Ok(())
}

fn apply_regex_to_chunks<'a>(
    chunks: Vec<Chunk<'a>>,
    rule: &RuleRef,
    regex: &Regex,
    replace_behavior: ReplaceBehavior,
) -> Vec<Chunk<'a>> {
    // NB: This function allocates the entire string and all chunks a second time. This means it
    // cannot reuse chunks and reallocates them. Ideally, we would be able to run the regex directly
    // on the chunks, but the `regex` crate does not support that.

    let mut search_string = String::new();
    for chunk in &chunks {
        match chunk {
            Chunk::Text { text } => search_string.push_str(&text.replace('\x00', "")),
            Chunk::Redaction { .. } => search_string.push('\x00'),
        }
    }

    // Early exit if this regex does not match and return the original chunks.
    let mut captures_iter = regex.captures_iter(&search_string).peekable();
    if captures_iter.peek().is_none() {
        return chunks;
    }

    let mut replacement_chunks = vec![];
    for chunk in chunks {
        if let Chunk::Redaction { .. } = chunk {
            replacement_chunks.push(chunk);
        }
    }
    replacement_chunks.reverse();

    fn process_text<'a>(
        text: &str,
        rv: &mut Vec<Chunk<'a>>,
        replacement_chunks: &mut Vec<Chunk<'a>>,
    ) {
        if text.is_empty() {
            return;
        }

        static NULL_SPLIT_RE: OnceCell<Regex> = OnceCell::new();
        let regex = NULL_SPLIT_RE.get_or_init(|| {
            #[allow(clippy::trivial_regex)]
            Regex::new("\x00").unwrap()
        });

        let mut pos = 0;
        for piece in regex.find_iter(text) {
            rv.push(Chunk::Text {
                text: Cow::Owned(text[pos..piece.start()].to_string()),
            });
            rv.push(replacement_chunks.pop().unwrap());
            pos = piece.end();
        }

        rv.push(Chunk::Text {
            text: Cow::Owned(text[pos..].to_string()),
        });
    }

    let mut pos = 0;
    let mut rv = Vec::with_capacity(replacement_chunks.len());

    match replace_behavior {
        ReplaceBehavior::Groups(ref groups) => {
            for m in captures_iter {
                for (idx, g) in m.iter().enumerate() {
                    if let Some(g) = g {
                        if groups.contains(&(idx as u8)) {
                            process_text(
                                &search_string[pos..g.start()],
                                &mut rv,
                                &mut replacement_chunks,
                            );
                            insert_replacement_chunks(rule, g.as_str(), &mut rv);
                            pos = g.end();
                        }
                    }
                }
            }
            process_text(&search_string[pos..], &mut rv, &mut replacement_chunks);
            debug_assert!(replacement_chunks.is_empty());
        }
        ReplaceBehavior::Value => {
            // We only want to replace a string value, and the replacement chunk for that is
            // inserted by insert_replacement_chunks. Adding chunks from replacement_chunks
            // results in the incorrect behavior of a total of more chunks than the input.
            insert_replacement_chunks(rule, &search_string, &mut rv);
        }
    }

    rv
}

fn insert_replacement_chunks(rule: &RuleRef, text: &str, output: &mut Vec<Chunk<'_>>) {
    match &rule.redaction {
        Redaction::Default | Redaction::Remove => {
            output.push(Chunk::Redaction {
                text: Cow::Borrowed(""),
                rule_id: Cow::Owned(rule.origin.to_string()),
                ty: RemarkType::Removed,
            });
        }
        Redaction::Mask => {
            let buf = vec!['*'; text.chars().count()];

            output.push(Chunk::Redaction {
                ty: RemarkType::Masked,
                rule_id: Cow::Owned(rule.origin.to_string()),
                text: buf.into_iter().collect(),
            })
        }
        Redaction::Hash => {
            output.push(Chunk::Redaction {
                ty: RemarkType::Pseudonymized,
                rule_id: Cow::Owned(rule.origin.to_string()),
                text: Cow::Owned(hash_value(text.as_bytes())),
            });
        }
        Redaction::Replace(replace) => {
            output.push(Chunk::Redaction {
                ty: RemarkType::Substituted,
                rule_id: Cow::Owned(rule.origin.to_string()),
                text: Cow::Owned(replace.text.clone()),
            });
        }
        Redaction::Other => relay_log::warn!("Incoming redaction is not supported"),
    }
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use insta::assert_debug_snapshot;

    use crate::pii::{DataScrubbingConfig, PiiConfig, ReplaceRedaction};
    use crate::processor::process_value;
    use crate::protocol::{
        Addr, DataElement, DebugImage, DebugMeta, Event, ExtraValue, Headers, HttpElement,
        LogEntry, NativeDebugImage, Request, Span, TagEntry, Tags,
    };
    use crate::testutils::assert_annotated_snapshot;
    use crate::types::{Annotated, FromValue, Object, Value};

    use super::*;

    fn to_pii_config(datascrubbing_config: &DataScrubbingConfig) -> Option<PiiConfig> {
        use crate::pii::convert::to_pii_config as to_pii_config_impl;
        let rv = to_pii_config_impl(datascrubbing_config).unwrap();
        if let Some(ref config) = rv {
            let roundtrip: PiiConfig =
                serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
            assert_eq!(&roundtrip, config);
        }
        rv
    }

    #[test]
    fn test_ip_stripped() {
        let mut data = Event::from_value(
            serde_json::json!({
                "user": {
                    "username": "73.133.27.120", // should be stripped despite not being "known ip field"
                    "ip_address": "is this an ip address? ", //  <--------
                },
                "breadcrumbs": {
                    "values": [
                        {
                            "message": "73.133.27.120",
                            "data": {
                                "test_data": "73.133.27.120" // test deep wildcard stripping
                                }
                        },
                    ],
                },
                "sdk": {
                    "client_ip": "should also be stripped"
                }
            })
            .into(),
        );

        dbg!(&data);

        let scrubbing_config = DataScrubbingConfig {
            scrub_data: false,
            scrub_ip_addresses: true,
            scrub_defaults: false,
            ..Default::default()
        };

        let pii_config = to_pii_config(&scrubbing_config).unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();

        assert_debug_snapshot!(&data);
        assert!(data
            .value()
            .unwrap()
            .user
            .value()
            .unwrap()
            .ip_address
            .meta()
            .original_value()
            .is_none());
    }

    /*

         objection: find out why user's original value isn't getting stripped despite being PII.
           so it seems that its not getting stripped cause its not passed to process_string.
           objection: find out why its not passed to process_string.

           but does that even matter though?



           state of the whatever...

           1. it tries to parse the json into an Event. If any of the fields fail to be parsed, it'll be
           an annotated None value and in the meta, the original value will be set, which is the text
           that failed to become a proper object.

           2. A datascrubbingconfig is created. It contains info on the scrubbing of data within this
           event, such as fields to exclude, and whether it should scrub ip addresses. It also has a
           oncecell PIIConfig as a field. im not exactly sure why it's like this.

           3. a PiiConfig is created from the datascrubbingconfig. I mean, pii is more narrow than
           datascrubbing but it still seems a bit odd to me the way its done.

           4. a PiiProcessor is created from the PiiConfig. by itself it only has the apply_all_rules
           method which... applies the rules. but it also implements the Processor trait which should
           process the values so thats cool. i guess thats where all the PII stuff is going on.

           5. it creates a processingstate.. which is interesting, hmm

           6. put the processing state and the processor and the data in process_value()

           7. process_value() will generically do the following:
               1. do some magic before_process shit
               2. based on the result of that, either keep, delete, or mark as invalid the value
               3. do some processing
               4. same as #2

           specifically in our case it will do the following:

           8. in before_process it checks if value_type of state contains some stuff, but state havent
           interacted at all with the value, so thats weird.

           9. apply_all_rules is called but with None as value, which i can't udnerstand the point then.
    */
    #[test]
    fn test_basic_stripping() {
        let config = PiiConfig::from_json(
            r##"
            {
                "rules": {
                    "remove_bad_headers": {
                        "type": "redact_pair",
                        "keyPattern": "(?i)cookie|secret[-_]?key"
                    }
                },
                "applications": {
                    "$string": ["@ip"],
                    "$object.**": ["remove_bad_headers"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            logentry: Annotated::new(LogEntry {
                formatted: Annotated::new("Hello world!".to_string().into()),
                ..Default::default()
            }),
            request: Annotated::new(Request {
                env: {
                    let mut rv = Object::new();
                    rv.insert(
                        "SECRET_KEY".to_string(),
                        Annotated::new(Value::String("134141231231231231231312".into())),
                    );
                    Annotated::new(rv)
                },
                headers: {
                    let rv = vec![
                        Annotated::new((
                            Annotated::new("Cookie".to_string().into()),
                            Annotated::new("super secret".to_string().into()),
                        )),
                        Annotated::new((
                            Annotated::new("X-Forwarded-For".to_string().into()),
                            Annotated::new("127.0.0.1".to_string().into()),
                        )),
                    ];
                    Annotated::new(Headers(PairList(rv)))
                },
                ..Default::default()
            }),
            tags: Annotated::new(Tags(
                vec![Annotated::new(TagEntry(
                    Annotated::new("forwarded_for".to_string()),
                    Annotated::new("127.0.0.1".to_string()),
                ))]
                .into(),
            )),
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_redact_containers() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "$object": ["@anything"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            extra: {
                let mut map = Object::new();
                map.insert(
                    "foo".to_string(),
                    Annotated::new(ExtraValue(Value::String("bar".to_string()))),
                );
                Annotated::new(map)
            },
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_redact_custom_pattern() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "$string": ["myrule"]
                },
                "rules": {
                    "myrule": {
                        "type": "pattern",
                        "pattern": "foo",
                        "redaction": {
                            "method": "replace",
                            "text": "asd"
                        }
                    }
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            extra: {
                let mut map = Object::new();
                map.insert(
                    "myvalue".to_string(),
                    Annotated::new(ExtraValue(Value::String("foobar".to_string()))),
                );
                Annotated::new(map)
            },
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_no_field_upsert() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "**": ["@anything:remove"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            extra: {
                let mut map = Object::new();
                map.insert(
                    "myvalue".to_string(),
                    Annotated::new(ExtraValue(Value::String("foobar".to_string()))),
                );
                Annotated::new(map)
            },
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_anything_hash_on_string() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "$string": ["@anything:hash"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            extra: {
                let mut map = Object::new();
                map.insert(
                    "myvalue".to_string(),
                    Annotated::new(ExtraValue(Value::String("foobar".to_string()))),
                );
                Annotated::new(map)
            },
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_anything_hash_on_container() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "$object": ["@anything:hash"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            extra: {
                let mut map = Object::new();
                map.insert(
                    "myvalue".to_string(),
                    Annotated::new(ExtraValue(Value::String("foobar".to_string()))),
                );
                Annotated::new(map)
            },
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_remove_debugmeta_path() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "debug_meta.images.*.code_file": ["@anything:remove"],
                    "debug_meta.images.*.debug_file": ["@anything:remove"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            debug_meta: Annotated::new(DebugMeta {
                images: Annotated::new(vec![Annotated::new(DebugImage::Symbolic(Box::new(
                    NativeDebugImage {
                        code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
                        code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
                        debug_id: Annotated::new(
                            "971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap(),
                        ),
                        debug_file: Annotated::new("wntdll.pdb".into()),
                        debug_checksum: Annotated::empty(),
                        arch: Annotated::new("arm64".to_string()),
                        image_addr: Annotated::new(Addr(0)),
                        image_size: Annotated::new(4096),
                        image_vmaddr: Annotated::new(Addr(32768)),
                        other: {
                            let mut map = Object::new();
                            map.insert(
                                "other".to_string(),
                                Annotated::new(Value::String("value".to_string())),
                            );
                            map
                        },
                    },
                )))]),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_replace_debugmeta_path() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "debug_meta.images.*.code_file": ["@anything:replace"],
                    "debug_meta.images.*.debug_file": ["@anything:replace"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            debug_meta: Annotated::new(DebugMeta {
                images: Annotated::new(vec![Annotated::new(DebugImage::Symbolic(Box::new(
                    NativeDebugImage {
                        code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
                        code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
                        debug_id: Annotated::new(
                            "971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap(),
                        ),
                        debug_file: Annotated::new("wntdll.pdb".into()),
                        debug_checksum: Annotated::empty(),
                        arch: Annotated::new("arm64".to_string()),
                        image_addr: Annotated::new(Addr(0)),
                        image_size: Annotated::new(4096),
                        image_vmaddr: Annotated::new(Addr(32768)),
                        other: {
                            let mut map = Object::new();
                            map.insert(
                                "other".to_string(),
                                Annotated::new(Value::String("value".to_string())),
                            );
                            map
                        },
                    },
                )))]),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_hash_debugmeta_path() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "debug_meta.images.*.code_file": ["@anything:hash"],
                    "debug_meta.images.*.debug_file": ["@anything:hash"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            debug_meta: Annotated::new(DebugMeta {
                images: Annotated::new(vec![Annotated::new(DebugImage::Symbolic(Box::new(
                    NativeDebugImage {
                        code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
                        code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
                        debug_id: Annotated::new(
                            "971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap(),
                        ),
                        debug_file: Annotated::new("wntdll.pdb".into()),
                        debug_checksum: Annotated::empty(),
                        arch: Annotated::new("arm64".to_string()),
                        image_addr: Annotated::new(Addr(0)),
                        image_size: Annotated::new(4096),
                        image_vmaddr: Annotated::new(Addr(32768)),
                        other: {
                            let mut map = Object::new();
                            map.insert(
                                "other".to_string(),
                                Annotated::new(Value::String("value".to_string())),
                            );
                            map
                        },
                    },
                )))]),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_debugmeta_path_not_addressible_with_wildcard_selector() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "$string": ["@anything:remove"],
                    "**": ["@anything:remove"],
                    "debug_meta.**": ["@anything:remove"],
                    "(debug_meta.images.**.code_file & $string)": ["@anything:remove"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            debug_meta: Annotated::new(DebugMeta {
                images: Annotated::new(vec![Annotated::new(DebugImage::Symbolic(Box::new(
                    NativeDebugImage {
                        code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
                        code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
                        debug_id: Annotated::new(
                            "971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap(),
                        ),
                        debug_file: Annotated::new("wntdll.pdb".into()),
                        debug_checksum: Annotated::empty(),
                        arch: Annotated::new("arm64".to_string()),
                        image_addr: Annotated::new(Addr(0)),
                        image_size: Annotated::new(4096),
                        image_vmaddr: Annotated::new(Addr(32768)),
                        other: {
                            let mut map = Object::new();
                            map.insert(
                                "other".to_string(),
                                Annotated::new(Value::String("value".to_string())),
                            );
                            map
                        },
                    },
                )))]),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_quoted_keys() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "extra.'special ,./<>?!@#$%^&*())''gärbage'''": ["@anything:remove"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            extra: {
                let mut map = Object::new();
                map.insert(
                    "do not ,./<>?!@#$%^&*())'ßtrip'".to_string(),
                    Annotated::new(ExtraValue(Value::String("foo".to_string()))),
                );
                map.insert(
                    "special ,./<>?!@#$%^&*())'gärbage'".to_string(),
                    Annotated::new(ExtraValue(Value::String("bar".to_string()))),
                );
                Annotated::new(map)
            },
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_logentry_value_types() {
        // Assert that logentry.formatted is addressable as $string, $message and $logentry.formatted
        for formatted_selector in &[
            "$logentry.formatted",
            "$message",
            "$logentry.formatted && $message",
            "$string",
        ] {
            let config = PiiConfig::from_json(&format!(
                r##"
                {{
                    "applications": {{
                        "{formatted_selector}": ["@anything:remove"]
                    }}
                }}
                "##,
                formatted_selector = dbg!(formatted_selector),
            ))
            .unwrap();

            let mut event = Annotated::new(Event {
                logentry: Annotated::new(LogEntry {
                    formatted: Annotated::new("Hello world!".to_string().into()),
                    ..Default::default()
                }),
                ..Default::default()
            });

            let mut processor = PiiProcessor::new(config.compiled());
            process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

            assert!(event
                .value()
                .unwrap()
                .logentry
                .value()
                .unwrap()
                .formatted
                .value()
                .is_none());
        }
    }

    #[test]
    fn test_ip_address_hashing() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "$user.ip_address": ["@ip:hash"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            user: Annotated::new(User {
                ip_address: Annotated::new(IpAddr("127.0.0.1".to_string())),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let user = event.value().unwrap().user.value().unwrap();

        assert!(user.ip_address.value().is_none());

        assert_eq!(
            user.id.value().unwrap().as_str(),
            "AE12FE3B5F129B5CC4CDD2B136B7B7947C4D2741"
        );
    }

    #[test]
    fn test_ip_address_hashing_does_not_overwrite_id() {
        let config = PiiConfig::from_json(
            r##"
            {
                "applications": {
                    "$user.ip_address": ["@ip:hash"]
                }
            }
            "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            user: Annotated::new(User {
                id: Annotated::new("123".to_string().into()),
                ip_address: Annotated::new(IpAddr("127.0.0.1".to_string())),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let user = event.value().unwrap().user.value().unwrap();

        // This will get wiped out in renormalization though
        assert_eq!(
            user.ip_address.value().unwrap().as_str(),
            "AE12FE3B5F129B5CC4CDD2B136B7B7947C4D2741"
        );

        assert_eq!(user.id.value().unwrap().as_str(), "123");
    }

    #[test]
    fn test_replace_replaced_text() {
        let chunks = vec![Chunk::Redaction {
            text: "[ip]".into(),
            rule_id: "@ip".into(),
            ty: RemarkType::Substituted,
        }];
        let rule = RuleRef {
            id: "@ip:replace".into(),
            origin: "@ip".into(),
            ty: RuleType::Ip,
            redaction: Redaction::Replace(ReplaceRedaction {
                text: "[ip]".into(),
            }),
        };
        let res = apply_regex_to_chunks(
            chunks.clone(),
            &rule,
            &Regex::new(r#".*"#).unwrap(),
            ReplaceBehavior::Value,
        );
        assert_eq!(chunks, res);
    }

    #[test]
    fn test_scrub_span_data_not_scrubbed() {
        let mut span = Annotated::new(Span {
            data: Annotated::new(DataElement {
                http: Annotated::new(HttpElement {
                    query: Annotated::new(Value::String("dance=true".to_owned())),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        let ds_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };
        let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        process_value(&mut span, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(span);
    }

    #[test]
    fn test_scrub_span_data_is_scrubbed() {
        let mut span = Annotated::new(Span {
            data: Annotated::new(DataElement {
                http: Annotated::new(HttpElement {
                    query: Annotated::new(Value::String(
                        "ccnumber=5105105105105100&process_id=123".to_owned(),
                    )),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        let ds_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };
        let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        process_value(&mut span, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(span);
    }

    #[test]
    fn test_scrub_span_data_object_is_scrubbed() {
        let mut span = Annotated::new(Span {
            data: Annotated::new(DataElement {
                http: Annotated::new(HttpElement {
                    query: Annotated::new(Value::Object({
                        let mut map = BTreeMap::new();
                        map.insert(
                            "ccnumber".to_owned(),
                            Annotated::new(Value::String("5105105105105100".to_owned())),
                        );
                        map.insert(
                            "process_id".to_owned(),
                            Annotated::new(Value::String("123".to_owned())),
                        );
                        map
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        let ds_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };
        let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        process_value(&mut span, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(span);
    }
}
