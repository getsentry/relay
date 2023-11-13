use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use once_cell::sync::OnceCell;
use regex::Regex;
use relay_event_schema::processor::{
    self, enum_set, Chunk, Pii, ProcessValue, ProcessingAction, ProcessingResult, ProcessingState,
    Processor, ValueType,
};
use relay_event_schema::protocol::{
    AsPair, Event, IpAddr, NativeImagePath, PairList, Replay, ResponseContext, User,
};
use relay_protocol::{Annotated, Meta, Remark, RemarkType, Value};

use crate::compiledconfig::{CompiledPiiConfig, RuleRef};
use crate::config::RuleType;
use crate::redactions::Redaction;
use crate::regexes::{self, PatternType, ReplaceBehavior, ANYTHING_REGEX};
use crate::utils;

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
        meta: &mut Meta,
        state: &ProcessingState<'_>,
        mut value: Option<&mut String>,
    ) -> ProcessingResult {
        let pii = state.attrs().pii;
        if pii == Pii::False {
            return Ok(());
        }

        for (selector, rules) in self.compiled_config.applications.iter() {
            if selector.matches_path(&state.path()) {
                #[allow(clippy::needless_option_as_deref)]
                for rule in rules {
                    let reborrowed_value = value.as_deref_mut();
                    apply_rule_to_value(meta, rule, state.path().key(), reborrowed_value)?;
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
        if let Some(Value::String(original_value)) = meta.original_value_as_mut() {
            // Also apply pii scrubbing to the original value (set by normalization or other processors),
            // such that we do not leak sensitive data through meta. Deletes `original_value` if an Error
            // value is returned.
            if let Some(parent) = state.iter().next() {
                let path = state.path();
                let new_state = parent.enter_borrowed(
                    path.key().unwrap_or(""),
                    Some(Cow::Borrowed(state.attrs())),
                    enum_set!(ValueType::String),
                );

                if self
                    .apply_all_rules(&mut Meta::default(), &new_state, Some(original_value))
                    .is_err()
                {
                    // `apply_all_rules` returned `DeleteValueHard` or `DeleteValueSoft`, so delete the original as well.
                    meta.set_original_value(Option::<String>::None);
                }
            }
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
        self.apply_all_rules(meta, state, None)
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
        self.apply_all_rules(meta, state, Some(value))
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
        utils::process_pairlist(self, value, state)
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

/// Scrubs GraphQL variables from the event.
pub fn scrub_graphql(event: &mut Event) {
    let mut keys: BTreeSet<&str> = BTreeSet::new();

    let mut is_graphql = false;

    // Collect the variables keys and scrub them out.
    if let Some(request) = event.request.value_mut() {
        if let Some(Value::Object(data)) = request.data.value_mut() {
            if let Some(api_target) = request.api_target.value() {
                if api_target.eq_ignore_ascii_case("graphql") {
                    is_graphql = true;
                }
            }

            if is_graphql {
                if let Some(Annotated(Some(Value::Object(variables)), _)) =
                    data.get_mut("variables")
                {
                    for (key, value) in variables.iter_mut() {
                        keys.insert(key);
                        value.set_value(Some(Value::String("[Filtered]".to_string())));
                    }
                }
            }
        }
    }

    if !is_graphql {
        return;
    }

    // Scrub PII from the data object if they match the variables keys.
    if let Some(contexts) = event.contexts.value_mut() {
        if let Some(response) = contexts.get_mut::<ResponseContext>() {
            if let Some(Value::Object(data)) = response.data.value_mut() {
                if let Some(Annotated(Some(Value::Object(graphql_data)), _)) = data.get_mut("data")
                {
                    if !keys.is_empty() {
                        scrub_graphql_data(&keys, graphql_data);
                    } else {
                        // If we don't have the variable keys, we scrub the whole data object
                        // because the query or mutation weren't parameterized.
                        data.remove("data");
                    }
                }
            }
        }
    }
}

/// Scrubs values from the data object to `[Filtered]`.
fn scrub_graphql_data(keys: &BTreeSet<&str>, data: &mut BTreeMap<String, Annotated<Value>>) {
    for (key, value) in data.iter_mut() {
        match value.value_mut() {
            Some(Value::Object(item_data)) => {
                scrub_graphql_data(keys, item_data);
            }
            _ => {
                if keys.contains(key.as_str()) {
                    value.set_value(Some(Value::String("[Filtered]".to_string())));
                }
            }
        }
    }
}

fn apply_rule_to_value(
    meta: &mut Meta,
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
        meta.add_remark(Remark::new(RemarkType::Removed, rule.origin.clone()));
        return Err(ProcessingAction::DeleteValueHard);
    }

    macro_rules! apply_regex {
        ($regex:expr, $replace_behavior:expr) => {
            if let Some(ref mut value) = value {
                processor::process_chunked_value(value, meta, |chunks| {
                    apply_regex_to_chunks(chunks, rule, $regex, $replace_behavior)
                });
            }
        };
    }

    for (pattern_type, regex, replace_behavior) in regexes::get_regex_for_rule_type(&rule.ty) {
        match pattern_type {
            PatternType::KeyValue => {
                if regex.is_match(key.unwrap_or("")) {
                    if value.is_some() && should_redact_chunks {
                        // If we're given a string value here, redact the value like we would with
                        // @anything.
                        apply_regex!(&ANYTHING_REGEX, replace_behavior);
                    } else {
                        meta.add_remark(Remark::new(RemarkType::Removed, rule.origin.clone()));
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
    let mut has_text = false;
    for chunk in &chunks {
        match chunk {
            Chunk::Text { text } => {
                has_text = true;
                search_string.push_str(&text.replace('\x00', ""));
            }
            Chunk::Redaction { .. } => search_string.push('\x00'),
        }
    }

    if !has_text {
        // Nothing to replace.
        return chunks;
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
                text: Cow::Owned(utils::hash_value(text.as_bytes())),
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
    use insta::assert_debug_snapshot;
    use relay_event_schema::processor::process_value;
    use relay_event_schema::protocol::{
        Addr, Breadcrumb, DebugImage, DebugMeta, Event, ExtraValue, Headers, LogEntry,
        NativeDebugImage, Request, Span, TagEntry, Tags,
    };
    use relay_protocol::{assert_annotated_snapshot, Annotated, FromValue, Object, Value};

    use super::*;
    use crate::{DataScrubbingConfig, PiiConfig, ReplaceRedaction};

    fn to_pii_config(datascrubbing_config: &DataScrubbingConfig) -> Option<PiiConfig> {
        use crate::convert::to_pii_config as to_pii_config_impl;
        let rv = to_pii_config_impl(datascrubbing_config).unwrap();
        if let Some(ref config) = rv {
            let roundtrip: PiiConfig =
                serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
            assert_eq!(&roundtrip, config);
        }
        rv
    }

    #[test]
    fn test_scrub_original_value() {
        let mut data = Event::from_value(
            serde_json::json!({
                "user": {
                    "username": "hey  man 73.133.27.120", // should be stripped despite not being "known ip field"
                    "ip_address": "is this an ip address? 73.133.27.120", //  <--------
                },
                "hpkp":"invalid data my ip address is  74.133.27.120 and my credit card number is  4571234567890111 ",
            })
            .into(),
        );

        let scrubbing_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_ip_addresses: true,
            scrub_defaults: true,
            ..Default::default()
        };

        let pii_config = to_pii_config(&scrubbing_config).unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();

        assert_debug_snapshot!(&data);
    }

    #[test]
    fn test_basic_stripping() {
        let config = serde_json::from_str::<PiiConfig>(
            r#"
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
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "$object": ["@anything"]
                }
            }
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
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
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "**": ["@anything:remove"]
                }
            }
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "$string": ["@anything:hash"]
                }
            }
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "$object": ["@anything:hash"]
                }
            }
            "#,
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
    fn test_ignore_user_agent_ip_scrubbing() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "headers": [
                        ["User-Agent", "127.0.0.1"],
                        ["X-Client-Ip", "10.0.0.1"]
                    ]
                },
            })
            .into(),
        );

        let scrubbing_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_ip_addresses: true,
            scrub_defaults: true,
            ..Default::default()
        };

        let pii_config = to_pii_config(&scrubbing_config).unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();

        assert_annotated_snapshot!(&data);
    }

    #[test]
    fn test_remove_debugmeta_path() {
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "debug_meta.images.*.code_file": ["@anything:remove"],
                    "debug_meta.images.*.debug_file": ["@anything:remove"]
                }
            }
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "debug_meta.images.*.code_file": ["@anything:replace"],
                    "debug_meta.images.*.debug_file": ["@anything:replace"]
                }
            }
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "debug_meta.images.*.code_file": ["@anything:hash"],
                    "debug_meta.images.*.debug_file": ["@anything:hash"]
                }
            }
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "$string": ["@anything:remove"],
                    "**": ["@anything:remove"],
                    "debug_meta.**": ["@anything:remove"],
                    "(debug_meta.images.**.code_file & $string)": ["@anything:remove"]
                }
            }
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "extra.'special ,./<>?!@#$%^&*())''gärbage'''": ["@anything:remove"]
                }
            }
            "#,
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
            let config = serde_json::from_str::<PiiConfig>(&format!(
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "$user.ip_address": ["@ip:hash"]
                }
            }
            "#,
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
        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "$user.ip_address": ["@ip:hash"]
                }
            }
            "#,
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
    fn test_replace_replaced_text_anything() {
        let chunks = vec![Chunk::Redaction {
            text: "[Filtered]".into(),
            rule_id: "@password:filter".into(),
            ty: RemarkType::Substituted,
        }];
        let rule = RuleRef {
            id: "@anything:filter".into(),
            origin: "@anything:filter".into(),
            ty: RuleType::Anything,
            redaction: Redaction::Replace(ReplaceRedaction {
                text: "[Filtered]".into(),
            }),
        };
        let res = apply_regex_to_chunks(
            chunks.clone(),
            &rule,
            &Regex::new(r#".*"#).unwrap(),
            ReplaceBehavior::Groups(smallvec::smallvec![0]),
        );
        assert_eq!(chunks, res);
    }

    #[test]
    fn test_scrub_span_data_http_not_scrubbed() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "http": {
                        "query": "dance=true"
                    }
                }
            }"#,
        )
        .unwrap();

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
    fn test_scrub_span_data_http_strings_are_scrubbed() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "http": {
                        "query": "ccnumber=5105105105105100&process_id=123",
                        "fragment": "ccnumber=5105105105105100,process_id=123"
                    }
                }
            }"#,
        )
        .unwrap();

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
    fn test_scrub_span_data_http_objects_are_scrubbed() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "http": {
                        "query": {
                            "ccnumber": "5105105105105100",
                            "process_id": "123"
                        },
                        "fragment": {
                            "ccnumber": "5105105105105100",
                            "process_id": "123"
                        }
                    }
                }
            }"#,
        )
        .unwrap();

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
    fn test_scrub_span_data_untyped_props_are_scrubbed() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "untyped": "ccnumber=5105105105105100",
                    "more_untyped": {
                        "typed": "no",
                        "scrubbed": "yes",
                        "ccnumber": "5105105105105100"
                    }
                }
            }"#,
        )
        .unwrap();

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
    fn test_scrub_breadcrumb_data_http_not_scrubbed() {
        let mut breadcrumb: Annotated<Breadcrumb> = Annotated::from_json(
            r#"{
                "data": {
                    "http": {
                        "query": "dance=true"
                    }
                }
            }"#,
        )
        .unwrap();

        let ds_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };
        let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut breadcrumb, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(breadcrumb);
    }

    #[test]
    fn test_scrub_breadcrumb_data_http_strings_are_scrubbed() {
        let mut breadcrumb: Annotated<Breadcrumb> = Annotated::from_json(
            r#"{
                "data": {
                    "http": {
                        "query": "ccnumber=5105105105105100&process_id=123",
                        "fragment": "ccnumber=5105105105105100,process_id=123"
                    }
                }
            }"#,
        )
        .unwrap();

        let ds_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };
        let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut breadcrumb, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(breadcrumb);
    }

    #[test]
    fn test_scrub_breadcrumb_data_http_objects_are_scrubbed() {
        let mut breadcrumb: Annotated<Breadcrumb> = Annotated::from_json(
            r#"{
                "data": {
                    "http": {
                        "query": {
                            "ccnumber": "5105105105105100",
                            "process_id": "123"
                        },
                        "fragment": {
                            "ccnumber": "5105105105105100",
                            "process_id": "123"
                        }
                    }
                }
            }"#,
        )
        .unwrap();

        let ds_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };
        let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        process_value(&mut breadcrumb, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(breadcrumb);
    }

    #[test]
    fn test_scrub_breadcrumb_data_untyped_props_are_scrubbed() {
        let mut breadcrumb: Annotated<Breadcrumb> = Annotated::from_json(
            r#"{
                "data": {
                    "untyped": "ccnumber=5105105105105100",
                    "more_untyped": {
                        "typed": "no",
                        "scrubbed": "yes",
                        "ccnumber": "5105105105105100"
                    }
                }
            }"#,
        )
        .unwrap();

        let ds_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };
        let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut breadcrumb, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(breadcrumb);
    }

    #[test]
    fn test_scrub_graphql_response_data_with_variables() {
        let mut data = Event::from_value(
            serde_json::json!({
              "request": {
                "data": {
                  "query": "{\n  viewer {\n    login\n  }\n}",
                  "variables": {
                    "login": "foo"
                  }
                },
                "api_target": "graphql"
              },
              "contexts": {
                "response": {
                  "type": "response",
                  "data": {
                    "data": {
                      "viewer": {
                        "login": "foo"
                      }
                    }
                  }
                }
              }
            })
            .into(),
        );

        scrub_graphql(data.value_mut().as_mut().unwrap());

        assert_debug_snapshot!(&data);
    }

    #[test]
    fn test_scrub_graphql_response_data_without_variables() {
        let mut data = Event::from_value(
            serde_json::json!({
              "request": {
                "data": {
                  "query": "{\n  viewer {\n    login\n  }\n}"
                },
                "api_target": "graphql"
              },
              "contexts": {
                "response": {
                  "type": "response",
                  "data": {
                    "data": {
                      "viewer": {
                        "login": "foo"
                      }
                    }
                  }
                }
              }
            })
            .into(),
        );

        scrub_graphql(data.value_mut().as_mut().unwrap());
        assert_debug_snapshot!(&data);
    }

    #[test]
    fn test_does_not_scrub_if_no_graphql() {
        let mut data = Event::from_value(
            serde_json::json!({
              "request": {
                "data": {
                  "query": "{\n  viewer {\n    login\n  }\n}",
                  "variables": {
                    "login": "foo"
                  }
                },
              },
              "contexts": {
                "response": {
                  "type": "response",
                  "data": {
                    "data": {
                      "viewer": {
                        "login": "foo"
                      }
                    }
                  }
                }
              }
            })
            .into(),
        );

        let scrubbing_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_ip_addresses: true,
            scrub_defaults: true,
            ..Default::default()
        };

        let pii_config = to_pii_config(&scrubbing_config).unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();

        assert_debug_snapshot!(&data);
    }
}
