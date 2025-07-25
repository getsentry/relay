use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::mem;
use std::sync::OnceLock;

use regex::Regex;
use relay_event_schema::processor::{
    self, Chunk, Pii, ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor,
    ValueType, enum_set, process_value,
};
use relay_event_schema::protocol::{
    AsPair, Event, IpAddr, NativeImagePath, PairList, Replay, ResponseContext, User,
};
use relay_protocol::{Annotated, Array, Meta, Remark, RemarkType, Value};

use crate::compiledconfig::{CompiledPiiConfig, RuleRef};
use crate::config::RuleType;
use crate::redactions::Redaction;
use crate::regexes::{self, ANYTHING_REGEX, PatternType, ReplaceBehavior};
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

impl Processor for PiiProcessor<'_> {
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

    fn process_array<T>(
        &mut self,
        array: &mut Array<T>,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        T: ProcessValue,
    {
        if is_pairlist(array) {
            for annotated in array {
                let mut mapped = mem::take(annotated).map_value(T::into_value);

                if let Some(Value::Array(pair)) = mapped.value_mut() {
                    let mut value = mem::take(&mut pair[1]);
                    let value_type = ValueType::for_field(&value);

                    if let Some(key_name) = &pair[0].as_str() {
                        // We enter the key of the first element of the array, since we treat it
                        // as a pair.
                        let key_state =
                            state.enter_borrowed(key_name, state.inner_attrs(), value_type);
                        // We process the value with a state that "simulates" the first value of the
                        // array as if it was the key of a dictionary.
                        process_value(&mut value, self, &key_state)?;
                    }

                    // Put value back into pair.
                    pair[1] = value;
                }

                // Put pair back into array.
                *annotated = T::from_value(mapped);
            }

            Ok(())
        } else {
            // If we didn't find a pairlist, we can process child values as normal.
            array.process_child_values(self, state)
        }
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
        NativeImagePath(value): &mut NativeImagePath,
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

        if let Some(index) = value.rfind(['/', '\\']) {
            let basename = value.split_off(index);
            match self.process_string(value, meta, state) {
                Ok(()) => value.push_str(&basename),
                Err(ProcessingAction::DeleteValueHard) | Err(ProcessingAction::DeleteValueSoft) => {
                    basename[1..].clone_into(value);
                }
                Err(ProcessingAction::InvalidTransaction(x)) => {
                    return Err(ProcessingAction::InvalidTransaction(x));
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
        let ip_was_valid = user.ip_address.value().is_none_or(IpAddr::is_valid);

        // Recurse into the user and does PII processing on fields.
        user.process_child_values(self, state)?;

        let has_other_fields = user.id.value().is_some()
            || user.username.value().is_some()
            || user.email.value().is_some();

        let ip_is_still_valid = user.ip_address.value().is_none_or(IpAddr::is_valid);

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
            user.ip_address.meta_mut().add_remark(Remark::new(
                RemarkType::Removed,
                "pii:ip_address".to_owned(),
            ));
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

#[derive(Default)]
struct PairListProcessor {
    is_pair: bool,
    has_string_key: bool,
}

impl PairListProcessor {
    /// Returns true if the processor identified the supplied data as an array composed of
    /// a key (string) and a value.
    fn is_pair_array(&self) -> bool {
        self.is_pair && self.has_string_key
    }
}

impl Processor for PairListProcessor {
    fn process_array<T>(
        &mut self,
        value: &mut Array<T>,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        T: ProcessValue,
    {
        self.is_pair = state.depth() == 0 && value.len() == 2;
        if self.is_pair {
            let key_type = ValueType::for_field(&value[0]);
            process_value(
                &mut value[0],
                self,
                &state.enter_index(0, state.inner_attrs(), key_type),
            )?;
        }

        Ok(())
    }

    fn process_string(
        &mut self,
        _value: &mut String,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult where {
        if state.depth() == 1 && state.path().index() == Some(0) {
            self.has_string_key = true;
        }

        Ok(())
    }
}

fn is_pairlist<T: ProcessValue>(array: &mut Array<T>) -> bool {
    for element in array.iter_mut() {
        let mut visitor = PairListProcessor::default();
        process_value(element, &mut visitor, ProcessingState::root()).ok();
        if !visitor.is_pair_array() {
            return false;
        }
    }

    !array.is_empty()
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
                        value.set_value(Some(Value::String("[Filtered]".to_owned())));
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
                    value.set_value(Some(Value::String("[Filtered]".to_owned())));
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

        static NULL_SPLIT_RE: OnceLock<Regex> = OnceLock::new();
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
        Redaction::Other => relay_log::debug!("Incoming redaction is not supported"),
    }
}

#[cfg(test)]
mod tests {
    use insta::{allow_duplicates, assert_debug_snapshot};
    use relay_event_schema::processor::process_value;
    use relay_event_schema::protocol::{
        Addr, Breadcrumb, DebugImage, DebugMeta, ExtraValue, Headers, LogEntry, Message,
        NativeDebugImage, Request, Span, TagEntry, Tags, TraceContext,
    };
    use relay_protocol::{FromValue, Object, assert_annotated_snapshot, get_value};
    use serde_json::json;

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
            json!({
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
    fn test_sentry_user() {
        let mut data = Event::from_value(
            json!({
                "user": {
                    "ip_address": "73.133.27.120",
                    "sentry_user": "ip:73.133.27.120",
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
                formatted: Annotated::new("Hello world!".to_owned().into()),
                ..Default::default()
            }),
            request: Annotated::new(Request {
                env: {
                    let mut rv = Object::new();
                    rv.insert(
                        "SECRET_KEY".to_owned(),
                        Annotated::new(Value::String("134141231231231231231312".into())),
                    );
                    Annotated::new(rv)
                },
                headers: {
                    let rv = vec![
                        Annotated::new((
                            Annotated::new("Cookie".to_owned().into()),
                            Annotated::new("super secret".to_owned().into()),
                        )),
                        Annotated::new((
                            Annotated::new("X-Forwarded-For".to_owned().into()),
                            Annotated::new("127.0.0.1".to_owned().into()),
                        )),
                    ];
                    Annotated::new(Headers(PairList(rv)))
                },
                ..Default::default()
            }),
            tags: Annotated::new(Tags(
                vec![Annotated::new(TagEntry(
                    Annotated::new("forwarded_for".to_owned()),
                    Annotated::new("127.0.0.1".to_owned()),
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
                    "foo".to_owned(),
                    Annotated::new(ExtraValue(Value::String("bar".to_owned()))),
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
                    "myvalue".to_owned(),
                    Annotated::new(ExtraValue(Value::String("foobar".to_owned()))),
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
                    "myvalue".to_owned(),
                    Annotated::new(ExtraValue(Value::String("foobar".to_owned()))),
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
                    "myvalue".to_owned(),
                    Annotated::new(ExtraValue(Value::String("foobar".to_owned()))),
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
                    "myvalue".to_owned(),
                    Annotated::new(ExtraValue(Value::String("foobar".to_owned()))),
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
            json!({
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
                        arch: Annotated::new("arm64".to_owned()),
                        image_addr: Annotated::new(Addr(0)),
                        image_size: Annotated::new(4096),
                        image_vmaddr: Annotated::new(Addr(32768)),
                        other: {
                            let mut map = Object::new();
                            map.insert(
                                "other".to_owned(),
                                Annotated::new(Value::String("value".to_owned())),
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
                        arch: Annotated::new("arm64".to_owned()),
                        image_addr: Annotated::new(Addr(0)),
                        image_size: Annotated::new(4096),
                        image_vmaddr: Annotated::new(Addr(32768)),
                        other: {
                            let mut map = Object::new();
                            map.insert(
                                "other".to_owned(),
                                Annotated::new(Value::String("value".to_owned())),
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
                        arch: Annotated::new("arm64".to_owned()),
                        image_addr: Annotated::new(Addr(0)),
                        image_size: Annotated::new(4096),
                        image_vmaddr: Annotated::new(Addr(32768)),
                        other: {
                            let mut map = Object::new();
                            map.insert(
                                "other".to_owned(),
                                Annotated::new(Value::String("value".to_owned())),
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
                        arch: Annotated::new("arm64".to_owned()),
                        image_addr: Annotated::new(Addr(0)),
                        image_size: Annotated::new(4096),
                        image_vmaddr: Annotated::new(Addr(32768)),
                        other: {
                            let mut map = Object::new();
                            map.insert(
                                "other".to_owned(),
                                Annotated::new(Value::String("value".to_owned())),
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
                    "do not ,./<>?!@#$%^&*())'ßtrip'".to_owned(),
                    Annotated::new(ExtraValue(Value::String("foo".to_owned()))),
                );
                map.insert(
                    "special ,./<>?!@#$%^&*())'gärbage'".to_owned(),
                    Annotated::new(ExtraValue(Value::String("bar".to_owned()))),
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
                "##
            ))
            .unwrap();

            let mut event = Annotated::new(Event {
                logentry: Annotated::new(LogEntry {
                    formatted: Annotated::new("Hello world!".to_owned().into()),
                    ..Default::default()
                }),
                ..Default::default()
            });

            let mut processor = PiiProcessor::new(config.compiled());
            process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

            assert!(
                event
                    .value()
                    .unwrap()
                    .logentry
                    .value()
                    .unwrap()
                    .formatted
                    .value()
                    .is_none()
            );
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
                ip_address: Annotated::new(IpAddr("127.0.0.1".to_owned())),
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
                id: Annotated::new("123".to_owned().into()),
                ip_address: Annotated::new(IpAddr("127.0.0.1".to_owned())),
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
    fn test_trace_route_params_scrubbed() {
        let mut trace_context: Annotated<TraceContext> = Annotated::from_json(
            r#"
            {
                "type": "trace",
                "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                "span_id": "fa90fdead5f74052",
                "data": {
                    "previousRoute": {
                        "params": {
                            "password": "test"
                        }
                    }
                }
            }
            "#,
        )
        .unwrap();

        let ds_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };
        let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());

        process_value(
            &mut trace_context,
            &mut pii_processor,
            ProcessingState::root(),
        )
        .unwrap();
        assert_annotated_snapshot!(trace_context);
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
    fn test_span_data_pii() {
        let mut span = Span::from_value(
            json!({
                "data": {
                    "code.filepath": "src/sentry/api/authentication.py",
                }
            })
            .into(),
        );

        let ds_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };
        let pii_config = ds_config.pii_config().unwrap().as_ref().unwrap();

        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        processor::process_value(&mut span, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_eq!(
            get_value!(span.data.code_filepath!).as_str(),
            Some("src/sentry/api/authentication.py")
        );
    }

    #[test]
    fn test_csp_source_file_pii() {
        let mut event = Event::from_value(
            json!({
                "csp": {
                    "source_file": "authentication.js",
                }
            })
            .into(),
        );

        let config = serde_json::from_str::<PiiConfig>(
            r#"
            {
                "applications": {
                    "csp.source_file": ["@anything:filter"]
                }
            }
            "#,
        )
        .unwrap();

        let mut pii_processor = PiiProcessor::new(config.compiled());
        processor::process_value(&mut event, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_eq!(get_value!(event.csp.source_file!).as_str(), "[Filtered]");
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
            json!({
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
            json!({
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
            json!({
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

    #[test]
    fn test_logentry_params_scrubbed() {
        let config = serde_json::from_str::<PiiConfig>(
            r##"
                {
                    "applications": {
                        "$string": ["@anything:remove"]
                    }
                }
                "##,
        )
        .unwrap();

        let mut event = Annotated::new(Event {
            logentry: Annotated::new(LogEntry {
                message: Annotated::new(Message::from("failed to parse report id=%s".to_owned())),
                formatted: Annotated::new("failed to parse report id=1".to_owned().into()),
                params: Annotated::new(Value::Array(vec![Annotated::new(Value::String(
                    "12345".to_owned(),
                ))])),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let params = get_value!(event.logentry.params!);
        assert_debug_snapshot!(params, @r###"
        Array(
            [
                Meta {
                    remarks: [
                        Remark {
                            ty: Removed,
                            rule_id: "@anything:remove",
                            range: None,
                        },
                    ],
                    errors: [],
                    original_length: None,
                    original_value: None,
                },
            ],
        )
        "###);
    }

    #[test]
    fn test_is_pairlist() {
        for (case, expected) in [
            (r#"[]"#, false),
            (r#"["foo"]"#, false),
            (r#"["foo", 123]"#, false),
            (r#"[[1, "foo"]]"#, false),
            (r#"[[["too_nested", 123]]]"#, false),
            (r#"[["foo", "bar"], [1, "foo"]]"#, false),
            (r#"[["foo", "bar"], ["foo", "bar", "baz"]]"#, false),
            (r#"[["foo", "bar", "baz"], ["foo", "bar"]]"#, false),
            (r#"["foo", ["bar", "baz"], ["foo", "bar"]]"#, false),
            (r#"[["foo", "bar"], [["too_nested", 123]]]"#, false),
            (r#"[["foo", 123]]"#, true),
            (r#"[["foo", "bar"]]"#, true),
            (
                r#"[["foo", "bar"], ["foo", {"nested": {"something": 1}}]]"#,
                true,
            ),
        ] {
            let v = Annotated::<Value>::from_json(case).unwrap();
            let Annotated(Some(Value::Array(mut a)), _) = v else {
                panic!()
            };
            assert_eq!(is_pairlist(&mut a), expected, "{case}");
        }
    }

    #[test]
    fn test_tuple_array_scrubbed_with_path_selector() {
        // We expect that both of these configs express the same semantics.
        let configs = vec![
            // This configuration matches on the authorization element (the 1st element of the array
            // represents the key).
            r##"
                {
                    "applications": {
                        "exception.values.0.stacktrace.frames.0.vars.headers.authorization": ["@anything:replace"]
                    }
                }
                "##,
            // This configuration matches on the 2nd element of the array.
            r##"
                {
                    "applications": {
                        "exception.values.0.stacktrace.frames.0.vars.headers.0.1": ["@anything:replace"]
                    }
                }
                "##,
        ];

        let mut event = Event::from_value(
            serde_json::json!(
            {
              "message": "hi",
              "exception": {
                "values": [
                  {
                    "type": "BrokenException",
                    "value": "Something failed",
                    "stacktrace": {
                      "frames": [
                        {
                            "vars": {
                                "headers": [
                                    ["authorization", "Bearer abc123"]
                                ]
                            }
                        }
                      ]
                    }
                  }
                ]
              }
            })
            .into(),
        );

        for config in configs {
            let config = serde_json::from_str::<PiiConfig>(config).unwrap();
            let mut processor = PiiProcessor::new(config.compiled());
            process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

            let vars = get_value!(event.exceptions.values[0].stacktrace.frames[0].vars).unwrap();

            allow_duplicates!(assert_debug_snapshot!(vars, @r###"
                              FrameVars(
                                  {
                                      "headers": Array(
                                          [
                                              Array(
                                                  [
                                                      String(
                                                          "authorization",
                                                      ),
                                                      Annotated(
                                                          String(
                                                              "[Filtered]",
                                                          ),
                                                          Meta {
                                                              remarks: [
                                                                  Remark {
                                                                      ty: Substituted,
                                                                      rule_id: "@anything:replace",
                                                                      range: Some(
                                                                          (
                                                                              0,
                                                                              10,
                                                                          ),
                                                                      ),
                                                                  },
                                                              ],
                                                              errors: [],
                                                              original_length: Some(
                                                                  13,
                                                              ),
                                                              original_value: None,
                                                          },
                                                      ),
                                                  ],
                                              ),
                                          ],
                                      ),
                                  },
                              )
                              "###));
        }
    }

    #[test]
    fn test_tuple_array_scrubbed_with_string_selector_and_password_matcher() {
        let config = serde_json::from_str::<PiiConfig>(
            r##"
                {
                    "applications": {
                        "$string": ["@password:remove"]
                    }
                }
                "##,
        )
        .unwrap();

        let mut event = Event::from_value(
            serde_json::json!(
            {
              "message": "hi",
              "exception": {
                "values": [
                  {
                    "type": "BrokenException",
                    "value": "Something failed",
                    "stacktrace": {
                      "frames": [
                        {
                            "vars": {
                                "headers": [
                                    ["authorization", "abc123"]
                                ]
                            }
                        }
                      ]
                    }
                  }
                ]
              }
            })
            .into(),
        );

        let mut processor = PiiProcessor::new(config.compiled());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let vars = get_value!(event.exceptions.values[0].stacktrace.frames[0].vars).unwrap();

        assert_debug_snapshot!(vars, @r###"
        FrameVars(
            {
                "headers": Array(
                    [
                        Array(
                            [
                                String(
                                    "authorization",
                                ),
                                Meta {
                                    remarks: [
                                        Remark {
                                            ty: Removed,
                                            rule_id: "@password:remove",
                                            range: None,
                                        },
                                    ],
                                    errors: [],
                                    original_length: None,
                                    original_value: None,
                                },
                            ],
                        ),
                    ],
                ),
            },
        )
        "###);
    }
}
