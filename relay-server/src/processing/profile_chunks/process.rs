use std::net::IpAddr;

use bytes::Bytes;
use smallvec::smallvec;

use relay_dynamic_config::Feature;
use relay_profiling::ProfileType;
use relay_quotas::DataCategory;

use crate::envelope::ContentType;
use crate::managed::{Quantities, RecordKeeper};
use crate::processing::Context;
use crate::processing::Managed;
use crate::processing::profile_chunks::{
    Error, ExpandedProfileChunk, ExpandedProfileChunks, RawProfile, Result, SerializedProfileChunks,
};
use crate::statsd::RelayCounters;
use crate::utils;

/// Expands serialized profile chunk items into typed representations.
///
/// Each item is individually parsed and validated. Items that fail are
/// removed with outcome tracking.
pub fn expand(
    chunks: Managed<SerializedProfileChunks>,
    ctx: Context<'_>,
) -> Managed<ExpandedProfileChunks> {
    chunks.map(|serialized, records| {
        let sdk = utils::client_name_tag(serialized.headers.meta().client_name());
        let client_ip = serialized.headers.meta().client_addr();
        let filter_settings = &ctx.project_info.config.filter_settings;

        let mut expanded = Vec::with_capacity(serialized.profile_chunks.len());

        for item in serialized.profile_chunks {
            let payload = item.payload();
            let is_perfetto = matches!(item.content_type(), Some(ContentType::PerfettoTrace));

            let result = if is_perfetto {
                expand_perfetto_profile_chunk(&item, client_ip, filter_settings, ctx, payload)
            } else {
                expand_json_item(&item, client_ip, filter_settings, ctx, payload)
            };

            match result {
                Ok(chunk) => {
                    track_quantities(&item, sdk, &chunk.quantities, records);
                    expanded.push(chunk);
                }
                Err(err) => {
                    track_quantities(&item, sdk, &item.quantities(), records);
                    drop(records.reject_err(err, &item));
                }
            }
        }

        ExpandedProfileChunks { chunks: expanded }
    })
}

fn expand_perfetto_profile_chunk(
    item: &crate::envelope::Item,
    client_ip: Option<IpAddr>,
    filter_settings: &relay_filter::ProjectFiltersConfig,
    ctx: Context<'_>,
    payload: Bytes,
) -> Result<ExpandedProfileChunk> {
    if ctx.should_filter(Feature::ContinuousProfilingPerfetto) {
        return Err(Error::FilterFeatureFlag);
    }
    if item.platform().is_none() {
        return Err(relay_profiling::ProfileError::PlatformNotSupported.into());
    }

    let profile_type = item
        .profile_type()
        .ok_or(relay_profiling::ProfileError::InvalidProfileType)?;
    let meta_length =
        item.meta_length()
            .ok_or(relay_profiling::ProfileError::InvalidSampledProfile)? as usize;
    let (json_payload, perfetto_payload) = payload
        .split_at_checked(meta_length)
        .ok_or(relay_profiling::ProfileError::InvalidSampledProfile)?;
    let expanded = relay_profiling::expand_perfetto(perfetto_payload, json_payload)?;
    if expanded.profile_type() != profile_type {
        return Err(relay_profiling::ProfileError::InvalidProfileType.into());
    }
    expanded.filter(client_ip, filter_settings, ctx.global_config)?;
    if expanded.payload.len() > ctx.config.max_profile_size() {
        return Err(relay_profiling::ProfileError::ExceedSizeLimit.into());
    }
    Ok(ExpandedProfileChunk {
        payload: Bytes::from(expanded.payload),
        raw_profile: Some(RawProfile {
            payload: payload.slice_ref(perfetto_payload),
            content_type: ContentType::PerfettoTrace,
        }),
        quantities: quantities_for(profile_type),
    })
}

fn expand_json_item(
    item: &crate::envelope::Item,
    client_ip: Option<IpAddr>,
    filter_settings: &relay_filter::ProjectFiltersConfig,
    ctx: Context<'_>,
    payload: Bytes,
) -> Result<ExpandedProfileChunk> {
    if item.meta_length().is_some() {
        return Err(relay_profiling::ProfileError::InvalidSampledProfile.into());
    }
    let pc = relay_profiling::ProfileChunk::new(payload)?;
    let profile_type = pc.profile_type();
    validate_profile_type(item, profile_type)?;
    pc.filter(client_ip, filter_settings, ctx.global_config)?;
    let expanded = pc.expand()?;
    if expanded.len() > ctx.config.max_profile_size() {
        return Err(relay_profiling::ProfileError::ExceedSizeLimit.into());
    }
    Ok(ExpandedProfileChunk {
        payload: Bytes::from(expanded),
        raw_profile: None,
        quantities: quantities_for(profile_type),
    })
}

fn validate_profile_type(item: &crate::envelope::Item, profile_type: ProfileType) -> Result<()> {
    if item.profile_type().is_some_and(|pt| pt != profile_type) {
        return Err(relay_profiling::ProfileError::InvalidProfileType.into());
    }
    Ok(())
}

fn track_quantities(
    item: &crate::envelope::Item,
    sdk: &str,
    quantities: &Quantities,
    records: &mut RecordKeeper<'_>,
) {
    if item.profile_type().is_none() {
        relay_statsd::metric!(
            counter(RelayCounters::ProfileChunksWithoutPlatform) += 1,
            sdk = sdk
        );
        for &(category, quantity) in quantities {
            records.modify_by(category, quantity as isize);
        }
    }
}

fn quantities_for(profile_type: ProfileType) -> Quantities {
    match profile_type {
        ProfileType::Ui => smallvec![(DataCategory::ProfileChunkUi, 1)],
        ProfileType::Backend => smallvec![(DataCategory::ProfileChunk, 1)],
    }
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use relay_dynamic_config::{Feature, FeatureSet, ProjectConfig};

    use super::*;
    use crate::Envelope;
    use crate::envelope::{ContentType, Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::processing::profile_chunks::SerializedProfileChunks;
    use crate::services::outcome::{DiscardReason, Outcome};
    use crate::services::projects::project::ProjectInfo;

    const PERFETTO_FIXTURE: &[u8] = include_bytes!(
        "../../../../relay-profiling/tests/fixtures/android/perfetto/android.pftrace"
    );

    const JSON_FIXTURE: &[u8] =
        include_bytes!("../../../../relay-profiling/tests/fixtures/sample/v2/valid.json");

    fn perfetto_meta() -> Vec<u8> {
        serde_json::json!({
            "version": "2",
            "chunk_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "profiler_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "platform": "android",
            "content_type": "perfetto",
            "client_sdk": {"name": "sentry-android", "version": "1.0"},
        })
        .to_string()
        .into_bytes()
    }

    fn make_compound_item(meta: &[u8], body: &[u8], platform: &str) -> Item {
        let meta_length = meta.len() as u32;
        let mut payload = bytes::BytesMut::new();
        payload.extend_from_slice(meta);
        payload.extend_from_slice(body);
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::PerfettoTrace, payload.freeze());
        item.set_meta_length(meta_length);
        item.set_platform(platform.to_owned());
        item
    }

    fn make_chunks(
        items: Vec<Item>,
    ) -> (
        Managed<SerializedProfileChunks>,
        crate::managed::ManagedTestHandle,
    ) {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let envelope = Envelope::from_request(None, RequestMeta::new(dsn));
        let headers = envelope.headers().clone();
        Managed::for_test(SerializedProfileChunks {
            headers,
            profile_chunks: items,
        })
        .build()
    }

    #[test]
    fn test_expand_compound_unknown_content_type() {
        let meta = perfetto_meta();
        let meta_length = meta.len() as u32;
        let mut payload = bytes::BytesMut::new();
        payload.extend_from_slice(&meta);
        payload.extend_from_slice(PERFETTO_FIXTURE);
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::OctetStream, payload.freeze());
        item.set_meta_length(meta_length);
        let (managed, _handle) = make_chunks(vec![item]);

        let expanded = expand(managed, Context::for_test());
        let chunks = expanded.accept(|c| c);
        assert!(chunks.chunks.is_empty(), "item should be dropped");
    }

    #[test]
    fn test_expand_compound_feature_flag_disabled() {
        let meta = perfetto_meta();
        let item = make_compound_item(&meta, PERFETTO_FIXTURE, "android");
        let (managed, _handle) = make_chunks(vec![item]);

        let expanded = expand(managed, Context::for_test());
        let chunks = expanded.accept(|c| c);
        assert!(
            chunks.chunks.is_empty(),
            "item should be dropped when feature flag is absent"
        );
    }

    #[test]
    fn test_expand_compound_meta_length_out_of_bounds() {
        let body = b"some bytes";
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(
            ContentType::PerfettoTrace,
            bytes::Bytes::from(body.as_ref()),
        );
        item.set_meta_length(body.len() as u32 + 100);
        item.set_platform("android".to_owned());
        let (managed, mut handle) = make_chunks(vec![item]);

        let ctx = Context {
            project_info: &ProjectInfo {
                config: ProjectConfig {
                    features: FeatureSet::from_iter([
                        Feature::ContinuousProfiling,
                        Feature::ContinuousProfilingPerfetto,
                    ]),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };

        let expanded = expand(managed, ctx);
        let chunks = expanded.accept(|c| c);
        assert!(
            chunks.chunks.is_empty(),
            "item should be dropped on out-of-bounds meta_length"
        );
        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::Profiling(
                "profiling_invalid_sampled_profile",
            )),
            DataCategory::ProfileChunkUi,
            1,
        );
    }

    #[test]
    fn test_expand_compound_missing_platform() {
        let meta = perfetto_meta();
        let meta_length = meta.len() as u32;
        let mut payload = bytes::BytesMut::new();
        payload.extend_from_slice(&meta);
        payload.extend_from_slice(PERFETTO_FIXTURE);
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::PerfettoTrace, payload.freeze());
        item.set_meta_length(meta_length);
        let (managed, _handle) = make_chunks(vec![item]);

        let ctx = Context {
            project_info: &ProjectInfo {
                config: ProjectConfig {
                    features: FeatureSet::from_iter([
                        Feature::ContinuousProfiling,
                        Feature::ContinuousProfilingPerfetto,
                    ]),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };

        let expanded = expand(managed, ctx);
        let chunks = expanded.accept(|c| c);
        assert!(
            chunks.chunks.is_empty(),
            "perfetto item without platform header should be rejected"
        );
    }

    #[test]
    fn test_expand_compound_success() {
        let meta = perfetto_meta();
        let item = make_compound_item(&meta, PERFETTO_FIXTURE, "android");
        let (managed, _handle) = make_chunks(vec![item]);

        let ctx = Context {
            project_info: &ProjectInfo {
                config: ProjectConfig {
                    features: FeatureSet::from_iter([
                        Feature::ContinuousProfiling,
                        Feature::ContinuousProfilingPerfetto,
                    ]),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };

        let expanded = expand(managed, ctx);
        let chunks = expanded.accept(|c| c);
        assert_eq!(chunks.chunks.len(), 1, "item should be retained");

        let chunk = &chunks.chunks[0];
        assert!(
            serde_json::from_slice::<serde_json::Value>(&chunk.payload).is_ok(),
            "payload must be valid JSON"
        );
        let raw_profile = chunk.raw_profile.as_ref().expect("expected raw_profile");
        assert_eq!(raw_profile.payload.as_ref(), PERFETTO_FIXTURE);
        assert_eq!(raw_profile.content_type, ContentType::PerfettoTrace);
    }

    fn make_json_item(payload: &[u8]) -> Item {
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::Json, bytes::Bytes::from(payload.to_vec()));
        item
    }

    #[test]
    fn test_expand_json_success() {
        let item = make_json_item(JSON_FIXTURE);
        let (managed, _handle) = make_chunks(vec![item]);

        let expanded = expand(managed, Context::for_test());
        let chunks = expanded.accept(|c| c);
        assert_eq!(chunks.chunks.len(), 1, "item should be retained");

        let chunk = &chunks.chunks[0];
        assert!(
            serde_json::from_slice::<serde_json::Value>(&chunk.payload).is_ok(),
            "payload must be valid JSON"
        );
        assert!(
            chunk.raw_profile.is_none(),
            "JSON items should not have raw_profile"
        );
    }

    #[test]
    fn test_expand_json_with_meta_length_rejected() {
        let mut item = make_json_item(JSON_FIXTURE);
        item.set_meta_length(10);
        let (managed, _handle) = make_chunks(vec![item]);

        let expanded = expand(managed, Context::for_test());
        let chunks = expanded.accept(|c| c);
        assert!(
            chunks.chunks.is_empty(),
            "JSON item with meta_length should be rejected"
        );
    }

    #[test]
    fn test_expand_json_mismatched_profile_type() {
        let mut item = make_json_item(JSON_FIXTURE);
        // fixture has platform "cocoa" → ProfileType::Ui,
        // but "node" → ProfileType::Backend, creating a mismatch
        item.set_platform("node".to_owned());
        let (managed, mut handle) = make_chunks(vec![item]);

        let expanded = expand(managed, Context::for_test());
        let chunks = expanded.accept(|c| c);
        assert!(
            chunks.chunks.is_empty(),
            "JSON item with mismatched profile_type header should be rejected"
        );
        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::Profiling("profiling_invalid_profile_type")),
            DataCategory::ProfileChunk,
            1,
        );
    }
}
