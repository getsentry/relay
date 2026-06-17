use std::net::IpAddr;

use bytes::Bytes;
use smallvec::smallvec;

use relay_dynamic_config::Feature;
use relay_profiling::ProfileType;
use relay_quotas::DataCategory;

use crate::envelope::ContentType;
use crate::envelope::Item;
use crate::managed::Quantities;
use crate::managed::RecordKeeper;
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
    let sdk = utils::client_name_tag(chunks.headers.meta().client_name());
    let client_ip = chunks.headers.meta().client_addr();

    chunks.map(|serialized, records| {
        let mut expanded = Vec::with_capacity(serialized.profile_chunks.len());

        for mut item in serialized.profile_chunks {
            let payload = item.payload();

            let result = match item.content_type() {
                Some(ContentType::PerfettoTrace) => {
                    expand_perfetto_profile_chunk(&item, client_ip, ctx, payload)
                }
                _ => expand_json_item(&mut item, client_ip, ctx, payload, sdk, records),
            };

            match result {
                Ok(chunk) => expanded.push(chunk),
                Err(err) => drop(records.reject_err(err, &item)),
            }
        }

        ExpandedProfileChunks { chunks: expanded }
    })
}

fn expand_perfetto_profile_chunk(
    item: &Item,
    client_ip: Option<IpAddr>,
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

    let filter_settings = &ctx.project_info.config.filter_settings;
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
    item: &mut Item,
    client_ip: Option<IpAddr>,
    ctx: Context<'_>,
    payload: Bytes,
    sdk: &str,
    records: &mut RecordKeeper<'_>,
) -> Result<ExpandedProfileChunk> {
    if item.meta_length().is_some() {
        return Err(relay_profiling::ProfileError::InvalidSampledProfile.into());
    }
    let pc = relay_profiling::ProfileChunk::new(payload)?;
    let profile_type = pc.profile_type();

    // Validate the item inferred profile type with the one from the payload.
    //
    // This is currently necessary to ensure profile chunks are emitted in the correct
    // data category, as well as rate limited with the correct data category.
    //
    // In the future we plan to make the profile type on the item header a necessity.
    // For more context see also: <https://github.com/getsentry/relay/pull/4595>.
    if item.profile_type().is_some_and(|pt| pt != profile_type) {
        return Err(relay_profiling::ProfileError::InvalidProfileType.into());
    }
    // Update the profile type to ensure the following outcomes are emitted in the correct
    // data category.
    //
    // Once the item header on the item is required, this is no longer required.
    if item.profile_type().is_none() {
        relay_statsd::metric!(
            counter(RelayCounters::ProfileChunksWithoutPlatform) += 1,
            sdk = sdk
        );

        item.set_platform(pc.platform().to_owned());
        debug_assert_eq!(item.profile_type(), Some(pc.profile_type()));
        match pc.profile_type() {
            ProfileType::Ui => records.modify_by(DataCategory::ProfileChunkUi, 1),
            ProfileType::Backend => records.modify_by(DataCategory::ProfileChunk, 1),
        }
    }

    let filter_settings = &ctx.project_info.config.filter_settings;
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

    #[test]
    fn test_expand_rejects_broken_chunk_individually() {
        // A valid and a broken chunk share the same envelope. The broken chunk must be rejected
        // on its own while the valid chunk is still expanded and retained.
        let valid = make_json_item(JSON_FIXTURE);

        // `meta_length` is not allowed on JSON chunks, so this chunk fails to expand. The
        // platform header attributes its rejection to the backend profile chunk category.
        let mut broken = make_json_item(JSON_FIXTURE);
        broken.set_meta_length(10);
        broken.set_platform("node".to_owned());

        let (managed, mut handle) = make_chunks(vec![valid, broken]);

        let expanded = expand(managed, Context::for_test());
        let chunks = expanded.accept(|c| c);
        assert_eq!(
            chunks.chunks.len(),
            1,
            "only the valid chunk should be retained"
        );

        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::Profiling(
                "profiling_invalid_sampled_profile",
            )),
            DataCategory::ProfileChunk,
            1,
        );
    }
}
