use std::net::IpAddr;

use relay_dynamic_config::Feature;
use relay_profiling::ProfileType;
use relay_quotas::DataCategory;

use crate::envelope::{ContentType, Item, ItemType};
use crate::processing::Context;
use crate::processing::Managed;
use crate::processing::profile_chunks::{Error, Result, SerializedProfileChunks};
use crate::statsd::RelayCounters;
use crate::utils;

/// Processes profile chunks.
pub fn process(profile_chunks: &mut Managed<SerializedProfileChunks>, ctx: Context<'_>) {
    // Only run this 'expensive' processing step in processing Relays.
    if !ctx.is_processing() {
        return;
    }

    let sdk = utils::client_name_tag(profile_chunks.headers.meta().client_name());
    let client_ip = profile_chunks.headers.meta().client_addr();
    let filter_settings = &ctx.project_info.config.filter_settings;

    profile_chunks.retain(
        |pc| &mut pc.profile_chunks,
        |item, records| -> Result<()> {
            if let Some(meta_length) = item.meta_length() {
                return process_compound_item(
                    item,
                    meta_length,
                    sdk,
                    client_ip,
                    filter_settings,
                    ctx,
                    records,
                );
            }

            let pc = relay_profiling::ProfileChunk::new(item.payload())?;

            // Validate the item inferred profile type with the one from the payload,
            // or if missing set it.
            //
            // This is currently necessary to ensure profile chunks are emitted in the correct
            // data category, as well as rate limited with the correct data category.
            //
            // In the future we plan to make the profile type on the item header a necessity.
            // For more context see also: <https://github.com/getsentry/relay/pull/4595>.
            if item
                .profile_type()
                .is_some_and(|pt| pt != pc.profile_type())
            {
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

            pc.filter(client_ip, filter_settings, ctx.global_config)?;

            let expanded = pc.expand()?;
            if expanded.len() > ctx.config.max_profile_size() {
                return Err(relay_profiling::ProfileError::ExceedSizeLimit.into());
            }

            *item = {
                let mut new_item = Item::new(ItemType::ProfileChunk);
                new_item.set_platform(pc.platform().to_owned());
                new_item.set_payload(ContentType::Json, expanded);
                new_item
            };

            Ok(())
        },
    );
}

/// Processes a compound profile chunk item (JSON metadata + binary blob).
///
/// The item payload is `[JSON metadata bytes][binary blob bytes]`, split at `meta_length`.
/// After expansion, the item is rebuilt with `[expanded JSON][raw binary]` and an updated
/// `meta_length`, so that `forward_store` can still extract the raw profile.
fn process_compound_item(
    item: &mut Item,
    meta_length: u32,
    sdk: &str,
    client_ip: Option<IpAddr>,
    filter_settings: &relay_filter::ProjectFiltersConfig,
    ctx: Context<'_>,
    records: &mut crate::managed::RecordKeeper,
) -> Result<()> {
    let payload = item.payload();
    let meta_length = meta_length as usize;

    let Some((meta_json, raw_profile)) = payload.split_at_checked(meta_length) else {
        return Err(relay_profiling::ProfileError::InvalidSampledProfile.into());
    };

    #[derive(serde::Deserialize)]
    struct ContentTypeProbe {
        content_type: Option<String>,
    }
    match serde_json::from_slice::<ContentTypeProbe>(meta_json)
        .ok()
        .and_then(|v| v.content_type)
        .as_deref()
    {
        Some("perfetto") => {}
        _ => return Err(relay_profiling::ProfileError::PlatformNotSupported.into()),
    }

    if ctx.should_filter(Feature::ContinuousProfilingPerfetto) {
        return Err(Error::FilterFeatureFlag);
    }

    let expanded = relay_profiling::expand_perfetto(raw_profile, meta_json)?;

    if expanded.payload.len() > ctx.config.max_profile_size() {
        return Err(relay_profiling::ProfileError::ExceedSizeLimit.into());
    }

    if item
        .profile_type()
        .is_some_and(|pt| pt != expanded.profile_type())
    {
        return Err(relay_profiling::ProfileError::InvalidProfileType.into());
    }

    if item.profile_type().is_none() {
        relay_statsd::metric!(
            counter(RelayCounters::ProfileChunksWithoutPlatform) += 1,
            sdk = sdk
        );
        item.set_platform(expanded.platform.clone());
        match expanded.profile_type() {
            ProfileType::Ui => records.modify_by(DataCategory::ProfileChunkUi, 1),
            ProfileType::Backend => records.modify_by(DataCategory::ProfileChunk, 1),
        }
    }

    expanded.filter(client_ip, filter_settings, ctx.global_config)?;

    // Rebuild the compound payload: [expanded JSON][raw binary].
    // This preserves the raw profile for downstream extraction in forward_store.
    let platform = expanded.platform;
    let expanded_payload = bytes::Bytes::from(expanded.payload);
    let mut compound = bytes::BytesMut::with_capacity(expanded_payload.len() + raw_profile.len());
    compound.extend_from_slice(&expanded_payload);
    compound.extend_from_slice(raw_profile);

    *item = {
        let mut new_item = Item::new(ItemType::ProfileChunk);
        new_item.set_platform(platform);
        new_item.set_payload(ContentType::Json, compound.freeze());
        new_item.set_meta_length(expanded_payload.len() as u32);
        new_item
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use relay_dynamic_config::{Feature, FeatureSet, ProjectConfig};

    use super::*;
    use crate::Envelope;
    use crate::envelope::ContentType;
    use crate::extractors::RequestMeta;
    use crate::managed::Managed;
    use crate::processing::Context;
    use crate::processing::profile_chunks::SerializedProfileChunks;
    use crate::services::projects::project::ProjectInfo;

    const PERFETTO_FIXTURE: &[u8] = include_bytes!(
        "../../../../relay-profiling/tests/fixtures/android/perfetto/android.pftrace"
    );

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

    fn make_compound_item(meta: &[u8], body: &[u8]) -> Item {
        let meta_length = meta.len() as u32;
        let mut payload = bytes::BytesMut::new();
        payload.extend_from_slice(meta);
        payload.extend_from_slice(body);
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::OctetStream, payload.freeze());
        item.set_meta_length(meta_length);
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

    /// Runs `process_compound_item` for the single item in `managed` and returns the
    /// inner [`SerializedProfileChunks`] after processing, consuming the managed value.
    fn run(managed: &mut Managed<SerializedProfileChunks>, ctx: Context<'_>) {
        let sdk = "";
        let client_ip = None;
        let filter_settings = Default::default();
        managed.retain(
            |pc| &mut pc.profile_chunks,
            |item, records| -> Result<()> {
                let meta_length = item.meta_length().unwrap_or(0);
                process_compound_item(
                    item,
                    meta_length,
                    sdk,
                    client_ip,
                    &filter_settings,
                    ctx,
                    records,
                )
            },
        );
    }

    #[test]
    fn test_process_compound_unknown_content_type() {
        // content_type is not "perfetto" → item is dropped immediately.
        let meta = serde_json::json!({
            "version": "2",
            "chunk_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "profiler_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "platform": "android",
            "content_type": "unknown",
            "client_sdk": {"name": "sentry-android", "version": "1.0"},
        })
        .to_string()
        .into_bytes();
        let item = make_compound_item(&meta, PERFETTO_FIXTURE);
        let (mut managed, _handle) = make_chunks(vec![item]);

        run(&mut managed, Context::for_test());

        let chunks = managed.accept(|c| c);
        assert!(chunks.profile_chunks.is_empty(), "item should be dropped");
    }

    #[test]
    fn test_process_compound_feature_flag_disabled() {
        // The ContinuousProfilingPerfetto feature is absent → item is dropped.
        // Default Context::for_test() uses relay mode = Managed with an empty feature set.
        let meta = perfetto_meta();
        let item = make_compound_item(&meta, PERFETTO_FIXTURE);
        let (mut managed, _handle) = make_chunks(vec![item]);

        run(&mut managed, Context::for_test());

        let chunks = managed.accept(|c| c);
        assert!(
            chunks.profile_chunks.is_empty(),
            "item should be dropped when feature flag is absent"
        );
    }

    #[test]
    fn test_process_compound_meta_length_out_of_bounds() {
        // meta_length header is larger than the actual payload → InvalidSampledProfile.
        let body = b"some bytes";
        let mut item = Item::new(ItemType::ProfileChunk);
        item.set_payload(ContentType::OctetStream, bytes::Bytes::from(body.as_ref()));
        item.set_meta_length(body.len() as u32 + 100);
        let (mut managed, _handle) = make_chunks(vec![item]);

        run(&mut managed, Context::for_test());

        let chunks = managed.accept(|c| c);
        assert!(
            chunks.profile_chunks.is_empty(),
            "item should be dropped on out-of-bounds meta_length"
        );
    }

    #[test]
    fn test_process_compound_success() {
        // Happy path: valid Perfetto trace + feature enabled → compound payload rebuilt.
        let meta = perfetto_meta();
        let item = make_compound_item(&meta, PERFETTO_FIXTURE);
        let (mut managed, _handle) = make_chunks(vec![item]);

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

        run(&mut managed, ctx);

        let mut chunks = managed.accept(|c| c);
        assert_eq!(chunks.profile_chunks.len(), 1, "item should be retained");

        let item = chunks.profile_chunks.remove(0);

        // The rebuilt item must carry a meta_length pointing to the expanded JSON.
        let meta_length = item
            .meta_length()
            .expect("rebuilt item must have meta_length");
        assert!(meta_length > 0);

        // The first meta_length bytes must be valid JSON (the expanded Sample v2 profile).
        let payload = item.payload();
        let (json_part, raw_part) = payload.split_at(meta_length as usize);
        assert!(
            serde_json::from_slice::<serde_json::Value>(json_part).is_ok(),
            "first meta_length bytes must be valid JSON"
        );

        // The raw binary is the original Perfetto trace preserved verbatim.
        assert_eq!(raw_part, PERFETTO_FIXTURE);
    }
}
