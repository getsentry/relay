var srcIndex = new Map(JSON.parse('[["bench_buffer",["",[],["main.rs"]]],["document_metrics",["",[],["main.rs"]]],["document_pii",["",[],["item_collector.rs","main.rs","pii_finder.rs"]]],["process_event",["",[],["main.rs"]]],["relay",["",[],["cli.rs","cliapp.rs","main.rs","setup.rs","utils.rs"]]],["relay_auth",["",[],["lib.rs"]]],["relay_base_schema",["",[["metrics",[],["mod.rs","mri.rs","name.rs","units.rs"]]],["data_category.rs","events.rs","lib.rs","organization.rs","project.rs","spans.rs"]]],["relay_cabi",["",[],["auth.rs","codeowners.rs","constants.rs","core.rs","ffi.rs","glob.rs","lib.rs","processing.rs"]]],["relay_cardinality",["",[["redis",[],["cache.rs","limiter.rs","mod.rs","quota.rs","script.rs","state.rs"]]],["config.rs","error.rs","lib.rs","limiter.rs","statsd.rs","window.rs"]]],["relay_cogs",["",[],["cogs.rs","lib.rs","recorder.rs"]]],["relay_common",["",[],["glob2.rs","lib.rs","macros.rs","time.rs"]]],["relay_config",["",[],["aggregator.rs","byte_size.rs","config.rs","lib.rs","redis.rs","upstream.rs"]]],["relay_crash",["",[],["lib.rs"]]],["relay_dynamic_config",["",[],["defaults.rs","error_boundary.rs","feature.rs","global.rs","lib.rs","metrics.rs","project.rs","utils.rs"]]],["relay_event_derive",["",[],["lib.rs"]]],["relay_event_normalization",["",[["normalize",[["span",[["description",[["sql",[],["mod.rs","parser.rs"]]],["mod.rs","redis.rs","resource.rs"]]],["ai.rs","country_subregion.rs","exclusive_time.rs","mod.rs","tag_extraction.rs"]]],["breakdowns.rs","contexts.rs","mod.rs","nel.rs","request.rs","user_agent.rs","utils.rs"]],["transactions",[],["mod.rs","processor.rs","rules.rs"]]],["clock_drift.rs","event.rs","event_error.rs","geo.rs","legacy.rs","lib.rs","logentry.rs","mechanism.rs","regexes.rs","remove_other.rs","replay.rs","schema.rs","stacktrace.rs","statsd.rs","timestamp.rs","trimming.rs","validation.rs"]]],["relay_event_schema",["",[["processor",[],["attrs.rs","chunks.rs","funcs.rs","impls.rs","mod.rs","traits.rs"]],["protocol",[["contexts",[],["app.rs","browser.rs","cloud_resource.rs","device.rs","gpu.rs","mod.rs","monitor.rs","nel.rs","os.rs","otel.rs","performance_score.rs","profile.rs","replay.rs","reprocessing.rs","response.rs","runtime.rs","trace.rs","user_report_v2.rs"]],["span",[],["convert.rs"]]],["base.rs","breadcrumb.rs","breakdowns.rs","client_report.rs","clientsdk.rs","constants.rs","debugmeta.rs","device_class.rs","event.rs","exception.rs","fingerprint.rs","logentry.rs","measurements.rs","mechanism.rs","metrics.rs","metrics_summary.rs","mod.rs","nel.rs","relay_info.rs","replay.rs","request.rs","security_report.rs","session.rs","span.rs","stacktrace.rs","tags.rs","templateinfo.rs","thread.rs","transaction.rs","types.rs","user.rs","user_report.rs","utils.rs"]]],["lib.rs"]]],["relay_ffi",["",[],["lib.rs"]]],["relay_ffi_macros",["",[],["lib.rs"]]],["relay_filter",["",[],["browser_extensions.rs","client_ips.rs","common.rs","config.rs","csp.rs","error_messages.rs","generic.rs","interface.rs","legacy_browsers.rs","lib.rs","localhost.rs","releases.rs","transaction_name.rs","web_crawlers.rs"]]],["relay_kafka",["",[["producer",[],["mod.rs","schemas.rs","utils.rs"]]],["config.rs","lib.rs","statsd.rs"]]],["relay_log",["",[],["lib.rs","setup.rs","test.rs","utils.rs"]]],["relay_metrics",["",[],["aggregator.rs","bucket.rs","cogs.rs","finite.rs","lib.rs","protocol.rs","statsd.rs","view.rs"]]],["relay_monitors",["",[],["lib.rs"]]],["relay_pattern",["",[],["lib.rs","typed.rs","wildmatch.rs"]]],["relay_pii",["",[],["attachments.rs","builtin.rs","compiledconfig.rs","config.rs","convert.rs","generate_selectors.rs","legacy.rs","lib.rs","minidumps.rs","processor.rs","redactions.rs","regexes.rs","selector.rs","utils.rs"]]],["relay_profiling",["",[["android",[],["chunk.rs","legacy.rs","mod.rs"]],["sample",[],["mod.rs","v1.rs","v2.rs"]]],["error.rs","extract_from_transaction.rs","lib.rs","measurements.rs","native_debug_image.rs","outcomes.rs","transaction_metadata.rs","types.rs","utils.rs"]]],["relay_protocol",["",[],["annotated.rs","condition.rs","impls.rs","lib.rs","macros.rs","meta.rs","size.rs","traits.rs","value.rs"]]],["relay_protocol_derive",["",[],["lib.rs"]]],["relay_quotas",["",[],["global.rs","lib.rs","quota.rs","rate_limit.rs","redis.rs"]]],["relay_redis",["",[],["config.rs","lib.rs","real.rs","scripts.rs"]]],["relay_replays",["",[],["lib.rs","recording.rs","transform.rs"]]],["relay_sampling",["",[],["config.rs","dsc.rs","evaluation.rs","lib.rs","redis_sampling.rs"]]],["relay_server",["",[["endpoints",[],["attachments.rs","batch_metrics.rs","batch_outcomes.rs","common.rs","envelope.rs","events.rs","forward.rs","health_check.rs","minidump.rs","mod.rs","monitor.rs","nel.rs","project_configs.rs","public_keys.rs","security_report.rs","statics.rs","store.rs","unreal.rs"]],["extractors",[],["content_type.rs","forwarded_for.rs","mime.rs","mod.rs","received_at.rs","remote.rs","request_meta.rs","signed_json.rs"]],["metrics",[],["bucket_encoding.rs","metric_stats.rs","minimal.rs","mod.rs","outcomes.rs","rate_limits.rs"]],["metrics_extraction",[["sessions",[],["mod.rs","types.rs"]],["transactions",[],["mod.rs","types.rs"]]],["event.rs","generic.rs","metrics_summary.rs","mod.rs"]],["middlewares",[],["body_timing.rs","cors.rs","decompression.rs","handle_panic.rs","metrics.rs","mod.rs","normalize_path.rs","sentry_tower.rs","trace.rs"]],["services",[["buffer",[["envelope_buffer",[],["mod.rs"]],["envelope_stack",[],["caching.rs","memory.rs","mod.rs","sqlite.rs"]],["envelope_store",[],["mod.rs","sqlite.rs"]],["stack_provider",[],["memory.rs","mod.rs","sqlite.rs"]]],["common.rs","mod.rs","testutils.rs"]],["metrics",[],["aggregator.rs","mod.rs","router.rs"]],["processor",[["span",[],["processing.rs"]]],["attachment.rs","dynamic_sampling.rs","event.rs","metrics.rs","profile.rs","profile_chunk.rs","replay.rs","report.rs","session.rs","span.rs","unreal.rs"]],["projects",[["cache",[],["handle.rs","legacy.rs","mod.rs","project.rs","service.rs","state.rs"]],["project",[],["info.rs","mod.rs"]],["source",[],["local.rs","mod.rs","redis.rs","upstream.rs"]]],["mod.rs"]],["server",[],["acceptor.rs","io.rs","mod.rs"]],["spooler",[],["mod.rs","spool_utils.rs","sql.rs"]]],["cogs.rs","global_config.rs","health_check.rs","mod.rs","outcome.rs","outcome_aggregator.rs","processor.rs","relays.rs","stats.rs","store.rs","test_store.rs","upstream.rs"]],["utils",[["scheduled",[],["futures.rs","mod.rs","queue.rs"]]],["api.rs","dynamic_sampling.rs","managed_envelope.rs","memory.rs","mod.rs","multipart.rs","native.rs","param_parser.rs","pick.rs","rate_limits.rs","retry.rs","serde.rs","sizes.rs","sleep_handle.rs","split_off.rs","statsd.rs","thread_pool.rs","unreal.rs"]]],["constants.rs","envelope.rs","http.rs","lib.rs","service.rs","statsd.rs"]]],["relay_spans",["",[],["lib.rs","span.rs","status_codes.rs"]]],["relay_statsd",["",[],["lib.rs"]]],["relay_system",["",[],["controller.rs","lib.rs","runtime.rs","service.rs","statsd.rs"]]],["relay_test",["",[],["lib.rs"]]],["relay_ua",["",[],["lib.rs"]]],["scrub_minidump",["",[],["main.rs"]]]]'));
createSrcSidebar();
//{"start":36,"fragment_lengths":[36,41,73,38,73,34,169,120,182,58,69,104,35,140,42,615,897,33,40,247,109,65,123,38,63,228,273,134,45,86,70,67,95,2276,63,36,90,34,32,39]}