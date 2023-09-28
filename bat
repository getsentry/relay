cargo test --workspace --all-features

running 1 test
test tests::test_parse_metrics ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s


running 7 tests
test tests::test_find_rs_files ... ok
test tests::test_single_type ... ok
test tests::test_pii_false ... ok
test tests::test_scoped_paths ... ok
test tests::test_pii_true ... ok
test tests::test_pii_all ... ok
test tests::test_pii_retain_additional_properties_truth_table ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 12 tests
test tests::test_deserialize_old_response ... ok
test tests::test_relay_version_any_supported ... ok
test tests::test_relay_version_current ... ok
test tests::test_relay_version_from_str ... ok
test tests::test_relay_version_oldest_supported ... ok
test tests::test_relay_version_oldest ... ok
test tests::test_relay_version_parse ... ok
test tests::test_keys ... ok
test tests::test_serializing ... ok
test tests::test_signatures ... ok
test tests::test_generate_strings_for_test_auth_py ... ok
test tests::test_registration ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test metrics::tests::test_custom_unit_parse ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 3 tests
test processing::pii_config_validation_invalid_regex ... ok
test processing::pii_config_validation_valid_regex ... ok
test codeowners::tests::test_translate_codeowners_pattern ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 25 tests
test glob3::tests::test_match_negative ... ok
test glob3::tests::test_match_neg_unsupported ... ok
test glob3::tests::test_match_literal ... ok
test glob3::tests::test_match_newline ... ok
test glob3::tests::test_match_newline_inner ... ok
test glob3::tests::test_match_inner ... ok
test glob3::tests::test_match_prefix ... ok
test glob3::tests::test_match_newline_pattern ... ok
test glob3::tests::test_match_utf8 ... ok
test glob3::tests::test_match_range ... ok
test time::tests::test_parse_datetime_bogus ... ok
test glob3::tests::test_match_suffix ... ok
test glob3::tests::test_match_range_neg ... ok
test time::tests::test_parse_timestamp_float ... ok
test time::tests::test_parse_timestamp_int ... ok
test time::tests::test_parse_timestamp_large_float ... ok
test glob2::tests::test_do_not_replace ... ok
test time::tests::test_parse_timestamp_neg_float ... ok
test time::tests::test_parse_timestamp_neg_int ... ok
test time::tests::test_parse_timestamp_other ... ok
test time::tests::test_parse_timestamp_str ... ok
test glob2::tests::test_glob_matcher ... ok
test glob2::tests::test_glob_replace ... ok
test glob2::tests::test_glob ... ok
test glob::tests::test_globs ... ok

test result: ok. 25 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s


running 10 tests
test byte_size::tests::test_as_bytes ... ok
test byte_size::tests::test_infer ... ok
test config::tests::test_emit_outcomes_invalid ... ok
test byte_size::tests::test_parse ... ok
test byte_size::tests::test_serde_number ... ok
test config::tests::test_emit_outcomes ... ok
test byte_size::tests::test_serde_string ... ok
test upstream::test::test_from_dsn ... ok
test upstream::test::test_basic_parsing ... ok
test config::tests::test_event_buffer_size ... ok

test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 39 tests
test native::bindgen_test_layout___darwin_arm_cpmu_state64 ... ok
test native::bindgen_test_layout___arm_legacy_debug_state ... ok
test native::bindgen_test_layout___darwin_arm_debug_state32 ... ok
test native::bindgen_test_layout___darwin_arm_debug_state64 ... ok
test native::bindgen_test_layout___arm_pagein_state ... ok
test native::bindgen_test_layout___darwin_arm_exception_state64 ... ok
test native::bindgen_test_layout___darwin_arm_exception_state ... ok
test native::bindgen_test_layout___darwin_arm_neon_state ... ok
test native::bindgen_test_layout___darwin_arm_neon_state64 ... ok
test native::bindgen_test_layout___darwin_arm_thread_state ... ok
test native::bindgen_test_layout___darwin_arm_thread_state64 ... ok
test native::bindgen_test_layout___darwin_arm_vfp_state ... ok
test native::bindgen_test_layout___darwin_mcontext32 ... ok
test native::bindgen_test_layout___darwin_mcontext64 ... ok
test native::bindgen_test_layout___darwin_pthread_handler_rec ... ok
test native::bindgen_test_layout___darwin_sigaltstack ... ok
test native::bindgen_test_layout___darwin_ucontext ... ok
test native::bindgen_test_layout___mbstate_t ... ok
test native::bindgen_test_layout___sigaction ... ok
test native::bindgen_test_layout___sigaction_u ... ok
test native::bindgen_test_layout___siginfo ... ok
test native::bindgen_test_layout__opaque_pthread_attr_t ... ok
test native::bindgen_test_layout__opaque_pthread_cond_t ... ok
test native::bindgen_test_layout__opaque_pthread_condattr_t ... ok
test native::bindgen_test_layout__opaque_pthread_mutex_t ... ok
test native::bindgen_test_layout__opaque_pthread_mutexattr_t ... ok
test native::bindgen_test_layout__opaque_pthread_once_t ... ok
test native::bindgen_test_layout__opaque_pthread_rwlock_t ... ok
test native::bindgen_test_layout__opaque_pthread_t ... ok
test native::bindgen_test_layout__opaque_pthread_rwlockattr_t ... ok
test native::bindgen_test_layout_sentry_ucontext_s ... ok
test native::bindgen_test_layout_imaxdiv_t ... ok
test native::bindgen_test_layout_sentry_uuid_s ... ok
test native::bindgen_test_layout_sentry_value_u ... ok
test native::bindgen_test_layout_sigaction ... ok
test native::bindgen_test_layout_sigevent ... ok
test native::bindgen_test_layout_sigvec ... ok
test native::bindgen_test_layout_sigval ... ok
test native::bindgen_test_layout_sigstack ... ok

test result: ok. 39 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test stats::tests::parses_metric ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 12 tests
test error_boundary::tests::test_deserialize_ok ... ok
test error_boundary::tests::test_deserialize_err ... ok
test error_boundary::tests::test_deserialize_syntax_err ... ok
test error_boundary::tests::test_serialize_err ... ok
test error_boundary::tests::test_serialize_ok ... ok
test feature::tests::roundtrip ... ok
test metrics::tests::parse_tag_spec_field ... ok
test metrics::tests::parse_tag_spec_unsupported ... ok
test global::tests::test_global_config_roundtrip ... ok
test metrics::tests::parse_tag_spec_value ... ok
test utils::tests::test_validate_json ... ok
test metrics::tests::parse_tag_mapping ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 423 tests
test clock_drift::tests::test_clock_drift_unix ... ok
test clock_drift::tests::test_process_datetime ... ok
test clock_drift::tests::test_clock_drift_lower_bound ... ok
test clock_drift::tests::test_no_sent_at ... ok
test event_error::tests::test_no_errors ... ok
test event_error::tests::test_nested_errors ... ok
test event_error::tests::test_original_value ... ok
test event_error::tests::test_errors_in_other ... ok
test event_error::tests::test_multiple_errors ... ok
test clock_drift::tests::test_no_clock_drift ... ok
test clock_drift::tests::test_clock_drift_from_past ... ok
test clock_drift::tests::test_clock_drift_from_future ... ok
test event_error::tests::test_top_level_errors ... ok
test normalize::breakdowns::tests::test_noop_breakdowns_with_empty_config ... ok
test normalize::breakdowns::tests::test_skip_with_empty_breakdowns_config ... ok
test normalize::breakdowns::tests::test_emit_ops_breakdown ... ok
test normalize::contexts::tests::test_broken_json_with_fallback ... ok
test normalize::contexts::tests::test_broken_json_without_fallback ... ok
test normalize::contexts::tests::test_infer_json ... ok
test normalize::contexts::tests::test_dotnet_core ... ok
test normalize::contexts::tests::test_dotnet_framework_48_without_build_id ... ok
test normalize::contexts::tests::test_dotnet_framework_472 ... ok
test normalize::contexts::tests::test_dotnet_framework_future_version ... ok
test normalize::contexts::tests::test_dotnet_native ... ok
test normalize::contexts::tests::test_macos_with_build ... ok
test normalize::contexts::tests::test_macos_without_build ... ok
test normalize::contexts::tests::test_name_not_overwritten ... ok
test normalize::contexts::tests::test_no_name ... ok
test normalize::contexts::tests::test_unity_android_api_version ... ok
test normalize::contexts::tests::test_unity_mac_os ... ok
test normalize::contexts::tests::test_linux_5_11 ... ok
test normalize::contexts::tests::test_unity_windows_os ... ok
test normalize::contexts::tests::test_version_not_overwritten ... ok
test normalize::contexts::tests::test_windows_10 ... ok
test normalize::contexts::tests::test_windows_11 ... ok
test normalize::contexts::tests::test_windows_11_future1 ... ok
test normalize::contexts::tests::test_windows_11_future2 ... ok
test normalize::contexts::tests::test_windows_7_or_server_2008 ... ok
test normalize::contexts::tests::test_windows_8_or_server_2012_or_later ... ok
test normalize::logentry::tests::test_empty_logentry ... ok
test normalize::logentry::tests::test_empty_missing_message ... ok
test normalize::contexts::tests::test_wsl_ubuntu ... ok
test normalize::contexts::tests::test_centos_runtime_info ... ok
test normalize::contexts::tests::test_unity_android_os ... ok
test normalize::contexts::tests::test_android_4_4_2 ... ok
test normalize::contexts::tests::test_centos_os_version ... ok
test normalize::contexts::tests::test_macos_runtime ... ok
test normalize::contexts::tests::test_ios_15_0 ... ok
test normalize::contexts::tests::test_macos_os_version ... ok
test normalize::logentry::tests::test_format_no_params ... ok
test normalize::logentry::tests::test_message_formatted_equal ... ok
test normalize::logentry::tests::test_only_message ... ok
test normalize::mechanism::tests::test_normalize_errno ... ok
test normalize::mechanism::tests::test_normalize_errno_fail ... ok
test normalize::mechanism::tests::test_normalize_errno_override ... ok
test normalize::mechanism::tests::test_normalize_mach_fail ... ok
test normalize::mechanism::tests::test_normalize_mach ... ok
test normalize::mechanism::tests::test_normalize_missing ... ok
test normalize::mechanism::tests::test_normalize_mach_override ... ok
test normalize::mechanism::tests::test_normalize_partial_signal ... ok
test normalize::mechanism::tests::test_normalize_signal ... ok
test normalize::mechanism::tests::test_normalize_signal_fail ... ok
test normalize::mechanism::tests::test_normalize_signal_override ... ok
test normalize::request::tests::test_broken_json_with_fallback ... ok
test normalize::request::tests::test_broken_json_without_fallback ... ok
test normalize::request::tests::test_cookies_in_header ... ok
test normalize::request::tests::test_cookies_in_header_dont_override_cookies ... ok
test normalize::request::tests::test_infer_binary ... ok
test normalize::request::tests::test_infer_json ... ok
test normalize::request::tests::test_infer_url_encoded_base64 ... ok
test normalize::request::tests::test_infer_url_encoded ... ok
test normalize::request::tests::test_infer_xml ... ok
test normalize::request::tests::test_infer_url_false_positive ... ok
test normalize::request::tests::test_query_string_empty_value ... ok
test normalize::request::tests::test_url_only_path ... ok
test normalize::request::tests::test_url_precedence ... ok
test normalize::request::tests::test_method_invalid ... ok
test normalize::request::tests::test_url_punycoded ... ok
test normalize::request::tests::test_method_valid ... ok
test normalize::request::tests::test_url_truncation ... ok
test normalize::request::tests::test_url_truncation_reversed ... ok
test normalize::request::tests::test_url_with_ellipsis ... ok
test normalize::request::tests::test_url_with_qs_and_fragment ... ok
test normalize::span::attributes::tests::test_child_spans_consumes_all_of_parent ... ok
test normalize::span::attributes::tests::test_child_spans_dont_intersect_parent ... ok
test normalize::span::attributes::tests::test_child_spans_extend_beyond_parent ... ok
test normalize::span::attributes::tests::test_childless_spans ... ok
test normalize::span::attributes::tests::test_nested_spans ... ok
test normalize::logentry::tests::test_format_python ... ok
test normalize::span::attributes::tests::test_only_immediate_child_spans_affect_calculation ... ok
test normalize::span::attributes::tests::test_overlapping_child_spans ... ok
test normalize::span::attributes::tests::test_skip_exclusive_time ... ok
test normalize::span::description::sql::tests::activerecord ... ok
test normalize::logentry::tests::test_format_python_named ... ok
test normalize::span::description::sql::parser::tests::parse_deep_expression ... ok
test normalize::span::description::sql::tests::boolean_not_in_mid_tablename_true ... ok
test normalize::span::description::sql::tests::already_scrubbed ... ok
test normalize::span::description::sql::tests::boolean_not_in_tablename_true ... ok
test normalize::span::description::sql::tests::boolean_not_in_tablename_false ... ok
test normalize::span::description::sql::tests::boolean_not_in_mid_tablename_false ... ok
test normalize::logentry::tests::test_format_java ... ok
test normalize::logentry::tests::test_format_dotnet ... ok
test normalize::span::description::sql::tests::bytesa ... ok
test normalize::span::description::sql::tests::boolean_where_false ... ok
test normalize::span::description::sql::tests::case_when ... ok
test normalize::span::description::sql::tests::boolean_where_true ... ok
test normalize::span::description::sql::tests::case_when_nested ... ok
test normalize::span::description::sql::tests::boolean_where_bool_insensitive ... ok
test normalize::span::description::sql::tests::close_cursor ... ok
test normalize::span::description::sql::tests::collapse_columns_distinct ... ok
test normalize::span::description::sql::tests::collapse_columns_nested ... ok
test normalize::span::description::sql::tests::collapse_columns ... ok
test normalize::span::description::sql::tests::collapse_columns_with_as ... ok
test normalize::span::description::sql::tests::collapse_partial_column_lists ... ok
test normalize::span::description::sql::tests::collapse_columns_with_as_without_quotes ... ok
test normalize::span::description::sql::tests::declare_cursor ... ok
test normalize::span::description::sql::tests::declare_cursor_advanced ... ok
test normalize::span::description::sql::tests::collapse_partial_column_lists_2 ... ok
test normalize::span::description::sql::tests::do_not_collapse_single_column ... ok
test normalize::span::description::sql::tests::digits_in_compound_table_name ... ok
test normalize::span::description::sql::tests::dont_scrub_double_quoted_strings_format_mysql ... ok
test normalize::span::description::sql::tests::dont_scrub_double_quoted_strings_format_postgres ... ok
test normalize::span::description::sql::tests::digits_in_table_name ... ok
test normalize::span::description::sql::tests::fetch_cursor ... ok
test normalize::span::description::sql::tests::dont_scrub_nulls ... ok
test normalize::span::description::sql::tests::jsonb ... ok
test normalize::span::description::sql::tests::not_a_comment ... ok
test normalize::span::description::sql::tests::multiple_statements ... ok
test normalize::span::description::sql::tests::mixed ... ok
test normalize::span::description::sql::tests::num_e_where ... ok
test normalize::span::description::sql::tests::num_negative_where ... ok
test normalize::span::description::sql::tests::num_limit ... ok
test normalize::span::description::sql::tests::parameters_values ... ok
test normalize::span::description::sql::tests::num_where ... ok
test normalize::span::description::sql::tests::parameters_in ... ok
test normalize::span::description::sql::tests::php_placeholders ... ok
test normalize::span::description::sql::tests::parameters_values_with_quotes ... ok
test normalize::span::description::sql::tests::quotes_in_cast ... ok
test normalize::span::description::sql::tests::qualified_wildcard ... ok
test normalize::span::description::sql::tests::quotes_in_function ... ok
test normalize::span::description::sql::tests::savepoint_lowercase ... ok
test normalize::span::description::sql::tests::quotes_in_join ... ok
test normalize::span::description::sql::tests::savepoint_quoted ... ok
test normalize::span::description::sql::tests::savepoint_uppercase ... ok
test normalize::span::description::sql::tests::savepoint_uppercase_semicolon ... ok
test normalize::span::description::sql::tests::single_digit_in_table_name ... ok
test normalize::span::description::sql::tests::single_quoted_string ... ok
test normalize::span::description::sql::tests::mysql_comment_generic ... ok
test normalize::span::description::sql::tests::activerecord_truncated ... ok
test normalize::span::description::sql::tests::mysql_comment ... ok
test normalize::span::description::sql::tests::savepoint_quoted_backtick ... ok
test normalize::span::description::sql::tests::named_placeholders ... ok
test normalize::span::description::sql::tests::strip_prefixes ... ok
test normalize::span::description::sql::tests::strip_prefixes_mysql ... ok
test normalize::span::description::sql::tests::single_quoted_string_finished ... ok
test normalize::span::description::sql::tests::strip_prefixes_ansi ... ok
test normalize::span::description::sql::tests::single_quoted_string_unfinished ... ok
test normalize::span::description::sql::tests::clickhouse ... ok
test normalize::span::description::sql::tests::unique_alias ... ok
test normalize::span::description::sql::tests::unparameterized_ins_nvarchar ... ok
test normalize::span::description::sql::tests::type_casts ... ok
test normalize::span::description::sql::tests::update_multiple ... ok
test normalize::span::description::sql::tests::strip_prefixes_truncated ... ok
test normalize::span::description::sql::tests::uuid_in_table_name ... ok
test normalize::span::description::sql::tests::unparameterized_ins_uppercase ... ok
test normalize::span::description::sql::tests::values_multi ... ok
test normalize::span::description::sql::tests::various_parameterized_ins_lowercase ... ok
test normalize::span::description::sql::tests::various_parameterized_questionmarks ... ok
test normalize::contexts::tests::test_get_product_name ... ok
test normalize::span::description::sql::tests::various_parameterized_ins_percentage ... ok
test normalize::span::description::sql::tests::various_parameterized_ins_dollar ... ok
test normalize::span::description::tests::active_record ... ok
test normalize::span::description::sql::tests::whitespace_and_comments ... ok
test normalize::span::description::tests::active_record_with_db_system ... ok
test normalize::span::description::sql::tests::various_parameterized_strings ... ok
test normalize::span::description::tests::informed_sql_parser ... ok
test normalize::span::description::tests::span_description_scrub_empty ... ok
test normalize::span::description::tests::span_description_scrub_hex ... ok
test normalize::span::description::tests::span_description_scrub_nothing_cache ... ok
test normalize::span::description::tests::span_description_scrub_cache ... ok
test normalize::span::description::tests::span_description_scrub_only_urllike_on_http_ops ... ok
test normalize::span::description::sql::tests::strip_prefixes_mysql_generic ... ok
test normalize::span::description::tests::span_description_scrub_only_dblike_on_db_ops ... ok
test normalize::span::description::tests::span_description_scrub_only_domain ... ok
test normalize::span::description::tests::span_description_scrub_path_ids_end ... ok
test normalize::span::description::tests::span_description_scrub_path_md5_hashes ... ok
test normalize::span::description::tests::span_description_scrub_path_multiple_ids ... ok
test normalize::span::description::tests::span_description_scrub_path_uuids ... ok
test normalize::span::description::tests::span_description_scrub_path_ids_middle ... ok
test normalize::span::description::tests::span_description_scrub_redis_invalid ... ok
test normalize::span::description::tests::span_description_scrub_nothing_in_resource ... ok
test normalize::span::description::tests::span_description_scrub_redis_long_command ... ok
test normalize::span::description::tests::span_description_scrub_redis_no_args ... ok
test normalize::span::description::tests::span_description_scrub_redis_set ... ok
test normalize::span::description::tests::span_description_scrub_path_sha_hashes ... ok
test normalize::span::description::tests::span_description_scrub_redis_set_quoted ... ok
test normalize::span::description::tests::span_description_scrub_redis_whitespace ... ok
test normalize::span::description::sql::tests::various_parameterized_cutoff ... ok
test normalize::span::description::tests::span_description_scrub_resource_css ... ok
test normalize::span::description::tests::span_description_scrub_resource_script_numeric_filename ... ok
test normalize::span::description::tests::span_description_scrub_ui_load ... ok
test normalize::span::description::sql::tests::update_single ... ok
test normalize::span::description::tests::span_description_scrub_resource_script ... ok
test normalize::span::tag_extraction::tests::extract_table_insert ... ok
test normalize::span::tag_extraction::tests::extract_table_delete ... ok
test normalize::span::tag_extraction::tests::extract_table_multiple_mysql ... ok
test normalize::span::tag_extraction::tests::extract_table_multiple ... ok
test normalize::span::tag_extraction::tests::extract_table_multiple_advanced ... ok
test normalize::span::description::sql::tests::unparameterized_ins_odbc_escape_sequence ... ok
test normalize::span::tag_extraction::tests::extract_table_select ... ok
test normalize::span::tag_extraction::tests::extract_table_select_nested ... ok
test normalize::span::tag_extraction::tests::extract_table_update ... ok
test normalize::span::tag_extraction::tests::test_truncate_string_no_panic ... ok
test normalize::stacktrace::tests::test_coerce_empty_filename ... ok
test normalize::stacktrace::tests::test_does_not_overwrite_filename ... ok
test normalize::stacktrace::tests::test_coerces_url_filenames ... ok
test normalize::stacktrace::tests::test_ignores_results_with_empty_path ... ok
test normalize::stacktrace::tests::test_is_url ... ok
test normalize::span::tag_extraction::tests::test_http_method_context ... ok
test normalize::span::tag_extraction::tests::test_http_method_request_prioritized ... ok
test normalize::span::tag_extraction::tests::test_http_method_txname ... ok
test normalize::stacktrace::tests::test_ignores_results_with_slash_path ... ok
test normalize::tests::test_context_line_default ... ok
test normalize::tests::test_context_line_retain ... ok
test normalize::span::tag_extraction::tests::extract_sql_action ... ok
test normalize::tests::test_drops_measurements_with_invalid_characters ... ok
test normalize::tests::test_discards_received ... ok
test normalize::tests::test_drops_too_long_measurement_names ... ok
test normalize::tests::test_empty_environment_is_removed ... ok
test normalize::tests::test_empty_environment_is_removed_and_overwritten_with_tag ... ok
test normalize::tests::test_empty_tags_removed ... ok
test normalize::tests::test_environment_tag_is_moved ... ok
test normalize::tests::test_event_level_defaulted ... ok
test normalize::tests::test_exception_invalid ... ok
test normalize::tests::test_frame_null_context_lines ... ok
test normalize::mechanism::tests::test_normalize_http_url ... ok
test normalize::tests::test_filter_custom_measurements ... ok
test normalize::tests::test_future_timestamp ... ok
test normalize::tests::test_android_medium_device_class ... ok
test normalize::tests::test_apple_high_device_class ... ok
test normalize::tests::test_apple_low_device_class ... ok
test normalize::tests::test_computed_measurements ... ok
test normalize::tests::test_android_high_device_class ... ok
test normalize::tests::test_apple_medium_device_class ... ok
test normalize::tests::test_geo_from_ip_address ... ok
test normalize::tests::test_android_low_device_class ... ok
test normalize::tests::test_internal_tags_removed ... ok
test normalize::tests::test_handles_type_in_value ... ok
test normalize::tests::test_invalid_release_removed ... ok
test normalize::tests::test_keeps_valid_measurement ... ok
test normalize::tests::test_json_value ... ok
test normalize::tests::test_grouping_config ... ok
test normalize::tests::test_light_normalization_respects_is_renormalize ... ok
test normalize::tests::test_max_custom_measurement ... ok
test normalize::tests::test_light_normalize_validates_spans ... ok
test normalize::tests::test_merge_builtin_measurement_keys ... ok
test normalize::tests::test_no_device_class ... ok
test normalize::tests::test_none_environment_errors ... ok
test normalize::tests::test_light_normalization_is_idempotent ... ok
test normalize::tests::test_logentry_error ... ok
test normalize::tests::test_normalize_app_start_cold_spans_for_react_native ... ok
test normalize::tests::test_normalize_app_start_spans_only_for_react_native_3_to_4_4 ... ok
test normalize::tests::test_normalize_app_start_measurements_does_not_add_measurements ... ok
test normalize::tests::test_normalize_dist_empty ... ok
test normalize::tests::test_normalize_dist_none ... ok
test normalize::tests::test_normalize_dist_trim ... ok
test normalize::tests::test_normalize_dist_whitespace ... ok
test normalize::tests::test_normalize_app_start_warm_spans_for_react_native ... ok
test normalize::tests::test_normalize_app_start_warm_measurements ... ok
test normalize::tests::test_normalize_app_start_cold_measurements ... ok
test normalize::tests::test_normalize_security_report ... ok
test normalize::tests::test_normalize_logger_exact_length ... ok
test normalize::tests::test_normalize_logger_empty ... ok
test normalize::tests::test_normalize_units ... ok
test normalize::tests::test_parses_sdk_info_from_header ... ok
test normalize::tests::test_normalize_logger_trimmed ... ok
test normalize::tests::test_normalize_logger_word_leading_dots ... ok
test normalize::tests::test_normalize_logger_short_no_trimming ... ok
test normalize::tests::test_normalize_logger_too_long_single_word ... ok
test normalize::tests::test_regression_backfills_abs_path_even_when_moving_stacktrace ... ok
test normalize::tests::test_rejects_empty_exception_fields ... ok
test normalize::tests::test_normalize_logger_word_trimmed_at_max ... ok
test normalize::tests::test_normalize_logger_word_trimmed_before_max ... ok
test normalize::tests::test_past_timestamp ... ok
test normalize::tests::test_replay_id_added_from_dsc ... ok
test normalize::tests::test_tags_deduplicated ... ok
test normalize::tests::test_too_long_distribution ... ok
test normalize::tests::test_top_level_keys_moved_into_tags ... ok
test normalize::tests::test_too_long_tags ... ok
test normalize::tests::test_transaction_level_untouched ... ok
test normalize::tests::test_transaction_status_defaulted_to_unknown ... ok
test normalize::tests::test_user_data_moved ... ok
test normalize::tests::test_unknown_debug_image ... ok
test normalize::tests::test_user_ip_from_client_ip_without_auto ... ok
test normalize::tests::test_user_ip_from_client_ip_without_appropriate_platform ... ok
test normalize::tests::test_user_ip_from_invalid_remote_addr ... ok
test normalize::tests::test_user_ip_from_remote_addr ... ok
test normalize::tests::test_user_ip_from_client_ip_with_auto ... ok
test normalize::user_agent::tests::test_choose_client_hints_for_os_context ... ok
test normalize::user_agent::tests::test_default_empty ... ok
test normalize::user_agent::tests::test_client_hint_parser ... ok
test normalize::user_agent::tests::test_client_hints_detected ... ok
test normalize::user_agent::tests::test_client_hints_with_unknown_browser ... ok
test normalize::user_agent::tests::test_ignore_empty_device ... ok
test normalize::user_agent::tests::test_ignore_empty_os ... ok
test normalize::user_agent::tests::test_ignore_empty_browser ... ok
test normalize::user_agent::tests::test_keep_empty_os_version ... ok
test normalize::tests::test_geo_in_light_normalize ... ok
test normalize::user_agent::tests::test_indicate_frozen_os_windows ... ok
test normalize::user_agent::tests::test_indicate_frozen_os_mac ... ok
test normalize::user_agent::tests::test_fallback_on_ua_string_for_os ... ok
test normalize::user_agent::tests::test_skip_no_user_agent ... ok
test normalize::user_agent::tests::test_strip_quotes ... ok
test normalize::user_agent::tests::test_strip_whitespace_and_quotes ... ok
test normalize::user_agent::tests::test_use_client_hints_for_device ... ok
test normalize::user_agent::tests::test_user_agent_does_not_override_prefilled ... ok
test normalize::user_agent::tests::test_verison_missing_minor ... ok
test normalize::user_agent::tests::test_version_major ... ok
test normalize::user_agent::tests::test_version_major_minor ... ok
test normalize::user_agent::tests::test_version_major_minor_patch ... ok
test normalize::user_agent::tests::test_version_none ... ok
test remove_other::tests::test_breadcrumb_errors ... ok
test remove_other::tests::test_remove_legacy_attributes ... ok
test remove_other::tests::test_remove_nested_other ... ok
test remove_other::tests::test_remove_unknown_attributes ... ok
test remove_other::tests::test_retain_context_other ... ok
test replay::tests::test_capped_values ... ok
test replay::tests::test_event_roundtrip ... ok
test replay::tests::test_lenient_release ... ok
test normalize::user_agent::tests::test_skip_unrecognizable_user_agent ... ok
test normalize::user_agent::tests::fallback_on_ua_string_when_missing_browser_field ... ok
test normalize::user_agent::tests::test_all_contexts ... ok
test normalize::user_agent::tests::test_device_context ... ok
test normalize::user_agent::tests::test_fallback_to_ua_if_no_client_hints ... ok
test replay::tests::test_truncated_list_less_than_limit ... ok
test replay::tests::test_validate_u16_segment_id ... ok
test schema::tests::test_client_sdk_missing_attribute ... ok
test schema::tests::test_invalid_email ... ok
test schema::tests::test_mechanism_missing_attributes ... ok
test schema::tests::test_newlines_release ... ok
test schema::tests::test_stacktrace_missing_attribute ... ok
test transactions::processor::tests::test_allows_transaction_event_with_empty_span_list ... ok
test transactions::processor::tests::test_allows_transaction_event_with_null_span_list ... ok
test transactions::processor::tests::test_allows_transaction_event_without_span_list ... ok
test transactions::processor::tests::test_default_transaction_source_unknown ... ok
test transactions::processor::tests::test_allows_valid_transaction_event_with_spans ... ok
test transactions::processor::tests::test_defaults_missing_op_in_context ... ok
test transactions::processor::tests::test_defaults_transaction_event_with_span_with_missing_op ... ok
test transactions::processor::tests::test_defaults_transaction_name_when_empty ... ok
test transactions::processor::tests::test_discards_on_missing_context ... ok
test transactions::processor::tests::test_discards_on_missing_contexts_map ... ok
test transactions::processor::tests::test_discards_on_missing_span_id_in_context ... ok
test transactions::processor::tests::test_discards_on_missing_trace_id_in_context ... ok
test transactions::processor::tests::test_defaults_transaction_name_when_missing ... ok
test transactions::processor::tests::test_discards_on_null_context ... ok
test transactions::processor::tests::test_discards_transaction_event_with_nulled_out_span ... ok
test transactions::processor::tests::test_discards_transaction_event_with_span_with_missing_span_id ... ok
test transactions::processor::tests::test_discards_transaction_event_with_span_with_missing_start_timestamp ... ok
test transactions::processor::tests::test_discards_transaction_event_with_span_with_missing_trace_id ... ok
test transactions::processor::tests::test_discards_when_missing_start_timestamp ... ok
test transactions::processor::tests::test_discards_when_missing_timestamp ... ok
test transactions::processor::tests::test_discards_when_timestamp_out_of_range ... ok
test transactions::processor::tests::test_is_high_cardinality_sdk_ruby_error ... ok
test transactions::processor::tests::test_is_high_cardinality_sdk_ruby_ok ... ok
test transactions::processor::tests::test_normalize_legacy_javascript ... ok
test transactions::processor::tests::test_normalize_legacy_python ... ok
test transactions::processor::tests::test_no_sanitized_if_no_rules ... ok
test transactions::processor::tests::test_normalize_twice ... ok
test transactions::processor::tests::test_replace_missing_timestamp ... ok
test normalize::user_agent::tests::test_os_context_short_version ... ok
test normalize::user_agent::tests::test_os_context ... ok
test transactions::processor::tests::test_skips_non_transaction_events ... ok
test transactions::processor::tests::test_normalize_transaction_names ... ok
test transactions::processor::tests::test_scrub_identifiers_and_apply_rules ... ok
test transactions::processor::tests::test_scrub_identifiers_before_rules ... ok
test transactions::processor::tests::test_transaction_name_normalize ... ok
test transactions::processor::tests::test_transaction_name_normalize_hex ... ok
test transactions::processor::tests::test_transaction_name_normalize_in_segments_1 ... ok
test transactions::processor::tests::test_transaction_name_normalize_in_segments_3 ... ok
test replay::tests::test_set_ip_address_missing_user_ip_address ... ok
test transactions::processor::tests::test_transaction_name_normalize_id ... ok
test transactions::processor::tests::test_transaction_name_normalize_in_segments_2 ... ok
test transactions::processor::tests::test_transaction_name_normalize_mark_as_sanitized ... ok
test transactions::processor::tests::test_transaction_name_normalize_in_segments_4 ... ok
test transactions::processor::tests::test_transaction_name_normalize_in_segments_5 ... ok
test transactions::processor::tests::test_transaction_name_normalize_mark_as_sanitized_when_ready ... ok
test transactions::processor::tests::test_transaction_name_normalize_url_encode_1 ... ok
test transactions::processor::tests::test_transaction_name_normalize_url_encode_5 ... ok
test transactions::processor::tests::test_transaction_name_normalize_url_encode_4 ... ok
test transactions::processor::tests::test_transaction_name_normalize_sha ... ok
test transactions::processor::tests::test_transaction_name_normalize_url_encode_6 ... ok
test transactions::processor::tests::test_transaction_name_normalize_url_encode_7 ... ok
test transactions::processor::tests::test_transaction_name_normalize_url_encode_3 ... ok
test transactions::processor::tests::test_transaction_name_normalize_url_encode_2 ... ok
test transactions::processor::tests::test_transaction_name_normalize_windows_path ... ok
test transactions::processor::tests::test_transaction_name_skip_original_value ... ok
test transactions::processor::tests::test_transaction_name_rename_end_slash ... ok
test transactions::processor::tests::test_transaction_name_unsupported_source ... ok
test transactions::rules::tests::test_rule_format ... ok
test normalize::user_agent::tests::test_os_context_full_version ... ok
test transactions::processor::tests::test_transaction_name_skip_replace_all ... ok
test transactions::rules::tests::test_rule_format_defaults ... ok
test transactions::rules::tests::test_rule_format_roundtrip ... ok
test transactions::rules::tests::test_rule_format_unsupported_reduction ... ok
test trimming::tests::test_basic_trimming ... ok
test transactions::processor::tests::test_transaction_name_skip_replace_all2 ... ok
test trimming::tests::test_custom_context_trimming ... ok
test transactions::processor::tests::test_transaction_name_normalize_uuid ... ok
test trimming::tests::test_databag_stripping ... ok
test trimming::tests::test_frame_hard_limit ... ok
test trimming::tests::test_frameqty_equals_limit ... ok
test trimming::tests::test_slim_frame_data_over_max ... ok
test transactions::processor::tests::test_transaction_name_rename_with_rules ... ok
test trimming::tests::test_slim_frame_data_under_max ... ok
test trimming::tests::test_string_trimming ... ok
test trimming::tests::test_tags_stripping ... ok
test trimming::tests::test_databag_state_leak ... ok
test replay::tests::test_set_user_agent_meta ... ok
test replay::tests::test_loose_type_requirements ... ok
test trimming::tests::test_extra_trimming_long_arrays ... ok
test trimming::tests::test_databag_array_stripping ... ok
test normalize::user_agent::tests::test_browser_context ... ok
test replay::tests::test_missing_user ... ok

test result: ok. 423 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.68s


running 170 tests
test processor::chunks::tests::test_chunk_split ... ok
test protocol::breadcrumb::tests::test_python_ty_regression ... ok
test protocol::breadcrumb::tests::test_breadcrumb_default_values ... ok
test protocol::clientsdk::tests::test_client_sdk_default_values ... ok
test protocol::client_report::tests::test_client_report_roundtrip ... ok
test protocol::breadcrumb::tests::test_breadcrumb_roundtrip ... ok
test protocol::clientsdk::tests::test_client_sdk_roundtrip ... ok
test protocol::contexts::browser::tests::test_browser_context_roundtrip ... ok
test protocol::contexts::cloud_resource::tests::test_cloud_resource_context_roundtrip ... ok
test protocol::contexts::app::tests::test_app_context_roundtrip ... ok
test protocol::contexts::os::tests::test_os_context_roundtrip ... ok
test protocol::contexts::profile::tests::test_trace_context_normalization ... ok
test protocol::contexts::profile::tests::test_trace_context_roundtrip ... ok
test protocol::contexts::otel::tests::test_otel_context_roundtrip ... ok
test protocol::contexts::replay::tests::test_replay_context_normalization ... ok
test protocol::contexts::replay::tests::test_trace_context_roundtrip ... ok
test protocol::contexts::device::tests::test_device_context_roundtrip ... ok
test protocol::contexts::reprocessing::tests::test_reprocessing_context_roundtrip ... ok
test protocol::contexts::runtime::tests::test_runtime_context_roundtrip ... ok
test protocol::contexts::response::tests::test_response_context_roundtrip ... ok
test protocol::contexts::tests::test_other_context_roundtrip ... ok
test protocol::contexts::tests::test_multiple_contexts_roundtrip ... ok
test protocol::contexts::tests::test_untagged_context_deserialize ... ok
test protocol::contexts::tests::test_context_processing ... ok
test protocol::contexts::trace::tests::test_trace_context_normalization ... ok
test protocol::contexts::trace::tests::test_trace_context_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_apple_default_values ... ok
test protocol::debugmeta::tests::test_debug_image_apple_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_elf_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_jvm_based_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_macho_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_other_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_pe_dotnet_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_proguard_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_pe_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_symbolic_default_values ... ok
test protocol::debugmeta::tests::test_debug_image_symbolic_legacy ... ok
test protocol::debugmeta::tests::test_debug_image_untagged_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_image_symbolic_roundtrip ... ok
test protocol::debugmeta::tests::test_debug_meta_default_values ... ok
test protocol::debugmeta::tests::test_debug_meta_roundtrip ... ok
test protocol::debugmeta::tests::test_source_map_image_roundtrip ... ok
test protocol::event::tests::test_empty_threads ... ok
test protocol::event::tests::test_event_default_values ... ok
test protocol::event::tests::test_event_type ... ok
test protocol::event::tests::test_field_value_provider_event_empty ... ok
test protocol::event::tests::test_extra_at ... ok
test protocol::event::tests::test_event_default_values_with_meta ... ok
test protocol::event::tests::test_field_value_provider_event_filled ... ok
test protocol::event::tests::test_fingerprint_empty_string ... ok
test protocol::event::tests::test_event_roundtrip ... ok
test protocol::event::tests::test_fingerprint_null_values ... ok
test protocol::event::tests::test_lenient_release ... ok
test protocol::exception::tests::test_coerces_object_value_to_string ... ok
test protocol::exception::tests::test_exception_default_values ... ok
test protocol::exception::tests::test_exception_empty_fields ... ok
test protocol::exception::tests::test_exception_roundtrip ... ok
test protocol::exception::tests::test_explicit_none ... ok
test protocol::fingerprint::tests::test_fingerprint_bool ... ok
test protocol::fingerprint::tests::test_fingerprint_empty ... ok
test protocol::fingerprint::tests::test_fingerprint_float ... ok
test protocol::fingerprint::tests::test_fingerprint_float_bounds ... ok
test protocol::fingerprint::tests::test_fingerprint_float_strip ... ok
test protocol::fingerprint::tests::test_fingerprint_float_trunc ... ok
test protocol::fingerprint::tests::test_fingerprint_invalid_fallback ... ok
test protocol::fingerprint::tests::test_fingerprint_number ... ok
test protocol::fingerprint::tests::test_fingerprint_string ... ok
test protocol::logentry::tests::test_logentry_empty_params ... ok
test protocol::logentry::tests::test_logentry_from_message ... ok
test protocol::logentry::tests::test_logentry_invalid_params ... ok
test protocol::logentry::tests::test_logentry_named_params ... ok
test protocol::logentry::tests::test_logentry_roundtrip ... ok
test protocol::mechanism::tests::test_mechanism_default_values ... ok
test protocol::mechanism::tests::test_mechanism_empty ... ok
test protocol::mechanism::tests::test_mechanism_legacy_conversion ... ok
test protocol::measurements::tests::test_measurements_serialization ... ok
test protocol::mechanism::tests::test_mechanism_roundtrip ... ok
test protocol::replay::tests::test_event_roundtrip ... ok
test protocol::replay::tests::test_lenient_release ... ok
test protocol::request::tests::test_cookies_invalid ... ok
test protocol::request::tests::test_cookies_array ... ok
test protocol::request::tests::test_cookies_object ... ok
test protocol::request::tests::test_cookies_parsing ... ok
test protocol::request::tests::test_header_normalization ... ok
test protocol::request::tests::test_header_from_sequence ... ok
test protocol::request::tests::test_headers_lenient_value ... ok
test protocol::request::tests::test_headers_multiple_values ... ok
test protocol::request::tests::test_query_invalid ... ok
test protocol::request::tests::test_query_string ... ok
test protocol::request::tests::test_query_string_legacy_nested ... ok
test protocol::request::tests::test_querystring_without_value ... ok
test protocol::request::tests::test_request_roundtrip ... ok
test protocol::security_report::tests::test_csp_coerce_blocked_uri_if_missing ... ok
test protocol::security_report::tests::test_csp_culprit_0 ... ok
test protocol::security_report::tests::test_csp_culprit_2 ... ok
test protocol::security_report::tests::test_csp_culprit_1 ... ok
test protocol::security_report::tests::test_csp_culprit_5 ... ok
test protocol::security_report::tests::test_csp_get_message_0 ... ok
test protocol::security_report::tests::test_csp_culprit_uri_without_scheme ... ok
test protocol::security_report::tests::test_csp_culprit_4 ... ok
test protocol::security_report::tests::test_csp_basic ... ok
test protocol::security_report::tests::test_csp_culprit_3 ... ok
test protocol::security_report::tests::test_csp_get_message_1 ... ok
test protocol::security_report::tests::test_csp_get_message_2 ... ok
test protocol::security_report::tests::test_csp_get_message_3 ... ok
test protocol::security_report::tests::test_csp_get_message_4 ... ok
test protocol::security_report::tests::test_csp_get_message_5 ... ok
test protocol::security_report::tests::test_csp_get_message_6 ... ok
test protocol::security_report::tests::test_csp_get_message_7 ... ok
test protocol::security_report::tests::test_csp_get_message_8 ... ok
test protocol::security_report::tests::test_csp_get_message_9 ... ok
test protocol::security_report::tests::test_effective_directive_from_violated_directive_single ... ok
test protocol::security_report::tests::test_csp_tags_stripe ... ok
test protocol::security_report::tests::test_csp_msdn ... ok
test protocol::security_report::tests::test_expectct_invalid ... ok
test protocol::security_report::tests::test_extract_effective_directive_from_long_form ... ok
test protocol::security_report::tests::test_csp_real ... ok
test protocol::security_report::tests::test_normalize_uri ... ok
test protocol::security_report::tests::test_expectct_basic ... ok
test protocol::security_report::tests::test_security_report_type_deserializer_recognizes_csp_reports ... ok
test protocol::security_report::tests::test_security_report_type_deserializer_recognizes_expect_ct_reports ... ok
test protocol::security_report::tests::test_security_report_type_deserializer_recognizes_expect_staple_reports ... ok
test protocol::security_report::tests::test_expectstaple_basic ... ok
test protocol::security_report::tests::test_hpkp_basic ... ok
test protocol::security_report::tests::test_security_report_type_deserializer_recognizes_hpkp_reports ... ok
test protocol::security_report::tests::test_unsplit_uri ... ok
test protocol::session::tests::test_session_abnormal_mechanism ... ok
test protocol::session::tests::test_session_default_timestamp_and_sid ... ok
test protocol::session::tests::test_session_invalid_abnormal_mechanism ... ok
test protocol::session::tests::test_session_default_values ... ok
test protocol::session::tests::test_session_ip_addr_auto ... ok
test protocol::session::tests::test_session_null_abnormal_mechanism ... ok
test protocol::session::tests::test_sessionstatus_unknown ... ok
test protocol::session::tests::test_session_roundtrip ... ok
test protocol::span::tests::test_getter_span_data ... ok
test protocol::span::tests::test_span_serialization ... ok
test protocol::stacktrace::tests::test_frame_default_values ... ok
test protocol::stacktrace::tests::test_frame_empty_context_lines ... ok
test protocol::stacktrace::tests::test_frame_vars_empty_annotated_is_serialized ... ok
test protocol::stacktrace::tests::test_frame_vars_null_preserved ... ok
test protocol::stacktrace::tests::test_frame_roundtrip ... ok
test protocol::stacktrace::tests::test_stacktrace_default_values ... ok
test protocol::stacktrace::tests::test_php_frame_vars ... ok
test protocol::tags::tests::test_tags_from_object ... ok
test protocol::stacktrace::tests::test_stacktrace_roundtrip ... ok
test protocol::templateinfo::tests::test_template_default_values ... ok
test protocol::tags::tests::test_tags_from_array ... ok
test protocol::templateinfo::tests::test_template_roundtrip ... ok
test protocol::thread::tests::test_thread_default_values ... ok
test protocol::thread::tests::test_thread_id ... ok
test protocol::thread::tests::test_thread_roundtrip ... ok
test protocol::transaction::tests::test_other_source_roundtrip ... ok
test protocol::thread::tests::test_thread_lock_reason_roundtrip ... ok
test protocol::types::tests::test_hex_deserialization ... ok
test protocol::transaction::tests::test_transaction_info_roundtrip ... ok
test protocol::types::tests::test_hex_from_string ... ok
test protocol::types::tests::test_hex_serialization ... ok
test protocol::types::tests::test_hex_to_string ... ok
test protocol::types::tests::test_level ... ok
test protocol::types::tests::test_ip_addr ... ok
test protocol::types::tests::test_timestamp_completely_out_of_range ... ok
test protocol::types::tests::test_timestamp_year_out_of_range ... ok
test protocol::types::tests::test_values_deserialization ... ok
test protocol::types::tests::test_values_serialization ... ok
test protocol::user::tests::test_explicit_none ... ok
test protocol::user::tests::test_geo_default_values ... ok
test protocol::user::tests::test_geo_roundtrip ... ok
test protocol::user::tests::test_user_invalid_id ... ok
test protocol::user::tests::test_user_lenient_id ... ok
test protocol::user::tests::test_user_roundtrip ... ok

test result: ok. 170 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s


running 2 tests
test tests::test_enums_processor_calls ... ok
test tests::test_simple_newtype ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 4 tests
test test_error ... ok
test test_ok ... ok
test test_panics ... ok
test test_unit ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 48 tests
test client_ips::tests::test_should_filter_blacklisted_ips ... ok
test browser_extensions::tests::test_dont_filter_when_disabled ... ok
test csp::tests::test_does_not_filter_benign_source_files ... ok
test csp::tests::test_does_not_filter_benign_uris ... ok
test csp::tests::test_does_not_filter_non_csp_messages ... ok
test csp::tests::test_filters_known_blocked_source_files ... ok
test csp::tests::test_filters_known_blocked_uris ... ok
test csp::tests::test_filters_known_document_uris ... ok
test csp::tests::test_scheme_domain_port ... ok
test csp::tests::test_matches_any_origin ... ok
test csp::tests::test_sentry_csp_filter_compatibility_bad_reports ... ok
test csp::tests::test_sentry_csp_filter_compatibility_good_reports ... ok
test error_messages::tests::test_filter_hydration_error ... ok
test error_messages::tests::test_should_filter_exception ... ok
test browser_extensions::tests::test_dont_filter_unkown_browser_extension ... ok
test browser_extensions::tests::test_filter_known_browser_extension_source ... ok
test legacy_browsers::tests::test_dont_filter_when_disabled ... ok
test browser_extensions::tests::test_filter_known_browser_extension_values ... ok
test config::tests::test_regression_legacy_browser_missing_options ... ok
test config::tests::test_empty_config ... ok
test config::tests::test_serialize_empty ... ok
test config::tests::test_serialize_full ... ok
test localhost::tests::test_dont_filter_missing_ip_or_domains ... ok
test localhost::tests::test_dont_filter_non_file_urls ... ok
test localhost::tests::test_dont_filter_non_local_domains ... ok
test localhost::tests::test_dont_filter_non_local_ip ... ok
test localhost::tests::test_dont_filter_when_disabled ... ok
test localhost::tests::test_filter_file_urls ... ok
test localhost::tests::test_filter_local_ip ... ok
test localhost::tests::test_filter_local_domains ... ok
test transaction_name::tests::test_does_not_filter_when_disabled ... ok
test transaction_name::tests::test_does_not_filter_when_disabled_with_flag ... ok
test transaction_name::tests::test_does_not_match_missing_transaction ... ok
test transaction_name::tests::test_does_not_filter_when_not_matching ... ok
test transaction_name::tests::test_filters_when_matching ... ok
test transaction_name::tests::test_does_not_match ... ok
test transaction_name::tests::test_matches ... ok
test transaction_name::tests::test_only_filters_transactions_not_anything_else ... ok
test web_crawlers::tests::test_filter_when_disabled ... ok
test releases::tests::test_release_filtering ... ok
test web_crawlers::tests::test_dont_filter_normal_user_agents ... ok
test web_crawlers::tests::test_filter_banned_user_agents ... ok
test legacy_browsers::tests::test_filter_default_browsers ... ok
test legacy_browsers::tests::test_dont_filter_default_above_minimum_versions ... ok
test legacy_browsers::tests::test_dont_filter_unconfigured_browsers ... ok
test legacy_browsers::tests::sentry_compatibility::test_dont_filter_sentry_allowed_user_agents ... ok
test legacy_browsers::tests::test_filter_configured_browsers ... ok
test legacy_browsers::tests::sentry_compatibility::test_filter_sentry_user_agents ... ok

test result: ok. 48 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.37s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test config::tests::test_kafka_config ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 65 tests
test aggregation::tests::test_bucket_value_cost ... ok
test aggregation::tests::test_bucket_key_cost ... ok
test aggregation::tests::test_aggregator_cost_enforcement_total ... ok
test aggregation::tests::test_aggregator_cost_tracking ... ok
test aggregation::tests::test_capped_iter_completeness_0 ... ok
test aggregation::tests::test_capped_iter_completeness_100 ... ok
test aggregation::tests::test_capped_iter_completeness_90 ... ok
test aggregation::tests::test_capped_iter_empty ... ok
test aggregation::tests::test_capped_iter_single ... ok
test aggregation::tests::test_capped_iter_split ... ok
test aggregation::tests::test_get_bucket_timestamp_multiple ... ok
test aggregation::tests::test_get_bucket_timestamp_non_multiple ... ok
test aggregation::tests::test_get_bucket_timestamp_overflow ... ok
test aggregation::tests::test_get_bucket_timestamp_zero ... ok
test aggregation::tests::test_parse_shift_key ... ok
test aggregation::tests::test_validate_bucket_key_chars ... ok
test aggregation::tests::test_validate_bucket_key_str_lens ... ok
test aggregation::tests::test_aggregator_mixed_projects ... ok
test bucket::tests::test_bucket_docs_roundtrip ... ok
test aggregation::tests::test_merge_back ... ok
test bucket::tests::test_bucket_value_merge_counter ... ok
test bucket::tests::test_bucket_value_merge_distribution ... ok
test bucket::tests::test_bucket_value_merge_gauge ... ok
test bucket::tests::test_bucket_value_merge_set ... ok
test aggregation::tests::test_aggregator_cost_enforcement_project ... ok
test bucket::tests::test_distribution_value_size ... ok
test bucket::tests::test_buckets_roundtrip ... ok
test bucket::tests::test_parse_all ... ok
test bucket::tests::test_parse_all_crlf ... ok
test bucket::tests::test_metrics_docs ... ok
test bucket::tests::test_parse_all_empty_lines ... ok
test bucket::tests::test_parse_all_trailing ... ok
test aggregation::tests::test_flush_bucket ... ok
test bucket::tests::test_parse_counter_packed ... ok
test aggregation::tests::test_validate_tag_values_special_chars ... ok
test bucket::tests::test_parse_distribution_packed ... ok
test bucket::tests::test_parse_empty_name ... ok
test bucket::tests::test_parse_garbage ... ok
test aggregation::tests::test_bucket_partitioning_128 ... ok
test aggregation::tests::test_bucket_partitioning_dummy ... ok
test bucket::tests::test_parse_bucket_defaults ... ok
test bucket::tests::test_parse_distribution ... ok
test bucket::tests::test_parse_counter ... ok
test bucket::tests::test_parse_buckets ... ok
test aggregation::tests::test_aggregator_merge_counters ... ok
test aggregation::tests::test_aggregator_merge_timestamps ... ok
test bucket::tests::test_parse_gauge ... ok
test bucket::tests::test_parse_gauge_packed ... ok
test bucket::tests::test_parse_histogram ... ok
test bucket::tests::test_parse_invalid_name ... ok
test aggregation::tests::test_cost_tracker ... ok
test bucket::tests::test_parse_invalid_name_with_leading_digit ... ok
test bucket::tests::test_parse_implicit_namespace ... ok
test bucket::tests::test_parse_sample_rate ... ok
test bucket::tests::test_parse_set ... ok
test bucket::tests::test_parse_set_hashed ... ok
test bucket::tests::test_parse_set_hashed_packed ... ok
test bucket::tests::test_parse_set_packed ... ok
test bucket::tests::test_parse_timestamp ... ok
test bucket::tests::test_parse_tags ... ok
test bucket::tests::test_parse_unit ... ok
test bucket::tests::test_parse_unit_regression ... ok
test bucket::tests::test_set_docs ... ok
test protocol::tests::test_sizeof_unit ... ok
test router::tests::condition_roundtrip ... ok

test result: ok. 65 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s


running 7 tests
test tests::truncate_basic ... ok
test tests::process_invalid_environment ... ok
test tests::process_empty_slug ... ok
test tests::process_with_upsert_short ... ok
test tests::process_with_upsert_interval ... ok
test tests::process_json_roundtrip ... ok
test tests::process_with_upsert_full ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 148 tests
test attachments::tests::test_fill_content_wstr ... ok
test attachments::tests::test_fill_content_wstr_panic - should panic ... ok
test attachments::tests::test_get_wstr_match ... ok
test attachments::tests::test_bytes_regexes ... ok
test attachments::tests::test_segments_all_data ... ok
test attachments::tests::test_segments_end_aligned ... ok
test attachments::tests::test_segments_garbage ... ok
test attachments::tests::test_segments_middle_2_byte_aligned ... ok
test attachments::tests::test_segments_middle_2_byte_aligned_mutation ... ok
test attachments::tests::test_segments_middle_unaligned ... ok
test attachments::tests::test_segments_multiple ... ok
test attachments::tests::test_segments_too_short ... ok
test attachments::tests::test_selectors ... ok
test attachments::tests::test_swap_content_wstr ... ok
test attachments::tests::test_swap_content_wstr_panic - should panic ... ok
test builtin::tests::test_builtin_rules_completeness ... ok
test attachments::tests::test_all_the_bytes ... ok
test builtin::tests::test_email ... ok
test builtin::tests::test_iban_different_rules ... ok
test builtin::tests::test_creditcard ... ok
test builtin::tests::test_iban_scrubbing_word_boundaries ... ok
test builtin::tests::test_invalid_iban_codes ... ok
test builtin::tests::test_imei ... ok
test attachments::tests::test_ip_masking ... ok
test attachments::tests::test_ip_replace_padding ... ok
test builtin::tests::test_mac ... ok
test builtin::tests::test_ipv6 ... ok
test builtin::tests::test_ipv4 ... ok
test builtin::tests::test_urlauth ... ok
test attachments::tests::test_ip_hash_trunchating ... ok
test attachments::tests::test_ip_replace_padding_utf16 ... ok
test attachments::tests::test_ip_removing ... ok
test builtin::tests::test_usssn ... ok
test builtin::tests::test_pemkey ... ok
test attachments::tests::test_ip_removing_utf16 ... ok
test attachments::tests::test_ip_hash_trunchating_utf16 ... ok
test builtin::tests::test_userpath ... ok
test builtin::tests::test_uuid ... ok
test attachments::tests::test_ip_masking_utf16 ... ok
test builtin::tests::test_valid_iban_codes ... ok
test convert::tests::test_convert_empty_sensitive_field ... ok
test convert::tests::test_convert_exclude_field ... ok
test convert::tests::test_datascrubbing_default ... ok
test convert::tests::test_convert_scrub_ip_only ... ok
test convert::tests::test_convert_default_pii_config ... ok
test convert::tests::test_convert_sensitive_fields ... ok
test convert::tests::test_csp_blocked_uri ... ok
test convert::tests::test_contexts ... ok
test convert::tests::test_authorization_scrubbing ... ok
test convert::tests::test_debug_meta_files_not_strippable ... ok
test convert::tests::test_does_not_fail_on_non_string ... ok
test convert::tests::test_does_not_sanitize_timestamp_looks_like_card ... ok
test convert::tests::test_doesnt_scrub_not_scrubbed ... ok
test convert::tests::test_does_sanitize_social_security_number ... ok
test convert::tests::test_does_sanitize_encrypted_private_key ... ok
test convert::tests::test_does_sanitize_public_key ... ok
test convert::tests::test_does_sanitize_private_key ... ok
test convert::tests::test_breadcrumb_message ... ok
test convert::tests::test_does_sanitize_rsa_private_key ... ok
test convert::tests::test_empty_field ... ok
test convert::tests::test_event_message_not_strippable ... ok
test convert::tests::test_exclude_fields_on_field_value ... ok
test convert::tests::test_exclude_fields_on_field_name ... ok
test convert::tests::test_ip_stripped ... ok
test convert::tests::test_extra ... ok
test convert::tests::test_http_remote_addr_stripped ... ok
test convert::tests::test_regression_more_odd_keys ... ok
test convert::tests::test_http ... ok
test convert::tests::test_querystring_as_pairlist ... ok
test convert::tests::test_explicit_fields_case_insensitive ... ok
test convert::tests::test_explicit_fields ... ok
test convert::tests::test_no_scrub_object_with_safe_fields ... ok
test convert::tests::test_querystring_as_pairlist_with_partials ... ok
test convert::tests::test_querystring_as_string ... ok
test convert::tests::test_querystring_as_string_with_partials ... ok
test convert::tests::test_odd_keys ... ok
test convert::tests::test_safe_fields_for_token ... ok
test convert::tests::test_sanitize_credit_card_discover ... ok
test convert::tests::test_sanitize_credit_card_amex ... ok
test convert::tests::test_sanitize_credit_card ... ok
test convert::tests::test_sanitize_credit_card_visa ... ok
test convert::tests::test_sanitize_credit_card_within_value_1 ... ok
test convert::tests::test_sanitize_credit_card_mastercard ... ok
test convert::tests::test_sanitize_http_body ... ok
test convert::tests::test_sanitize_credit_card_within_value_2 ... ok
test convert::tests::test_sanitize_url_2 ... ok
test convert::tests::test_sanitize_url_1 ... ok
test convert::tests::test_sanitize_url_4 ... ok
test convert::tests::test_sanitize_additional_sensitive_fields ... ok
test convert::tests::test_sanitize_url_3 ... ok
test convert::tests::test_sanitize_url_6 ... ok
test convert::tests::test_sanitize_url_5 ... ok
test convert::tests::test_sanitize_url_7 ... ok
test convert::tests::test_should_have_mysql_pwd_as_a_default_2 ... ok
test convert::tests::test_should_have_mysql_pwd_as_a_default_1 ... ok
test generate_selectors::tests::test_empty ... ok
test convert::tests::test_scrub_object ... ok
test convert::tests::test_stacktrace_paths_not_strippable ... ok
test convert::tests::test_stacktrace ... ok
test minidumps::tests::test_module_list_removed_lin ... ok
test minidumps::tests::test_module_list_selectors ... ok
test convert::tests::test_user ... ok
test minidumps::tests::test_linux_environ_valuetype ... ok
test generate_selectors::tests::test_full ... ok
test minidumps::tests::test_module_list_removed_win ... ok
test minidumps::tests::test_stack_scrubbing_deep_wildcard ... ok
test processor::tests::test_anything_hash_on_container ... ok
test minidumps::tests::test_module_list_removed_mac ... ok
test minidumps::tests::test_stack_scrubbing_binary_not_stack ... ok
test minidumps::tests::test_stack_scrubbing_backwards_compatible_selector ... ok
test minidumps::tests::test_stack_scrubbing_valuetype_not_fully_qualified ... ok
test processor::tests::test_anything_hash_on_string ... ok
test minidumps::tests::test_stack_scrubbing_valuetype_selector - should panic ... ok
test processor::tests::test_debugmeta_path_not_addressible_with_wildcard_selector ... ok
test processor::tests::test_ip_address_hashing ... ok
test processor::tests::test_ip_address_hashing_does_not_overwrite_id ... ok
test processor::tests::test_logentry_value_types ... ok
test convert::tests::test_sensitive_cookies ... ok
test processor::tests::test_hash_debugmeta_path ... ok
test minidumps::tests::test_stack_scrubbing_path_item_selector ... ok
test processor::tests::test_quoted_keys ... ok
test processor::tests::test_no_field_upsert ... ok
test processor::tests::test_redact_containers ... ok
test processor::tests::test_remove_debugmeta_path ... ok
test processor::tests::test_redact_custom_pattern ... ok
test processor::tests::test_replace_replaced_text_anything ... ok
test processor::tests::test_replace_replaced_text ... ok
test processor::tests::test_replace_debugmeta_path ... ok
test processor::tests::test_scrub_breadcrumb_data_http_not_scrubbed ... ok
test minidumps::tests::test_stack_scrubbing_wildcard - should panic ... ok
test processor::tests::test_scrub_breadcrumb_data_http_strings_are_scrubbed ... ok
test processor::tests::test_scrub_breadcrumb_data_untyped_props_are_scrubbed ... ok
test processor::tests::test_scrub_breadcrumb_data_http_objects_are_scrubbed ... ok
test processor::tests::test_does_not_scrub_if_no_graphql ... ok
test processor::tests::test_basic_stripping ... ok
test redactions::tests::test_redaction_deser_method ... ok
test redactions::tests::test_redaction_deser_other ... ok
test processor::tests::test_scrub_span_data_http_not_scrubbed ... ok
test processor::tests::test_scrub_graphql_response_data_with_variables ... ok
test selector::tests::test_invalid ... ok
test selector::tests::test_roundtrip ... ok
test selector::tests::test_attachments_matching ... ok
test processor::tests::test_scrub_original_value ... ok
test processor::tests::test_scrub_graphql_response_data_without_variables ... ok
test processor::tests::test_scrub_span_data_untyped_props_are_scrubbed ... ok
test processor::tests::test_scrub_span_data_http_strings_are_scrubbed ... ok
test processor::tests::test_scrub_span_data_http_objects_are_scrubbed ... ok
test selector::tests::test_matching ... ok

test result: ok. 148 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s


running 1 test
test test_scrub_pii_from_annotated_replay ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s


running 30 tests
test measurements::tests::test_value_as_float ... ok
test measurements::tests::test_value_as_string ... ok
test native_debug_image::tests::test_native_debug_image_compatibility ... ok
test sample::tests::test_expand_with_all_samples_outside_transaction ... ok
test sample::tests::test_copying_transaction ... ok
test sample::tests::test_expand ... ok
test sample::tests::test_filter_samples ... ok
test sample::tests::test_expand_with_samples_inclusive ... ok
test sample::tests::test_extract_transaction_tags ... ok
test sample::tests::test_keep_profile_under_max_duration ... ok
test android::tests::test_remove_invalid_events ... ok
test sample::tests::test_parse_profile_with_all_samples_filtered ... ok
test sample::tests::test_parse_with_no_transaction ... ok
test sample::tests::test_profile_cleanup_metadata ... ok
test sample::tests::test_reject_profile_over_max_duration ... ok
test sample::tests::test_profile_remove_idle_samples_at_start_and_end ... ok
test sample::tests::test_roundtrip ... ok
test tests::test_minimal_profile_with_version ... ok
test tests::test_minimal_profile_without_version ... ok
test transaction_metadata::tests::test_invalid_transaction_metadata ... ok
test tests::test_expand_profile_with_version ... ok
test transaction_metadata::tests::test_valid_transaction_metadata ... ok
test transaction_metadata::tests::test_valid_transaction_metadata_without_relative_timestamp ... ok
test android::tests::test_no_transaction ... ok
test extract_from_transaction::tests::test_extract_transaction_metadata ... ok
test android::tests::test_transactions_to_top_level ... ok
test tests::test_expand_profile_without_version ... ok
test android::tests::test_extract_transaction_metadata ... ok
test android::tests::test_timestamp ... ok
test android::tests::test_roundtrip_android ... ok

test result: ok. 30 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.51s


running 11 tests
test macros::tests::get_path ... ok
test macros::tests::get_path_array ... ok
test macros::tests::get_path_empty ... ok
test macros::tests::get_path_combined ... ok
test macros::tests::get_path_object ... ok
test macros::tests::get_value ... ok
test macros::tests::get_value_array ... ok
test macros::tests::get_value_combined ... ok
test macros::tests::get_value_empty ... ok
test macros::tests::get_value_object ... ok
test size::tests::test_estimate_size ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test test_annotated_deserialize_with_meta ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 20 tests
test test_signed_integers ... ok
test test_floats ... ok
test test_skip_array_never ... ok
test test_skip_array_null ... ok
test test_skip_array_empty ... ok
test test_skip_array_null_deep ... ok
test test_skip_array_empty_deep ... ok
test test_skip_object_empty ... ok
test test_skip_object_never ... ok
test test_skip_object_empty_deep ... ok
test test_skip_object_null ... ok
test test_skip_serialization_on_regular_structs ... ok
test test_skip_object_null_deep ... ok
test test_skip_tuple_empty_deep ... ok
test test_skip_tuple_empty ... ok
test test_skip_tuple_never ... ok
test test_skip_tuple_null ... ok
test test_skip_tuple_null_deep ... ok
test test_wrapper_structs_and_skip_serialization ... ok
test test_unsigned_integers ... ok

test result: ok. 20 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 44 tests
test quota::tests::test_quota_invalid_limited_mixed ... ok
test quota::tests::test_quota_invalid_only_unknown ... ok
test quota::tests::test_quota_invalid_unlimited_mixed ... ok
test quota::tests::test_quota_matches_key_scope ... ok
test quota::tests::test_quota_matches_multiple_categores ... ok
test quota::tests::test_quota_matches_no_categories ... ok
test quota::tests::test_quota_matches_no_invalid_scope ... ok
test quota::tests::test_quota_matches_organization_scope ... ok
test quota::tests::test_quota_matches_unknown_category ... ok
test quota::tests::test_quota_matches_project_scope ... ok
test quota::tests::test_quota_valid_reject_all ... ok
test quota::tests::test_quota_valid_reject_all_mixed ... ok
test rate_limit::tests::test_rate_limit_matches_categories ... ok
test rate_limit::tests::test_parse_retry_after ... ok
test rate_limit::tests::test_rate_limit_matches_organization ... ok
test rate_limit::tests::test_rate_limit_matches_key ... ok
test rate_limit::tests::test_rate_limit_matches_project ... ok
test quota::tests::test_parse_quota_limited ... ok
test quota::tests::test_parse_quota_reject_all ... ok
test quota::tests::test_parse_quota_unlimited ... ok
test quota::tests::test_parse_quota_project ... ok
test quota::tests::test_parse_quota_key ... ok
test quota::tests::test_parse_quota_unknown_variants ... ok
test rate_limit::tests::test_rate_limits_add_replacement ... ok
test quota::tests::test_parse_quota_reject_transactions ... ok
test quota::tests::test_parse_quota_project_large ... ok
test rate_limit::tests::test_rate_limits_add_buckets ... ok
test rate_limit::tests::test_rate_limits_add_shadowing ... ok
test rate_limit::tests::test_rate_limits_check ... ok
test rate_limit::tests::test_rate_limits_check_quotas ... ok
test rate_limit::tests::test_rate_limits_clean_expired ... ok
test rate_limit::tests::test_rate_limits_longest ... ok
test redis::tests::test_get_redis_key_scoped ... ok
test rate_limit::tests::test_rate_limits_merge ... ok
test redis::tests::test_get_redis_key_unscoped ... ok
test redis::tests::test_large_redis_limit_large ... ok
test redis::tests::test_bails_immediately_without_any_quota ... ok
test redis::tests::test_zero_size_quotas ... ok
test redis::tests::test_limited_with_unlimited_quota ... ok
test redis::tests::test_quantity_0 ... ok
test redis::tests::test_quota_go_over ... ok
test redis::tests::test_quota_with_quantity ... ok
test redis::tests::test_simple_quota ... ok
test redis::tests::test_is_rate_limited_script ... ok

test result: ok. 44 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.08s


running 4 tests
test config::tests::test_redis_single ... ok
test config::tests::test_redis_single_opts_default ... ok
test config::tests::test_redis_cluster_nodes_opts ... ok
test config::tests::test_redis_single_opts ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 15 tests
test recording::tests::test_pii_credit_card_removal ... ignored, type 3 nodes are not supported
test recording::tests::test_pii_ip_address_removal ... ignored, type 3 nodes are not supported
test recording::tests::test_scrub_pii_full_snapshot_event ... ignored, type 2 nodes are not supported
test recording::tests::test_scrub_pii_incremental_snapshot_event ... ignored, type 3 nodes are not supported
test recording::tests::test_scrub_at_path ... ok
test recording::tests::test_process_recording_no_contents ... ok
test recording::tests::test_process_recording_no_headers ... ok
test recording::tests::test_process_recording_no_body_data ... ok
test recording::tests::test_process_recording_bad_body_data ... ok
test recording::tests::test_process_recording_end_to_end ... ok
test recording::tests::test_scrub_pii_navigation ... ok
test recording::tests::test_scrub_pii_resource ... ok
test recording::tests::test_scrub_pii_key_based ... ok
test recording::tests::test_scrub_pii_custom_event ... ok
test recording::tests::test_scrub_pii_key_based_edge_cases ... ok

test result: ok. 11 passed; 0 failed; 4 ignored; 0 measured; 0 filtered out; finished in 0.07s


running 45 tests
test condition::tests::test_and_combinator ... ok
test condition::tests::test_not_combinator ... ok
test condition::tests::test_or_combinator ... ok
test condition::tests::unsupported_rule_deserialize ... ok
test config::tests::test_sample_rate_returns_same_samplingvalue_variant ... ok
test config::tests::config_deserialize ... ok
test config::tests::test_non_decaying_sampling_rule_deserialization_with_factor ... ok
test config::tests::test_sample_rate_valid_time_range ... ok
test config::tests::test_sample_rate_with_constant_decayingfn ... ok
test config::tests::test_non_decaying_sampling_rule_deserialization ... ok
test config::tests::test_sample_rate_with_linear_decay ... ok
test config::tests::test_sampling_config_with_rules_and_rules_v2_deserialization ... ok
test config::tests::test_sampling_rule_with_constant_decaying_function_deserialization ... ok
test config::tests::test_sampling_config_with_rules_and_rules_v2_serialization ... ok
test config::tests::test_supported ... ok
test config::tests::test_sampling_rule_with_linear_decaying_function_deserialization ... ok
test dsc::tests::getter_filled ... ok
test config::tests::test_unsupported_rule_type ... ok
test dsc::tests::getter_empty ... ok
test dsc::tests::parse_full ... ok
test dsc::tests::parse_user ... ok
test condition::tests::test_does_not_match ... ok
test dsc::tests::test_parse_sampled_with_incoming_boolean ... ok
test dsc::tests::test_parse_sample_rate_bogus ... ok
test dsc::tests::test_parse_sample_rate_number ... ok
test dsc::tests::test_parse_sample_rate_negative ... ok
test dsc::tests::test_parse_sampled_with_incoming_boolean_as_string ... ok
test dsc::tests::test_parse_sampled_with_incoming_invalid_boolean_as_string ... ok
test dsc::tests::test_parse_sampled_with_incoming_null_value ... ok
test evaluation::tests::matched_rule_ids_display ... ok
test evaluation::tests::matched_rule_ids_parse ... ok
test evaluation::tests::test_adjust_by_client_sample_rate ... ok
test evaluation::tests::test_adjust_sample_rate ... ok
test evaluation::tests::test_expired_rules ... ok
test evaluation::tests::test_get_sampling_match_result_with_no_match ... ok
test evaluation::tests::test_matches_reservoir ... ok
test evaluation::tests::test_repeatable_seed ... ok
test evaluation::tests::test_reservoir_evaluator_limit ... ok
test evaluation::tests::test_sample_rate_compounding ... ok
test evaluation::tests::test_condition_matching ... ok
test condition::tests::test_matches ... ok
test dsc::tests::test_parse_sample_rate ... ok
test dsc::tests::test_parse_sample_rate_scientific_notation ... ok
test dsc::tests::test_parse_user_partial ... ok
test condition::tests::deserialize ... ok

test result: ok. 45 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s


running 197 tests
test actors::processor::tests::test_breadcrumbs_reversed_with_none ... ok
test actors::processor::tests::test_breadcrumbs_truncation ... ok
test actors::processor::tests::test_breadcrumbs_order_with_none ... ok
test actors::processor::tests::test_breadcrumbs_file1 ... ok
test actors::processor::tests::test_breadcrumbs_file2 ... ok
test actors::processor::tests::test_compute_sampling_decision_matching ... ok
test actors::processor::tests::test_client_sample_rate ... ok
test actors::processor::tests::test_empty_breadcrumbs_item ... ok
test actors::processor::tests::test_error_is_tagged_correctly_if_trace_sampling_result_is_none ... ok
test actors::processor::tests::test_error_is_not_tagged_if_already_tagged ... ok
test actors::processor::tests::test_from_outcome_type_client_discard ... ok
test actors::processor::tests::test_from_outcome_type_filtered ... ok
test actors::processor::tests::test_from_outcome_type_rate_limited ... ok
test actors::processor::tests::test_from_outcome_type_sampled ... ok
test actors::processor::tests::test_it_keeps_or_drops_transactions ... ok
test actors::processor::tests::test_error_is_tagged_correctly_if_trace_sampling_result_is_some ... ok
test actors::processor::tests::test_client_report_forwarding ... ok
test actors::processor::tests::test_client_report_removal_in_processing ... ok
test actors::processor::tests::test_dsc_respects_metrics_extracted ... ok
test actors::processor::tests::test_client_report_removal ... ok
test actors::processor::tests::test_matching_with_unsupported_rule ... ok
test actors::processor::tests::test_mri_overhead_constant ... ok
test actors::processor::tests::test_process_session_invalid_json ... ok
test actors::global_config::tests::proxy_relay_does_not_make_upstream_request ... ok
test actors::processor::tests::test_process_session_invalid_timestamp ... ok
test actors::global_config::tests::managed_relay_makes_upstream_request - should panic ... ok
test actors::global_config::tests::shutdown_service ... ok
test actors::processor::tests::test_process_session_sequence_overflow ... ok
test actors::processor::tests::test_unprintable_fields ... ok
test actors::processor::tests::test_processor_panics ... ok
test actors::processor::tests::test_process_session_keep_item ... ok
test actors::processor::tests::test_process_session_metrics_extracted ... ok
test actors::project::tests::get_state_expired ... ok
test actors::project::tests::test_rate_limit_incoming_buckets ... ok
test actors::project::tests::test_rate_limit_incoming_metrics ... ok
test actors::project::tests::test_rate_limit_incoming_buckets_no_quota ... ok
test actors::processor::tests::test_user_report_invalid ... ok
test actors::project::tests::test_rate_limit_incoming_metrics_no_quota ... ok
test actors::project::tests::test_stale_cache ... ok
test actors::project_redis::tests::test_parse_redis_response ... ok
test actors::project_redis::tests::test_parse_redis_response_compressed ... ok
test actors::project_local::tests::test_multi_pub_static_config ... ok
test actors::project_local::tests::test_symlinked_projects ... ok
test actors::processor::tests::test_log_transaction_metrics_no_match ... ok
test actors::processor::tests::test_log_transaction_metrics_none ... ok
test actors::processor::tests::test_log_transaction_metrics_rule ... ok
test actors::processor::tests::test_log_transaction_metrics_pattern ... ok
test actors::processor::tests::test_log_transaction_metrics_both ... ok
test actors::store::tests::test_return_attachments_when_missing_event_item ... ok
test actors::spooler::tests::metrics_work ... ok
test actors::store::tests::test_send_standalone_attachments_when_transaction ... ok
test actors::store::tests::test_store_attachment_in_event_when_not_a_transaction ... ok
test endpoints::common::tests::test_minimal_empty_event ... ok
test endpoints::common::tests::test_minimal_event_id ... ok
test endpoints::common::tests::test_minimal_event_invalid_type ... ok
test endpoints::common::tests::test_minimal_event_type ... ok
test endpoints::minidump::tests::test_validate_minidump ... ok
test envelope::tests::test_deserialize_envelope_empty ... ok
test envelope::tests::test_deserialize_envelope_empty_item_eof ... ok
test envelope::tests::test_deserialize_envelope_empty_item_newline ... ok
test envelope::tests::test_deserialize_envelope_implicit_length ... ok
test envelope::tests::test_deserialize_envelope_empty_newline ... ok
test envelope::tests::test_deserialize_envelope_implicit_length_empty_eof ... ok
test envelope::tests::test_deserialize_envelope_implicit_length_eof ... ok
test envelope::tests::test_deserialize_envelope_replay_recording ... ok
test envelope::tests::test_deserialize_envelope_multiple_items ... ok
test envelope::tests::test_deserialize_envelope_unknown_item ... ok
test envelope::tests::test_deserialize_envelope_view_hierarchy ... ok
test envelope::tests::test_envelope_add_item ... ok
test envelope::tests::test_deserialize_request_meta ... ok
test envelope::tests::test_envelope_empty ... ok
test envelope::tests::test_item_empty ... ok
test envelope::tests::test_envelope_take_item ... ok
test envelope::tests::test_item_set_header ... ok
test envelope::tests::test_item_set_payload ... ok
test envelope::tests::test_parse_request_envelope ... ok
test envelope::tests::test_parse_request_no_dsn ... ok
test envelope::tests::test_parse_request_no_origin ... ok
test envelope::tests::test_parse_request_sent_at ... ok
test envelope::tests::test_parse_request_sent_at_null ... ok
test envelope::tests::test_parse_request_validate_key - should panic ... ok
test envelope::tests::test_parse_request_validate_origin - should panic ... ok
test envelope::tests::test_parse_request_validate_project - should panic ... ok
test envelope::tests::test_serialize_envelope_attachments ... ok
test envelope::tests::test_serialize_envelope_empty ... ok
test envelope::tests::test_split_envelope_all ... ok
test envelope::tests::test_split_envelope_none ... ok
test envelope::tests::test_split_envelope_some ... ok
test extractors::forwarded_for::tests::test_fall_back_on_forwarded_for_header ... ok
test extractors::forwarded_for::tests::test_get_empty_string_if_invalid_header ... ok
test extractors::forwarded_for::tests::test_prefer_vercel_forwarded ... ok
test extractors::start_time::tests::start_time_from_timestamp ... ok
test extractors::request_meta::tests::test_request_meta_roundtrip ... ok
test metrics_extraction::generic::tests::extract_counter ... ok
test metrics_extraction::generic::tests::extract_set ... ok
test metrics_extraction::generic::tests::extract_distribution ... ok
test metrics_extraction::generic::tests::extract_tag_precedence ... ok
test metrics_extraction::generic::tests::extract_tag_conditions ... ok
test metrics_extraction::sessions::tests::test_extract_session_metrics ... ok
test metrics_extraction::sessions::tests::test_extract_session_metrics_abnormal ... ok
test metrics_extraction::sessions::tests::test_extract_session_metrics_errored ... ok
test metrics_extraction::sessions::tests::test_extract_session_metrics_aggregate ... ok
test metrics_extraction::sessions::tests::test_extract_session_metrics_fatal ... ok
test metrics_extraction::sessions::tests::test_extract_session_metrics_ok ... ok
test metrics_extraction::sessions::tests::test_nil_to_none ... ok
test metrics_extraction::generic::tests::extract_tag_precedence_multiple_rules ... ok
test metrics_extraction::transactions::tests::test_any_client_route ... ok
test metrics_extraction::transactions::tests::test_computed_metrics ... ok
test metrics_extraction::transactions::tests::test_conditional_tagging ... ok
test metrics_extraction::transactions::tests::test_custom_measurements ... ok
test metrics_extraction::transactions::tests::test_express ... ok
test metrics_extraction::transactions::tests::test_get_eventuser_tag ... ok
test metrics_extraction::transactions::tests::test_express_options ... ok
test metrics_extraction::transactions::tests::test_js_url_strict ... ok
test metrics_extraction::transactions::tests::test_legacy_js_does_not_look_like_url ... ok
test metrics_extraction::transactions::tests::test_legacy_js_looks_like_url ... ok
test metrics_extraction::transactions::tests::test_metric_measurement_unit_overrides ... ok
test metrics_extraction::transactions::tests::test_metric_measurement_units ... ok
test metrics_extraction::transactions::tests::test_other_client_unknown ... ok
test metrics_extraction::transactions::tests::test_parse_transaction_name_strategy ... ok
test metrics_extraction::transactions::tests::test_other_client_url ... ok
test metrics_extraction::transactions::tests::test_extract_transaction_metrics ... ok
test metrics_extraction::transactions::tests::test_python_200 ... ok
test metrics_extraction::transactions::tests::test_python_404 ... ok
test metrics_extraction::event::tests::test_extract_span_metrics_mobile ... ok
test metrics_extraction::transactions::tests::test_root_counter_keep ... ok
test metrics_extraction::transactions::tests::test_span_tags ... ok
test metrics_extraction::transactions::tests::test_transaction_duration ... ok
test metrics_extraction::transactions::tests::test_unknown_transaction_status ... ok
test metrics_extraction::transactions::tests::test_unknown_transaction_status_no_trace_context ... ok
test middlewares::normalize_path::tests::root ... ok
test middlewares::normalize_path::tests::query_and_fragment ... ok
test middlewares::normalize_path::tests::no_trailing_slash ... ok
test middlewares::normalize_path::tests::path ... ok
test utils::dynamic_sampling::tests::test_is_trace_fully_sampled_with_invalid_inputs ... ok
test utils::dynamic_sampling::tests::test_is_trace_fully_sampled_return_true_with_unsupported_rules ... ok
test utils::dynamic_sampling::tests::test_is_trace_fully_sampled_with_valid_dsc_and_sampling_config ... ok
test utils::dynamic_sampling::tests::test_match_rules_return_drop_with_match_and_0_sample_rate ... ok
test utils::dynamic_sampling::tests::test_match_rules_return_keep_with_match_and_100_sample_rate ... ok
test utils::dynamic_sampling::tests::test_match_rules_return_keep_with_no_match ... ok
test utils::dynamic_sampling::tests::test_match_rules_with_traces_rules_return_keep_when_match ... ok
test utils::metrics_rate_limits::tests::profiles_limits_are_reported ... ok
test utils::garbage::tests::test_garbage_disposal ... ok
test utils::metrics_rate_limits::tests::profiles_quota_is_enforced ... ok
test utils::multipart::tests::test_empty_formdata ... ok
test utils::multipart::tests::test_formdata ... ok
test utils::multipart::tests::test_get_boundary ... ok
test utils::param_parser::tests::test_aggregator_base_0 ... ok
test utils::multipart::tests::missing_trailing_newline ... ok
test utils::param_parser::tests::test_aggregator_base_1 ... ok
test utils::param_parser::tests::test_aggregator_empty ... ok
test utils::param_parser::tests::test_aggregator_holes ... ok
test utils::param_parser::tests::test_aggregator_override ... ok
test utils::param_parser::tests::test_aggregator_reversed ... ok
test utils::param_parser::tests::test_chunk_index ... ok
test utils::param_parser::tests::test_index_parser ... ok
test utils::param_parser::tests::test_merge_vals ... ok
test utils::rate_limits::tests::test_enforce_event_metrics_extracted ... ok
test utils::param_parser::tests::test_update_value ... ok
test utils::rate_limits::tests::test_enforce_event_metrics_extracted_no_indexing_quota ... ok
test utils::rate_limits::tests::test_enforce_limit_assumed_attachments ... ok
test utils::rate_limits::tests::test_enforce_limit_assumed_event ... ok
test utils::rate_limits::tests::test_enforce_limit_attachments ... ok
test utils::rate_limits::tests::test_enforce_limit_error_event ... ok
test utils::rate_limits::tests::test_enforce_limit_error_with_attachments ... ok
test utils::rate_limits::tests::test_enforce_limit_minidump ... ok
test utils::rate_limits::tests::test_enforce_limit_monitor_checkins ... ok
test utils::rate_limits::tests::test_enforce_limit_profiles ... ok
test utils::rate_limits::tests::test_enforce_limit_replays ... ok
test utils::rate_limits::tests::test_enforce_limit_sessions ... ok
test utils::rate_limits::tests::test_enforce_pass_empty ... ok
test utils::rate_limits::tests::test_enforce_pass_minidump ... ok
test utils::rate_limits::tests::test_enforce_pass_sessions ... ok
test utils::rate_limits::tests::test_enforce_skip_rate_limited ... ok
test utils::rate_limits::tests::test_enforce_transaction_attachment_enforced ... ok
test utils::rate_limits::tests::test_enforce_transaction_attachment_enforced_metrics_extracted_indexing_quota ... ok
test utils::rate_limits::tests::test_enforce_transaction_no_indexing_quota ... ok
test utils::rate_limits::tests::test_enforce_transaction_no_metrics_extracted ... ok
test utils::rate_limits::tests::test_enforce_transaction_profile_enforced ... ok
test utils::rate_limits::tests::test_format_rate_limits ... ok
test utils::rate_limits::tests::test_parse_invalid_rate_limits ... ok
test utils::rate_limits::tests::test_parse_rate_limits ... ok
test utils::rate_limits::tests::test_parse_rate_limits_only_unknown ... ok
test utils::semaphore::tests::test_empty ... ok
test utils::semaphore::tests::test_single_thread ... ok
test utils::semaphore::tests::test_multi_thread ... ok
test utils::unreal::tests::test_merge_unreal_context_is_assert_level_error ... ok
test utils::unreal::tests::test_merge_unreal_context_is_esure_level_warning ... ok
test utils::unreal::tests::test_merge_unreal_context ... ok
test utils::unreal::tests::test_merge_unreal_logs ... ok
test utils::unreal::tests::test_merge_unreal_context_event ... ok
test metrics_extraction::event::tests::test_extract_span_metrics_all_modules ... ok
test metrics_extraction::event::tests::test_extract_span_metrics ... ok
test actors::spooler::tests::dequeue_waits_for_permits ... ok
test actors::spooler::tests::ensure_start_time_restore ... ok
test actors::processor::tests::test_browser_version_extraction_with_pii_like_data ... ok
test actors::project_cache::tests::always_spools ... ok

test result: ok. 197 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.87s


running 12 tests
test legacy_python::test_processing ... ok
test cordova::test_processing ... ok
test legacy_node_exception::test_processing ... ok
test dotnet::test_processing ... ok
test test_event_schema_snapshot ... ok
test unity_windows::test_processing ... ok
test unity_macos::test_processing ... ok
test unity_android::test_processing ... ok
test unity_ios::test_processing ... ok
test unity_linux::test_processing ... ok
test android::test_processing ... ok
test cocoa::test_processing ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s


running 1 test
test test_reponse_context_pii ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s


running 2 tests
test tests::current_client_is_global_client ... ok
test tests::test_capturing_client ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test service::tests::test_backpressure_metrics ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test relay-auth/src/lib.rs - (line 14) ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.92s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 5 tests
test relay-common/src/macros.rs - macros::derive_fromstr_and_display (line 80) ... ok
test relay-common/src/time.rs - time::UnixTimestamp::to_instant (line 147) ... ok
test relay-common/src/time.rs - time::chrono_to_positive_millis (line 53) ... ok
test relay-common/src/time.rs - time::chrono_to_positive_millis (line 43) ... ok
test relay-common/src/time.rs - time::duration_to_millis (line 25) ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.81s


running 2 tests
test relay-config/src/byte_size.rs - byte_size::ByteSize (line 24) ... ok
test relay-config/src/byte_size.rs - byte_size::ByteSize (line 33) ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.62s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 2 tests
test relay-event-schema/src/processor/attrs.rs - processor::attrs::BoxCow (line 378) ... ignored
test relay-event-schema/src/processor/chunks.rs - processor::chunks (line 8) ... ok

test result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.28s


running 8 tests
test relay-ffi/src/lib.rs - set_panic_hook (line 282) ... ok
test relay-ffi/src/lib.rs - (line 13) ... ok
test relay-ffi/src/lib.rs - (line 79) ... ok
test relay-ffi/src/lib.rs - (line 58) ... ok
test relay-ffi/src/lib.rs - take_last_error (line 188) ... ok
test relay-ffi/src/lib.rs - (line 41) ... ok
test relay-ffi/src/lib.rs - with_last_error (line 161) ... ok
test relay-ffi/src/lib.rs - Panic (line 212) ... ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.65s


running 1 test
test relay-ffi-macros/src/lib.rs - catch_unwind (line 52) ... ignored

test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 2 tests
test relay-kafka/src/config.rs - config::Sharded (line 196) ... ignored
test relay-kafka/src/lib.rs - (line 9) - compile fail ... ok

test result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.10s


running 9 tests
test relay-log/src/lib.rs - (line 101) ... ok
test relay-log/src/utils.rs - utils::backtrace_enabled (line 10) ... ok
test relay-log/src/lib.rs - (line 72) ... ok
test relay-log/src/lib.rs - (line 46) ... ok
test relay-log/src/test.rs - test::init_test (line 31) ... ok
test relay-log/src/lib.rs - (line 58) ... ok
test relay-log/src/setup.rs - setup::init (line 206) ... ok
test relay-log/src/lib.rs - (line 88) ... ok
test relay-log/src/lib.rs - (line 9) ... ok

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.35s


running 5 tests
test relay-metrics/src/bucket.rs - bucket::Bucket::parse (line 665) ... ok
test relay-metrics/src/protocol.rs - protocol::MetricResourceIdentifier (line 211) ... ok
test relay-metrics/src/bucket.rs - bucket::Bucket::parse_all (line 687) ... ok
test relay-metrics/src/bucket.rs - bucket::dist (line 116) ... ok
test relay-metrics/src/bucket.rs - bucket::DistributionValue (line 82) ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.73s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 4 tests
test relay-protocol/src/traits.rs - traits::SkipSerialization (line 35) ... ignored
test relay-protocol/src/traits.rs - traits::Getter (line 161) ... ok
test relay-protocol/src/macros.rs - macros::get_path (line 32) ... ok
test relay-protocol/src/macros.rs - macros::get_value (line 105) ... ok

test result: ok. 3 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.31s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 3 tests
test relay-replays/src/transform.rs - transform::Deserializer (line 159) ... ignored
test relay-replays/src/transform.rs - transform::Transform (line 33) ... ignored
test relay-replays/src/recording.rs - recording::RecordingScrubber (line 278) ... ok

test result: ok. 1 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 0.52s


running 20 tests
test relay-sampling/src/condition.rs - condition::RuleCondition::Or (line 411) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition (line 328) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::Lte (line 367) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::Glob (line 400) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::And (line 423) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::Gt (line 378) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::Eq (line 345) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::Lt (line 389) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::Not (line 435) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::Gte (line 356) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::and (line 575) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::eq (line 467) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::eq_ignore_case (line 487) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::glob (line 504) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::lt (line 547) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::gt (line 521) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::gte (line 534) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::lte (line 560) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::negate (line 621) ... ok
test relay-sampling/src/condition.rs - condition::RuleCondition::or (line 598) ... ok

test result: ok. 20 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.86s


running 2 tests
test relay-server/src/actors/mod.rs - actors (line 24) ... ignored
test relay-server/src/utils/multipart.rs - utils::multipart::get_multipart_boundary (line 134) ... ignored

test result: ok. 0 passed; 0 failed; 2 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 8 tests
test relay-statsd/src/lib.rs - (line 23) - compile ... ok
test relay-statsd/src/lib.rs - (line 50) ... ok
test relay-statsd/src/lib.rs - GaugeMetric (line 492) ... ok
test relay-statsd/src/lib.rs - HistogramMetric (line 411) ... ok
test relay-statsd/src/lib.rs - (line 34) ... ok
test relay-statsd/src/lib.rs - CounterMetric (line 358) ... ok
test relay-statsd/src/lib.rs - SetMetric (line 448) ... ok
test relay-statsd/src/lib.rs - TimerMetric (line 297) ... ok

test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.84s


running 16 tests
test relay-system/src/service.rs - service::FromMessage (line 572) ... ok
test relay-system/src/service.rs - service::BroadcastChannel (line 314) ... ok
test relay-system/src/service.rs - service::BroadcastSender<T>::into_channel (line 505) ... ok
test relay-system/src/service.rs - service::FromMessage (line 598) ... ok
test relay-system/src/service.rs - service::BroadcastChannel<T>::is_attached (line 408) ... ok
test relay-system/src/service.rs - service::BroadcastChannel<T>::attach (line 363) ... ok
test relay-system/src/service.rs - service::Service (line 897) - compile ... ok
test relay-system/src/service.rs - service::BroadcastSender<T>::send (line 480) ... ok
test relay-system/src/service.rs - service::BroadcastChannel<T>::send (line 384) ... ok
test relay-system/src/service.rs - service::BroadcastSender (line 445) ... ok
test relay-system/src/controller.rs - controller::Controller (line 118) ... ok
test relay-system/src/service.rs - service::FromMessage (line 626) ... ok
test relay-system/src/service.rs - service::Interface (line 34) ... ok
test relay-system/src/service.rs - service::Interface (line 78) ... ok
test relay-system/src/service.rs - service::Interface (line 54) ... ok
test relay-system/src/service.rs - service::Service (line 937) ... ok

test result: ok. 16 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.38s


running 1 test
test relay-test/src/lib.rs - (line 11) ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.29s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

