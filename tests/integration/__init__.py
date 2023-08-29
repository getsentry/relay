GLOBAL_CONFIG = {
    "measurements": {
        "builtinMeasurements": [
            {"name": "app_start_cold", "unit": "millisecond"},
            {"name": "app_start_warm", "unit": "millisecond"},
            {"name": "cls", "unit": "none"},
            {"name": "fcp", "unit": "millisecond"},
            {"name": "fid", "unit": "millisecond"},
            {"name": "fp", "unit": "millisecond"},
            {"name": "frames_frozen_rate", "unit": "ratio"},
            {"name": "frames_frozen", "unit": "none"},
            {"name": "frames_slow_rate", "unit": "ratio"},
            {"name": "frames_slow", "unit": "none"},
            {"name": "frames_total", "unit": "none"},
            {"name": "inp", "unit": "millisecond"},
            {"name": "lcp", "unit": "millisecond"},
            {"name": "stall_count", "unit": "none"},
            {"name": "stall_longest_time", "unit": "millisecond"},
            {"name": "stall_percentage", "unit": "ratio"},
            {"name": "stall_total_time", "unit": "millisecond"},
            {"name": "ttfb.requesttime", "unit": "millisecond"},
            {"name": "ttfb", "unit": "millisecond"},
            {"name": "time_to_full_display", "unit": "millisecond"},
            {"name": "time_to_initial_display", "unit": "millisecond"},
        ],
        "maxCustomMeasurements": 10,
    }
}
