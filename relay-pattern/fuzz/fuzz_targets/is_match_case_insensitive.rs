#![no_main]

use libfuzzer_sys::fuzz_target;
use relay_pattern::Gas;

fuzz_target!(|input: (&str, &str)| {
    let (pattern, haystack) = input;
    if let Ok(pattern) = relay_pattern::Pattern::builder(pattern)
        .case_insensitive(true)
        .build()
    {
        let _ = std::hint::black_box(pattern.is_match(haystack, Gas::CONSTRAINED));
    }
});
