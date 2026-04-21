#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    test_ffi_config_parse(data);
    test_cache_info_parse(data);
});

fn test_ffi_config_parse(data: &[u8]) {
    if let Ok(s) = std::str::from_utf8(data) {
        let trimmed = s.trim();
        if trimmed.len() > 1000 {
            return;
        }

        if let Ok(config) = serde_json::from_str::<turbolite::TurboliteConfig>(trimmed) {
            // Phase Cirrus collapsed StorageBackend out of config; just
            // touch the fields that survived the refactor.
            std::hint::black_box(&config.cache_dir);
            std::hint::black_box(&config.compression);
            std::hint::black_box(&config.prefetch);
        }
    }
}

fn test_cache_info_parse(data: &[u8]) {
    if let Ok(s) = std::str::from_utf8(data) {
        let trimmed = s.trim();
        if trimmed.is_empty() || trimmed.len() > 100_000 {
            return;
        }

        if let Ok(_info) = serde_json::from_str::<serde_json::Value>(trimmed) {
            std::hint::black_box(trimmed);
        }
    }
}
