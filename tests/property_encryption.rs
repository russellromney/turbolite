//! Siege 7: Encryption property tests.
//!
//! Validates AES-GCM and AES-CTR round-trips, nonce behavior,
//! and tampered ciphertext detection.

#![cfg(feature = "encryption")]

use proptest::prelude::*;
use turbolite::compress::{
    decrypt_ctr, decrypt_gcm, decrypt_gcm_random_nonce, encrypt_ctr, encrypt_gcm,
    encrypt_gcm_random_nonce,
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    // --- AES-GCM (page-num-derived nonce) ---

    #[test]
    fn gcm_roundtrip(
        data in proptest::collection::vec(any::<u8>(), 0..65536),
        page_num in 0u64..1_000_000,
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        let key: [u8; 32] = key.try_into().expect("key len");
        let encrypted = encrypt_gcm(&data, page_num, &key).expect("encrypt");
        let decrypted = decrypt_gcm(&encrypted, page_num, &key).expect("decrypt");
        prop_assert_eq!(&data, &decrypted);
    }

    #[test]
    fn gcm_ciphertext_differs_from_plaintext(
        data in proptest::collection::vec(any::<u8>(), 64..65536),
        page_num in 0u64..1_000_000,
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        let key: [u8; 32] = key.try_into().expect("key len");
        let encrypted = encrypt_gcm(&data, page_num, &key).expect("encrypt");
        // Ciphertext should differ from plaintext (except astronomically unlikely)
        prop_assert_ne!(&data[..], &encrypted[..data.len()]);
    }

    #[test]
    fn gcm_different_page_nums_produce_different_ciphertext(
        data in proptest::collection::vec(any::<u8>(), 64..1024),
        page_a in 0u64..1_000_000,
        page_b in 0u64..1_000_000,
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        prop_assume!(page_a != page_b);
        let key: [u8; 32] = key.try_into().expect("key len");
        let enc_a = encrypt_gcm(&data, page_a, &key).expect("encrypt a");
        let enc_b = encrypt_gcm(&data, page_b, &key).expect("encrypt b");
        prop_assert_ne!(enc_a, enc_b);
    }

    #[test]
    fn gcm_wrong_page_num_fails_decrypt(
        data in proptest::collection::vec(any::<u8>(), 64..1024),
        page_num in 0u64..1_000_000,
        wrong_page in 0u64..1_000_000,
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        prop_assume!(page_num != wrong_page);
        let key: [u8; 32] = key.try_into().expect("key len");
        let encrypted = encrypt_gcm(&data, page_num, &key).expect("encrypt");
        let result = decrypt_gcm(&encrypted, wrong_page, &key);
        prop_assert!(result.is_err(), "decryption with wrong page_num should fail");
    }

    #[test]
    fn gcm_wrong_key_fails_decrypt(
        data in proptest::collection::vec(any::<u8>(), 64..1024),
        page_num in 0u64..1_000_000,
        key_a in proptest::collection::vec(any::<u8>(), 32..=32),
        key_b in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        prop_assume!(key_a != key_b);
        let key_a: [u8; 32] = key_a.try_into().expect("key len");
        let key_b: [u8; 32] = key_b.try_into().expect("key len");
        let encrypted = encrypt_gcm(&data, page_num, &key_a).expect("encrypt");
        let result = decrypt_gcm(&encrypted, page_num, &key_b);
        prop_assert!(result.is_err(), "decryption with wrong key should fail");
    }

    #[test]
    fn gcm_tampered_ciphertext_fails(
        data in proptest::collection::vec(any::<u8>(), 64..1024),
        page_num in 0u64..1_000_000,
        key in proptest::collection::vec(any::<u8>(), 32..=32),
        tamper_pos in 0..1040usize,
        tamper_byte in any::<u8>(),
    ) {
        let key: [u8; 32] = key.try_into().expect("key len");
        let encrypted = encrypt_gcm(&data, page_num, &key).expect("encrypt");
        if tamper_pos < encrypted.len() {
            let mut tampered = encrypted.clone();
            tampered[tamper_pos] ^= tamper_byte | 1; // ensure at least 1 bit flips
            let result = decrypt_gcm(&tampered, page_num, &key);
            prop_assert!(result.is_err(), "tampered ciphertext should fail GCM auth");
        }
    }

    // --- AES-GCM with random nonce (S3 mode) ---

    #[test]
    fn gcm_random_nonce_roundtrip(
        data in proptest::collection::vec(any::<u8>(), 0..65536),
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        let key: [u8; 32] = key.try_into().expect("key len");
        let encrypted = encrypt_gcm_random_nonce(&data, &key).expect("encrypt");
        // Output is nonce (12 bytes) + ciphertext + tag (16 bytes)
        prop_assert_eq!(encrypted.len(), 12 + data.len() + 16);
        let decrypted = decrypt_gcm_random_nonce(&encrypted, &key).expect("decrypt");
        prop_assert_eq!(&data, &decrypted);
    }

    #[test]
    fn gcm_random_nonce_produces_different_ciphertext(
        data in proptest::collection::vec(any::<u8>(), 64..1024),
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        let key: [u8; 32] = key.try_into().expect("key len");
        let enc_a = encrypt_gcm_random_nonce(&data, &key).expect("encrypt a");
        let enc_b = encrypt_gcm_random_nonce(&data, &key).expect("encrypt b");
        // Random nonces mean different ciphertext each time
        prop_assert_ne!(enc_a, enc_b);
    }

    #[test]
    fn gcm_random_nonce_tampered_fails(
        data in proptest::collection::vec(any::<u8>(), 64..1024),
        key in proptest::collection::vec(any::<u8>(), 32..=32),
        tamper_pos in 0..1052usize,
        tamper_byte in any::<u8>(),
    ) {
        let key: [u8; 32] = key.try_into().expect("key len");
        let encrypted = encrypt_gcm_random_nonce(&data, &key).expect("encrypt");
        if tamper_pos < encrypted.len() {
            let mut tampered = encrypted.clone();
            tampered[tamper_pos] ^= tamper_byte | 1;
            let result = decrypt_gcm_random_nonce(&tampered, &key);
            prop_assert!(result.is_err(), "tampered random-nonce ciphertext should fail");
        }
    }

    #[test]
    fn gcm_random_nonce_too_short_fails(
        short_data in proptest::collection::vec(any::<u8>(), 0..12),
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        let key: [u8; 32] = key.try_into().expect("key len");
        let result = decrypt_gcm_random_nonce(&short_data, &key);
        prop_assert!(result.is_err(), "data shorter than nonce should fail");
    }

    // --- AES-CTR (WAL mode, no auth tag) ---

    #[test]
    fn ctr_roundtrip(
        data in proptest::collection::vec(any::<u8>(), 0..65536),
        offset in 0u64..1_000_000,
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        let key: [u8; 32] = key.try_into().expect("key len");
        let encrypted = encrypt_ctr(&data, offset, &key).expect("encrypt");
        // CTR mode: no size overhead
        prop_assert_eq!(encrypted.len(), data.len());
        let decrypted = decrypt_ctr(&encrypted, offset, &key).expect("decrypt");
        prop_assert_eq!(&data, &decrypted);
    }

    #[test]
    fn ctr_different_offsets_produce_different_ciphertext(
        data in proptest::collection::vec(any::<u8>(), 64..1024),
        offset_a in 0u64..1_000_000,
        offset_b in 0u64..1_000_000,
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        prop_assume!(offset_a != offset_b);
        let key: [u8; 32] = key.try_into().expect("key len");
        let enc_a = encrypt_ctr(&data, offset_a, &key).expect("encrypt a");
        let enc_b = encrypt_ctr(&data, offset_b, &key).expect("encrypt b");
        prop_assert_ne!(enc_a, enc_b);
    }

    #[test]
    fn ctr_is_symmetric(
        data in proptest::collection::vec(any::<u8>(), 0..1024),
        offset in 0u64..1_000_000,
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        // CTR mode: encrypt == decrypt (same operation)
        let key: [u8; 32] = key.try_into().expect("key len");
        let enc = encrypt_ctr(&data, offset, &key).expect("encrypt");
        let dec = decrypt_ctr(&data, offset, &key).expect("decrypt via decrypt_ctr");
        prop_assert_eq!(enc, dec, "encrypt and decrypt should be identical in CTR mode");
    }

    #[test]
    fn gcm_deterministic_for_same_page(
        data in proptest::collection::vec(any::<u8>(), 64..1024),
        page_num in 0u64..1_000_000,
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        // Page-num-derived nonce is deterministic: same inputs = same output
        let key: [u8; 32] = key.try_into().expect("key len");
        let enc_a = encrypt_gcm(&data, page_num, &key).expect("encrypt a");
        let enc_b = encrypt_gcm(&data, page_num, &key).expect("encrypt b");
        prop_assert_eq!(enc_a, enc_b);
    }
}
