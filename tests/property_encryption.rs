//! Siege 7: Encryption property tests.
//!
//! Validates AES-GCM and AES-CTR round-trips, nonce behavior,
//! and tampered ciphertext detection.

#![cfg(feature = "encryption")]

use proptest::prelude::*;
use turbolite::compress::{
    decrypt_ctr, decrypt_gcm_random_nonce, encrypt_ctr, encrypt_gcm_random_nonce,
};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    // The deterministic page-number-nonce GCM (encrypt_gcm/decrypt_gcm) was
    // removed: a rewrite reused the nonce, which under GCM XORs plaintexts and
    // leaks the GHASH auth subkey (tag forgery). All rewritable paths now use a
    // fresh random nonce stored inline (encrypt_gcm_random_nonce).

    // --- AES-GCM with random nonce ---

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

    // --- Page rewrite must NOT reuse a nonce/keystream (F1/F2 regression) ---

    #[test]
    fn gcm_page_rewrite_differs_and_both_decrypt(
        data in proptest::collection::vec(any::<u8>(), 64..4096),
        key in proptest::collection::vec(any::<u8>(), 32..=32),
    ) {
        // Encrypting the SAME page twice (a rewrite of the same logical slot)
        // must produce DIFFERENT ciphertext (fresh random nonce), and BOTH
        // ciphertexts must decrypt back to the original plaintext.
        let key: [u8; 32] = key.try_into().expect("key len");
        let enc1 = encrypt_gcm_random_nonce(&data, &key).expect("encrypt 1");
        let enc2 = encrypt_gcm_random_nonce(&data, &key).expect("encrypt 2");
        prop_assert_ne!(&enc1, &enc2, "page rewrite must not reuse nonce/keystream");
        prop_assert_eq!(decrypt_gcm_random_nonce(&enc1, &key).expect("dec 1"), data.clone());
        prop_assert_eq!(decrypt_gcm_random_nonce(&enc2, &key).expect("dec 2"), data);
    }
}
