#[test]
fn test_s3_key_format() {
    // Verify the key format is "pg/{gid}_v{version}"
    let key = format!("pg/{}_v{}", 5, 3);
    assert_eq!(key, "pg/5_v3");
    let key0 = format!("pg/{}_v{}", 0, 1);
    assert_eq!(key0, "pg/0_v1");
}
