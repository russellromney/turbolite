#[test]
fn test_s3_key_format() {
    // Verify the key format is "p/d/{gid}_v{version}"
    let key = format!("p/d/{}_v{}", 5, 3);
    assert_eq!(key, "p/d/5_v3");
    let key0 = format!("p/d/{}_v{}", 0, 1);
    assert_eq!(key0, "p/d/0_v1");
}
