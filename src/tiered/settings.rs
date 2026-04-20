//! Small parsing helpers reused by config loaders.

/// Parse a human-readable byte size string into u64 bytes.
/// Supports: "512MB", "2GB", "512M", "2G", "1073741824", "0" (unlimited).
/// Case-insensitive suffixes. Returns None on parse failure.
pub fn parse_byte_size(value: &str) -> Option<u64> {
    let s = value.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    let s_upper = s.to_uppercase();
    let (num_part, multiplier) = if s_upper.ends_with("GB") {
        (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
    } else if s_upper.ends_with("MB") {
        (&s[..s.len() - 2], 1024u64 * 1024)
    } else if s_upper.ends_with("KB") {
        (&s[..s.len() - 2], 1024u64)
    } else if s_upper.ends_with('G') {
        (&s[..s.len() - 1], 1024u64 * 1024 * 1024)
    } else if s_upper.ends_with('M') {
        (&s[..s.len() - 1], 1024u64 * 1024)
    } else if s_upper.ends_with('K') {
        (&s[..s.len() - 1], 1024u64)
    } else {
        return None;
    };
    let num: f64 = num_part.trim().parse().ok()?;
    if num < 0.0 {
        return None;
    }
    Some((num * multiplier as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_byte_size_numeric() {
        assert_eq!(parse_byte_size("0"), Some(0));
        assert_eq!(parse_byte_size("1073741824"), Some(1073741824));
        assert_eq!(parse_byte_size("512"), Some(512));
    }

    #[test]
    fn test_parse_byte_size_suffixes() {
        assert_eq!(parse_byte_size("512MB"), Some(512 * 1024 * 1024));
        assert_eq!(parse_byte_size("512mb"), Some(512 * 1024 * 1024));
        assert_eq!(parse_byte_size("2GB"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_byte_size("2G"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_byte_size("512M"), Some(512 * 1024 * 1024));
        assert_eq!(parse_byte_size("64KB"), Some(64 * 1024));
        assert_eq!(parse_byte_size("64K"), Some(64 * 1024));
    }

    #[test]
    fn test_parse_byte_size_fractional() {
        assert_eq!(parse_byte_size("1.5GB"), Some((1.5 * 1024.0 * 1024.0 * 1024.0) as u64));
        assert_eq!(parse_byte_size("0.5M"), Some((0.5 * 1024.0 * 1024.0) as u64));
    }

    #[test]
    fn test_parse_byte_size_invalid() {
        assert_eq!(parse_byte_size(""), None);
        assert_eq!(parse_byte_size("abc"), None);
        assert_eq!(parse_byte_size("MB"), None);
        assert_eq!(parse_byte_size("-1GB"), None);
    }
}
