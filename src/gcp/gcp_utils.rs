/// Given unix path, returns the bucket path without trailing separator
pub fn to_bucket_path(race_id: &str, nested_bucket_path: &str) -> String {
    let s = nested_bucket_path
        .strip_prefix("/")
        .unwrap_or(nested_bucket_path);
    format!("{}/{}", race_id, s.strip_suffix("/").unwrap_or(s))
}
