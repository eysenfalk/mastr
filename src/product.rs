//! Mastr fork policy. Existing upstream download implementations are retained for
//! mergeability, but cannot run until a Mastr release source is implemented.

pub(crate) fn require_release_source() -> Result<(), String> {
    Err("Mastr self-updates and release downloads are disabled: no Mastr release source is configured. Build locally; for a different remote platform provide MASTR_REMOTE_BINARY.".to_owned())
}

#[cfg(test)]
mod tests {
    #[test]
    fn upstream_release_downloads_are_disabled() {
        assert!(super::require_release_source()
            .unwrap_err()
            .contains("disabled"));
    }
}
