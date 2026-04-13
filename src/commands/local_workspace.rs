use std::{
    fs,
    path::{Path, PathBuf},
};

pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("workspace root")
        .to_path_buf()
}

pub fn local_cli_root() -> PathBuf {
    workspace_root()
        .join("code")
        .join("rust")
        .join("apps")
        .join("artisan-cli")
        .join("local")
}

pub fn local_reconciliation_root() -> PathBuf {
    local_cli_root().join("reconciliation")
}

pub fn default_local_core_catalog_path() -> PathBuf {
    local_reconciliation_root().join("canonical_catalog.toml")
}

pub fn resolve_existing_core_catalog_path(explicit: Option<&PathBuf>) -> Option<PathBuf> {
    if let Some(path) = explicit {
        return Some(path.clone());
    }

    let default = default_local_core_catalog_path();
    default.exists().then_some(default)
}

pub fn resolve_output_core_catalog_path(explicit: Option<&PathBuf>) -> PathBuf {
    explicit
        .cloned()
        .unwrap_or_else(default_local_core_catalog_path)
}

pub fn ensure_parent_dir(path: &Path) -> Result<(), String> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };

    fs::create_dir_all(parent)
        .map_err(|e| format!("failed to create directory {}: {e}", parent.display()))
}

#[cfg(test)]
mod tests {
    use super::{
        default_local_core_catalog_path, local_cli_root, resolve_existing_core_catalog_path,
        resolve_output_core_catalog_path,
    };
    use std::path::PathBuf;

    #[test]
    fn explicit_input_path_wins() {
        let explicit = PathBuf::from("/tmp/example.toml");
        assert_eq!(
            resolve_existing_core_catalog_path(Some(&explicit)),
            Some(explicit.clone())
        );
        assert_eq!(resolve_output_core_catalog_path(Some(&explicit)), explicit);
    }

    #[test]
    fn default_output_path_lives_under_ignored_local_dir() {
        let default = default_local_core_catalog_path();
        assert!(default.starts_with(local_cli_root()));
        assert!(default.ends_with("canonical_catalog.toml"));
    }
}
