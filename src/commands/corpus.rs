use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct CorpusManifest {
    pub title: String,
    #[serde(default)]
    pub game_system: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub group: Vec<CorpusGroup>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct CorpusGroup {
    pub name: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub confidence: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub pcgen_paths: Vec<String>,
    #[serde(default)]
    pub herolab_paths: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorpusSide {
    Pcgen,
    Herolab,
}

pub fn load_corpus_paths(
    manifest_path: &Path,
    side: CorpusSide,
    selected_groups: &[String],
) -> Result<Vec<PathBuf>, String> {
    let raw = fs::read_to_string(manifest_path).map_err(|e| {
        format!(
            "failed to read corpus manifest {}: {e}",
            manifest_path.display()
        )
    })?;
    let manifest: CorpusManifest = toml::from_str(&raw).map_err(|e| {
        format!(
            "failed to parse corpus manifest {}: {e}",
            manifest_path.display()
        )
    })?;

    let groups = select_groups(&manifest, selected_groups)?;
    let workspace_root = workspace_root();
    let mut seen = BTreeSet::new();
    let mut paths = Vec::new();

    for group in groups {
        let entries = match side {
            CorpusSide::Pcgen => &group.pcgen_paths,
            CorpusSide::Herolab => &group.herolab_paths,
        };
        for entry in entries {
            let resolved = workspace_root.join(entry);
            if seen.insert(resolved.clone()) {
                paths.push(resolved);
            }
        }
    }

    if paths.is_empty() {
        let side_name = match side {
            CorpusSide::Pcgen => "PCGen",
            CorpusSide::Herolab => "HeroLab",
        };
        return Err(format!(
            "corpus manifest {} produced no {side_name} paths",
            manifest_path.display()
        ));
    }

    Ok(paths)
}

fn select_groups<'a>(
    manifest: &'a CorpusManifest,
    selected_groups: &[String],
) -> Result<Vec<&'a CorpusGroup>, String> {
    if selected_groups.is_empty() {
        return Ok(manifest.group.iter().collect());
    }

    let mut out = Vec::new();
    let mut missing = Vec::new();
    for wanted in selected_groups {
        if let Some(group) = manifest
            .group
            .iter()
            .find(|group| group.name.eq_ignore_ascii_case(wanted))
        {
            out.push(group);
        } else {
            missing.push(wanted.clone());
        }
    }

    if !missing.is_empty() {
        return Err(format!("unknown corpus group(s): {}", missing.join(", ")));
    }

    Ok(out)
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(4)
        .expect("artisan workspace root")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_manifest() -> CorpusManifest {
        CorpusManifest {
            title: "PF1".to_string(),
            game_system: Some("pathfinder".to_string()),
            created: None,
            notes: None,
            group: vec![
                CorpusGroup {
                    name: "One".to_string(),
                    kind: None,
                    confidence: None,
                    notes: None,
                    pcgen_paths: vec!["externals/A/a.lst".to_string()],
                    herolab_paths: vec!["externals/B/a.user".to_string()],
                },
                CorpusGroup {
                    name: "Two".to_string(),
                    kind: None,
                    confidence: None,
                    notes: None,
                    pcgen_paths: vec![
                        "externals/A/a.lst".to_string(),
                        "externals/A/b.lst".to_string(),
                    ],
                    herolab_paths: vec!["externals/B/b.user".to_string()],
                },
            ],
        }
    }

    #[test]
    fn select_groups_uses_all_groups_by_default() {
        let manifest = sample_manifest();
        let selected = select_groups(&manifest, &[]).expect("select groups");
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn select_groups_matches_case_insensitively() {
        let manifest = sample_manifest();
        let selected = select_groups(&manifest, &[String::from("two")]).expect("select groups");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "Two");
    }

    #[test]
    fn select_groups_reports_missing_names() {
        let manifest = sample_manifest();
        let err = select_groups(&manifest, &[String::from("missing")]).unwrap_err();
        assert!(err.contains("missing"));
    }
}
