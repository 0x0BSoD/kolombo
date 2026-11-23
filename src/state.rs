//! Choices remembered between runs, in `$HOME/.config/kolombo/state` (YAML).

use crate::{Column, ListView, resources::{self, ResourceInfo}};
use serde::{Deserialize, Serialize};
use std::{fs, io, path::{Path, PathBuf}};

/// Which list was on screen.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug, Default)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    #[default]
    Namespaces,
    Resource,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
pub struct Sort {
    pub column: Column,
    pub ascending: bool,
}

impl Default for Sort {
    fn default() -> Self {
        Self { column: Column::Name, ascending: true }
    }
}

impl Sort {
    pub fn of(list: &ListView) -> Self {
        Self { column: list.sort_col, ascending: list.ascending }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug, Default)]
#[serde(default)]
pub struct Sorts {
    pub namespaces: Sort,
    /// Order of whichever resource list is open (pods, services, a CRD...).
    #[serde(alias = "pods")]
    pub resource: Sort,
}

/// `#[serde(default)]` fills any missing field, so an old or hand-edited file still loads.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
#[serde(default)]
pub struct State {
    /// The list that was open: the namespaces, or `resource`.
    pub open: Level,
    /// The resource type of the list below the namespaces, as API discovery described it:
    /// enough to open it again without asking the cluster (group/version/kind/name/scope).
    pub resource: ResourceInfo,
    /// Namespace the resource list was limited to; absent = all namespaces (or a cluster-scoped type).
    pub namespace: Option<String>,
    pub sort: Sorts,
}

impl Default for State {
    fn default() -> Self {
        let pods = resources::builtin().into_iter().find(|r| r.name == "pods").unwrap_or_default();
        Self { open: Level::default(), resource: pods, namespace: None, sort: Sorts::default() }
    }
}

pub fn default_path() -> Option<PathBuf> {
    Some(PathBuf::from(std::env::var_os("HOME")?).join(".config/kolombo/state"))
}

impl State {
    /// Reads the file, creating it (and its directory) with defaults when missing.
    /// The String is a warning for the status line, empty when all is well.
    pub fn load(path: Option<&Path>) -> (State, String) {
        let Some(path) = path else { return (State::default(), "state: $HOME is not set, nothing is saved".into()) };
        match fs::read_to_string(path) {
            Ok(text) => match serde_yaml_ng::from_str(&text) {
                Ok(state) => (state, String::new()),
                Err(e) => (State::default(), format!("state file ignored, it will be overwritten: {e}")),
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let state = State::default();
                let note = state.save(path).err().map(|e| format!("state not created: {e}")).unwrap_or_default();
                (state, note)
            }
            Err(e) => (State::default(), format!("state not read: {e}")),
        }
    }

    /// Writes a temp file and renames it over the real one, so a crash can't leave half a file.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, serde_yaml_ng::to_string(self).map_err(io::Error::other)?)?;
        fs::rename(tmp, path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_schema_describes_the_resource_for_reopening() {
        let mut state = State::default();
        state.open = Level::Resource;
        state.resource = ResourceInfo {
            name: "widgets".into(),
            kind: "Widget".into(),
            group: "example.com".into(),
            version: "v1alpha1".into(),
            namespaced: true,
            ..Default::default()
        };
        state.namespace = Some("team-a".into());
        let yaml = serde_yaml_ng::to_string(&state).unwrap();
        println!("{yaml}");
        for want in ["open: resource", "group: example.com", "version: v1alpha1", "kind: Widget", "name: widgets", "namespace: team-a"] {
            assert!(yaml.contains(want), "missing {want}");
        }
        assert_eq!(serde_yaml_ng::from_str::<State>(&yaml).unwrap(), state);
    }

    #[test]
    fn creates_missing_file_and_round_trips() {
        let dir = std::env::temp_dir().join(format!("kolombo-state-test-{}", std::process::id()));
        let path = dir.join("nested/state");
        let (state, note) = State::load(Some(&path));
        assert!(path.exists() && note.is_empty() && state == State::default());

        let saved = State {
            namespace: Some("kube-system".into()),
            open: Level::Resource,
            sort: Sorts { resource: Sort { column: Column::Age, ascending: false }, ..Default::default() },
            ..Default::default()
        };
        saved.save(&path).unwrap();
        assert_eq!(State::load(Some(&path)).0, saved);

        // a partial or damaged file never stops the app from starting
        fs::write(&path, "namespace: x\nsort:\n  pods: { column: age, ascending: false }\n").unwrap();
        let (old, _) = State::load(Some(&path));
        assert_eq!(old.namespace.as_deref(), Some("x"));
        assert_eq!(old.sort.resource.column, Column::Age); // `pods:` is the old name of `resource:`
        assert_eq!(old.resource.name, "pods");
        fs::write(&path, "sort: [oops").unwrap();
        let (fallback, warning) = State::load(Some(&path));
        assert_eq!(fallback, State::default());
        assert!(warning.contains("ignored"));
        fs::remove_dir_all(dir).unwrap();
    }
}
