//! Choices remembered between runs, in `$HOME/.config/kolombo/state` (YAML).

use crate::{Column, ListView};
use serde::{Deserialize, Serialize};
use std::{fs, io, path::{Path, PathBuf}};

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug, Default)]
#[serde(rename_all = "lowercase")]
pub enum Resource {
    #[default]
    Pods,
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
    pub pods: Sort,
}

/// `#[serde(default)]` fills any missing field, so an old or hand-edited file still loads.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
#[serde(default)]
pub struct State {
    /// What the list level below the namespaces shows.
    pub resource: Resource,
    /// Namespace whose `resource` list was open; absent = the namespace list was open.
    pub namespace: Option<String>,
    /// The all-namespaces list was open (a namespace could be called anything, so not a magic name).
    pub all_namespaces: bool,
    pub sort: Sorts,
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
    fn creates_missing_file_and_round_trips() {
        let dir = std::env::temp_dir().join(format!("kolombo-state-test-{}", std::process::id()));
        let path = dir.join("nested/state");
        let (state, note) = State::load(Some(&path));
        assert!(path.exists() && note.is_empty() && state == State::default());

        let saved = State {
            namespace: Some("kube-system".into()),
            sort: Sorts { pods: Sort { column: Column::Age, ascending: false }, ..Default::default() },
            ..Default::default()
        };
        saved.save(&path).unwrap();
        assert_eq!(State::load(Some(&path)).0, saved);

        // a partial or damaged file never stops the app from starting
        fs::write(&path, "namespace: x\n").unwrap();
        assert_eq!(State::load(Some(&path)).0.namespace.as_deref(), Some("x"));
        fs::write(&path, "sort: [oops").unwrap();
        let (fallback, warning) = State::load(Some(&path));
        assert_eq!(fallback, State::default());
        assert!(warning.contains("ignored"));
        fs::remove_dir_all(dir).unwrap();
    }
}
