//! Helm releases, read the way `helm list` does: from the `owner=helm` Secrets in each namespace.
//! Each revision is one Secret whose `release` value is base64(gzip(JSON)); only the newest revision
//! of each release is decoded.

use crate::tree_view::Source;
use base64::{Engine, engine::general_purpose::STANDARD};
use flate2::read::GzDecoder;
use k8s_openapi::{api::core::v1::Secret, jiff, serde_json::Value};
use kube::{Api, Client, ResourceExt, api::ListParams};
use ratatui::{
    Frame,
    crossterm::event::{KeyCode, KeyEvent},
    layout::{Constraint, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Cell, Row, Table, TableState},
};
use std::{
    collections::HashMap,
    io::Read,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Release {
    pub name: String,
    pub ns: String,
    pub revision: u32,
    pub status: String,
    pub chart: String, // name-version
    pub app_version: String,
    pub updated: i64, // seconds since epoch
    pub manifest: String,
    pub values: Value, // what the user supplied (helm -f / --set)
}

/// Payload of a release Secret to JSON: undo the Secret's own base64 (already done by the API types),
/// Helm's base64, then gzip.
pub fn decode(raw: &[u8]) -> Result<Value, String> {
    let packed = STANDARD.decode(raw).map_err(|e| format!("base64: {e}"))?;
    let json = if packed.starts_with(&[0x1f, 0x8b]) {
        let mut out = Vec::new();
        GzDecoder::new(packed.as_slice())
            .read_to_end(&mut out)
            .map_err(|e| format!("gzip: {e}"))?;
        out
    } else {
        packed
    };
    k8s_openapi::serde_json::from_slice(&json).map_err(|e| format!("json: {e}"))
}

fn text(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn release_from(secret: &Secret) -> Option<Release> {
    let raw = &secret.data.as_ref()?.get("release")?.0;
    let r = decode(raw).ok()?;
    let meta = &r["chart"]["metadata"];
    let updated = r["info"]["last_deployed"]
        .as_str()
        .and_then(|t| t.parse::<jiff::Timestamp>().ok())
        .map(|t| t.as_second());
    Some(Release {
        name: text(&r["name"]),
        ns: secret.namespace().unwrap_or_default(),
        revision: r["version"].as_u64().unwrap_or(0) as u32,
        status: text(&r["info"]["status"]),
        chart: format!("{}-{}", text(&meta["name"]), text(&meta["version"])),
        app_version: text(&meta["appVersion"]),
        updated: updated.unwrap_or_else(|| crate::created_secs(&secret.metadata)),
        manifest: text(&r["manifest"]),
        values: r["config"].clone(),
    })
}

fn revision_label(s: &Secret) -> u32 {
    s.labels()
        .get("version")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// The newest revision of each release (by the `version` label), decoded, sorted by namespace and name.
pub fn latest(secrets: &[Secret]) -> Vec<Release> {
    let mut newest: HashMap<(String, String), &Secret> = HashMap::new();
    for s in secrets {
        let key = (
            s.namespace().unwrap_or_default(),
            s.labels().get("name").cloned().unwrap_or_default(),
        );
        if newest
            .get(&key)
            .is_none_or(|old| revision_label(old) < revision_label(s))
        {
            newest.insert(key, s);
        }
    }
    let mut out: Vec<Release> = newest.values().filter_map(|s| release_from(s)).collect();
    out.sort_by(|a, b| (&a.ns, &a.name).cmp(&(&b.ns, &b.name)));
    out
}

#[derive(Default)]
struct Snapshot {
    ready: bool,
    error: Option<String>,
    releases: Vec<Release>,
}

fn status_color(s: &str) -> Color {
    match s {
        "deployed" => Color::Green,
        "failed" => Color::Red,
        "superseded" | "uninstalled" => Color::DarkGray,
        _ => Color::Yellow, // pending-install, pending-upgrade, uninstalling...
    }
}

pub enum Act {
    Close,
    Quit,
    Command,
    Tree(Source),
    Values(String, Value), // release name, the user-supplied values as JSON
}

pub struct HelmView {
    snap: Arc<Mutex<Snapshot>>,
    task: tokio::task::JoinHandle<()>,
    table: TableState,
    selected: Option<(String, String)>, // (namespace, name): the highlight survives a refresh
}

impl Drop for HelmView {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl HelmView {
    pub fn open(rt: &tokio::runtime::Handle, client: &Client) -> Self {
        let snap = Arc::new(Mutex::new(Snapshot::default()));
        let (out, client) = (snap.clone(), client.clone());
        let task = rt.spawn(async move {
            loop {
                let found = Api::<Secret>::all(client.clone())
                    .list(&ListParams::default().labels("owner=helm"))
                    .await;
                *out.lock().unwrap() = match found {
                    // decoding gunzips every newest revision: keep it off the UI thread
                    Ok(list) => Snapshot {
                        ready: true,
                        error: None,
                        releases: tokio::task::spawn_blocking(move || latest(&list.items))
                            .await
                            .unwrap_or_default(),
                    },
                    Err(e) => Snapshot {
                        ready: true,
                        error: Some(e.to_string()),
                        releases: vec![],
                    },
                };
                tokio::time::sleep(Duration::from_secs(crate::POLL_SECS)).await;
            }
        });
        Self {
            snap,
            task,
            table: TableState::default(),
            selected: None,
        }
    }

    fn index(&self, rels: &[Release]) -> usize {
        self.selected
            .as_ref()
            .and_then(|(ns, n)| rels.iter().position(|r| &r.ns == ns && &r.name == n))
            .unwrap_or(0)
    }

    fn step(&mut self, delta: isize) {
        let rels = self.snap.lock().unwrap().releases.clone();
        let to = (self.index(&rels) as isize + delta)
            .clamp(0, rels.len().saturating_sub(1) as isize) as usize;
        self.selected = rels.get(to).map(|r| (r.ns.clone(), r.name.clone()));
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Option<Act> {
        match key.code {
            KeyCode::Esc => return Some(Act::Close),
            KeyCode::Char('q') => return Some(Act::Quit),
            KeyCode::Char(':') => return Some(Act::Command),
            KeyCode::Char('j') | KeyCode::Down => self.step(1),
            KeyCode::Char('k') | KeyCode::Up => self.step(-1),
            KeyCode::Char('g') => self.step(isize::MIN / 2),
            KeyCode::Char('G') => self.step(isize::MAX / 2),
            KeyCode::Enter | KeyCode::Char('y') => {
                let rels = self.snap.lock().unwrap().releases.clone();
                let r = rels.get(self.index(&rels))?;
                return Some(if key.code == KeyCode::Enter {
                    Act::Tree(Source::Helm {
                        ns: r.ns.clone(),
                        name: r.name.clone(),
                        chart: r.chart.clone(),
                        status: r.status.clone(),
                        revision: r.revision,
                        manifest: r.manifest.clone(),
                    })
                } else {
                    Act::Values(format!("{}/{} values", r.ns, r.name), r.values.clone())
                });
            }
            _ => {}
        }
        None
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, status: &str) {
        let dim = Style::new().fg(Color::DarkGray);
        let (releases, error, ready) = {
            let s = self.snap.lock().unwrap();
            (s.releases.clone(), s.error.clone(), s.ready)
        };
        if !ready {
            return crate::spinner::render(frame, area, "loading helm releases…");
        }
        let now = jiff::Timestamp::now().as_second();
        let rows: Vec<Row> = releases
            .iter()
            .map(|r| {
                Row::new(vec![
                    Cell::from(r.name.clone()),
                    Cell::from(r.ns.clone()),
                    Cell::from(r.revision.to_string()),
                    Cell::from(Span::styled(
                        r.status.clone(),
                        Style::new().fg(status_color(&r.status)),
                    )),
                    Cell::from(r.chart.clone()),
                    Cell::from(r.app_version.clone()),
                    Cell::from(crate::format_age(now - r.updated)),
                ])
            })
            .collect();
        let widths = [
            Constraint::Fill(2),
            Constraint::Fill(1),
            Constraint::Length(4),
            Constraint::Length(16),
            Constraint::Fill(2),
            Constraint::Length(14),
            Constraint::Length(7),
        ];
        let header = Row::new([
            "NAME",
            "NAMESPACE",
            "REV",
            "STATUS",
            "CHART",
            "APP VERSION",
            "UPDATED",
        ])
        .style(dim);
        let mut title = format!(" Helm releases ({}) ", releases.len());
        if let Some(e) = &error {
            title = format!(" Helm releases · cannot list secrets: {e} ");
        }
        let mut block = Block::bordered().border_style(dim).title(title);
        if !status.is_empty() {
            block = block.title_bottom(Line::from(format!(" {status} ")).right_aligned());
        }
        let at = self.index(&releases);
        self.selected = releases.get(at).map(|r| (r.ns.clone(), r.name.clone()));
        self.table.select((!releases.is_empty()).then_some(at));
        let table = Table::new(rows, widths)
            .header(header)
            .block(block)
            .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED));
        frame.render_stateful_widget(table, area, &mut self.table);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;

    /// What Helm stores: base64(gzip(json)), then the API base64s the Secret value once more.
    fn secret(ns: &str, name: &str, version: u32, status: &str, chart_version: &str) -> Secret {
        let json = format!(
            r#"{{"name": "{name}", "version": {version}, "manifest": "kind: Service\nmetadata:\n  name: web\n", "config": {{"replicas": 2}},
                "info": {{"status": "{status}", "last_deployed": "2026-01-01T00:00:00Z"}},
                "chart": {{"metadata": {{"name": "shop", "version": "{chart_version}", "appVersion": "9.9"}}}}}}"#
        );
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(json.as_bytes()).unwrap();
        let inner = STANDARD.encode(gz.finish().unwrap());
        let outer = STANDARD.encode(inner);
        serde_yaml_ng::from_str(&format!(
            r#"{{"metadata": {{"name": "sh.helm.release.v1.{name}.v{version}", "namespace": "{ns}", "labels": {{"owner": "helm", "name": "{name}", "version": "{version}", "status": "{status}"}}}},
                "type": "helm.sh/release.v1", "data": {{"release": "{outer}"}}}}"#
        ))
        .unwrap()
    }

    #[test]
    fn decodes_the_release_payload_and_keeps_only_the_newest_revision() {
        let secrets = vec![
            secret("shop", "shop", 1, "superseded", "1.0.0"),
            secret("shop", "shop", 3, "deployed", "1.2.0"),
            secret("shop", "shop", 2, "superseded", "1.1.0"),
            secret("other", "db", 1, "failed", "0.1.0"),
        ];
        let rels = latest(&secrets);
        assert_eq!(rels.len(), 2);
        let shop = rels.iter().find(|r| r.name == "shop").unwrap();
        assert_eq!(
            (
                shop.revision,
                shop.status.as_str(),
                shop.chart.as_str(),
                shop.app_version.as_str()
            ),
            (3, "deployed", "shop-1.2.0", "9.9")
        );
        assert_eq!(shop.updated, 1767225600);
        assert!(shop.manifest.contains("kind: Service"));
        assert_eq!(shop.values["replicas"], 2);
        assert_eq!(rels[0].ns, "other"); // sorted by namespace
        assert!(decode(b"!!not base64").is_err());
    }
}
