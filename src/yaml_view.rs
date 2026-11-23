//! Read-only YAML pane: syntax highlight, folding, vim motions, line selection + copy.

use k8s_openapi::api::core::v1::Namespace;
use ratatui::{
    Frame,
    layout::Rect,
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Paragraph},
};
use serde_yaml_ng::Value;
use std::collections::HashSet;

/// metadata keys the API server fills in; hidden until `s`.
const SERVICE_METADATA: [&str; 6] =
    ["managedFields", "resourceVersion", "uid", "generation", "creationTimestamp", "selfLink"];
const LAST_APPLIED: &str = "kubectl.kubernetes.io/last-applied-configuration";

/// What the main loop must do after a key.
pub enum Outcome {
    Close,
    Copy(String),
}

pub struct YamlView {
    ns: Namespace,
    show_all: bool,
    lines: Vec<String>,
    folded: HashSet<usize>, // indexes into `lines` of collapsed blocks
    cursor: usize,          // position in visible(), not in `lines`
    scroll: usize,
    height: usize,         // body rows, learned at render time
    anchor: Option<usize>, // line index where visual selection started
}

fn indent(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// serde_yaml writes list dashes at the same indent as their key, so a key line
/// owns following lines that are deeper, or same-indent "- " items.
fn block_end(lines: &[String], i: usize) -> usize {
    let ind = indent(&lines[i]);
    let is_key = lines[i].ends_with(':');
    let mut j = i + 1;
    while j < lines.len() {
        let d = indent(&lines[j]);
        if d > ind || (is_key && d == ind && lines[j].trim_start().starts_with("- ")) {
            j += 1;
        } else {
            break;
        }
    }
    j
}

fn to_lines(ns: &Namespace, show_all: bool) -> Vec<String> {
    let mut v = serde_yaml_ng::to_value(ns).unwrap_or_default();
    if !show_all {
        if let Some(meta) = v.get_mut("metadata").and_then(Value::as_mapping_mut) {
            for k in SERVICE_METADATA {
                meta.remove(k);
            }
            let empty = meta
                .get_mut("annotations")
                .and_then(Value::as_mapping_mut)
                .is_some_and(|a| {
                    a.remove(LAST_APPLIED);
                    a.is_empty()
                });
            if empty {
                meta.remove("annotations");
            }
        }
    }
    serde_yaml_ng::to_string(&v).unwrap_or_default().lines().map(String::from).collect()
}

/// `key: value` -> (key, value). None for plain/quoted scalars and list items.
fn split_key(s: &str) -> Option<(&str, &str)> {
    if s.starts_with(['"', '\'']) {
        return None;
    }
    if let Some(k) = s.strip_suffix(':') {
        return Some((k, ""));
    }
    s.split_once(": ")
}

fn value_style(v: &str) -> Style {
    let color = match v {
        "true" | "false" => Color::Magenta,
        "null" | "~" => Color::DarkGray,
        _ if v.starts_with(|c: char| c.is_ascii_digit() || c == '-') && v.parse::<f64>().is_ok() => Color::Yellow,
        _ => Color::Green,
    };
    Style::new().fg(color)
}

fn highlight(line: &str) -> Vec<Span<'static>> {
    let dim = Style::new().fg(Color::DarkGray);
    let trimmed = line.trim_start();
    let mut spans = vec![Span::raw(line[..line.len() - trimmed.len()].to_string())];
    let mut rest = trimmed;
    while let Some(r) = rest.strip_prefix("- ") {
        spans.push(Span::styled("- ", dim));
        rest = r;
    }
    match split_key(rest) {
        Some((key, val)) => {
            spans.push(Span::styled(format!("{key}:"), Style::new().fg(Color::Cyan)));
            if !val.is_empty() {
                spans.push(Span::raw(" "));
                spans.push(Span::styled(val.to_string(), value_style(val)));
            }
        }
        None => spans.push(Span::styled(rest.to_string(), value_style(rest))),
    }
    spans
}

impl YamlView {
    pub fn new(ns: Namespace) -> Self {
        let lines = to_lines(&ns, false);
        Self { ns, show_all: false, lines, folded: HashSet::new(), cursor: 0, scroll: 0, height: 0, anchor: None }
    }

    /// Indexes of lines on screen: children of folded blocks are skipped.
    fn visible(&self) -> Vec<usize> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < self.lines.len() {
            out.push(i);
            i = if self.folded.contains(&i) { block_end(&self.lines, i) } else { i + 1 };
        }
        out
    }

    /// Line range [lo, end) the selection (or just the cursor line) covers.
    fn range(&self, vis: &[usize]) -> (usize, usize) {
        let c = vis.get(self.cursor).copied().unwrap_or(0);
        let a = self.anchor.unwrap_or(c);
        let (lo, hi) = (a.min(c), a.max(c));
        let end = if self.folded.contains(&hi) { block_end(&self.lines, hi) } else { hi + 1 };
        (lo, end.min(self.lines.len()))
    }

    pub fn name(&self) -> String {
        self.ns.metadata.name.clone().unwrap_or_default()
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, message: &str) {
        self.height = area.height.saturating_sub(2).max(1) as usize;
        let vis = self.visible();
        self.cursor = self.cursor.min(vis.len().saturating_sub(1));
        if self.cursor < self.scroll {
            self.scroll = self.cursor;
        } else if self.cursor >= self.scroll + self.height {
            self.scroll = self.cursor + 1 - self.height;
        }
        let (lo, end) = self.range(&vis);
        let dim = Style::new().fg(Color::DarkGray);

        let rows: Vec<Line> = vis
            .iter()
            .enumerate()
            .skip(self.scroll)
            .take(self.height)
            .map(|(pos, &i)| {
                let foldable = block_end(&self.lines, i) > i + 1;
                let folded = foldable && self.folded.contains(&i);
                let marker = match (foldable, folded) {
                    (true, true) => "▸ ",
                    (true, false) => "▾ ",
                    _ => "  ",
                };
                let mut spans = vec![Span::styled(marker, dim)];
                spans.extend(highlight(&self.lines[i]));
                if folded {
                    spans.push(Span::styled(format!(" … {} lines", block_end(&self.lines, i) - i - 1), dim));
                }
                let mut line = Line::from(spans);
                if pos == self.cursor {
                    line = line.style(Style::new().bg(Color::Rgb(69, 71, 90)));
                } else if self.anchor.is_some() && (lo..end).contains(&i) {
                    line = line.style(Style::new().bg(Color::Rgb(49, 50, 68)));
                }
                line
            })
            .collect();

        let bold = Style::new().add_modifier(Modifier::BOLD);
        // Mode tags sit top-right; breadcrumbs and key help live in the app header.
        let mut tags = Vec::new();
        if self.anchor.is_some() {
            tags.push(Span::styled(" -- VISUAL -- ", bold.fg(Color::Yellow)));
        }
        if !self.show_all {
            tags.push(Span::styled(" service fields hidden ", dim));
        }
        frame.render_widget(
            Paragraph::new(rows).block(
                Block::bordered()
                    .title(" YAML ")
                    .title(Line::from(tags).right_aligned())
                    .title_bottom(format!(" {message} ")),
            ),
            area,
        );
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Option<Outcome> {
        let vis = self.visible();
        let last = vis.len().saturating_sub(1);
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let half = (self.height / 2).max(1);
        match key.code {
            KeyCode::Char('d') if ctrl => self.cursor += half,
            KeyCode::Char('u') if ctrl => self.cursor = self.cursor.saturating_sub(half),
            KeyCode::Char('j') | KeyCode::Down => self.cursor += 1,
            KeyCode::Char('k') | KeyCode::Up => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Char('g') => self.cursor = 0,
            KeyCode::Char('G') => self.cursor = last,
            KeyCode::Char(' ') => {
                let i = vis[self.cursor.min(last)];
                if block_end(&self.lines, i) > i + 1 && !self.folded.remove(&i) {
                    self.folded.insert(i);
                }
            }
            KeyCode::Char('s') => {
                self.show_all = !self.show_all;
                self.lines = to_lines(&self.ns, self.show_all);
                self.folded.clear(); // indexes no longer line up
                self.anchor = None;
                self.cursor = 0;
                self.scroll = 0;
            }
            KeyCode::Char('v' | 'V') => {
                self.anchor = if self.anchor.is_some() { None } else { vis.get(self.cursor).copied() };
            }
            KeyCode::Char('y') => {
                let (lo, end) = self.range(&vis);
                self.anchor = None;
                return Some(Outcome::Copy(self.lines[lo..end].join("\n")));
            }
            KeyCode::Char('Y') => return Some(Outcome::Copy(self.lines.join("\n"))),
            KeyCode::Esc if self.anchor.is_some() => self.anchor = None,
            KeyCode::Esc | KeyCode::Char('q') => return Some(Outcome::Close),
            _ => {}
        }
        self.cursor = self.cursor.min(self.visible().len().saturating_sub(1));
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folds_keys_with_same_indent_list_items() {
        let l: Vec<String> = ["a:", "  b: 1", "c:", "- x", "- y", "d: 2"].map(String::from).to_vec();
        assert_eq!(block_end(&l, 0), 2);
        assert_eq!(block_end(&l, 2), 5);
        assert_eq!(block_end(&l, 5), 6);
    }

    #[test]
    fn hides_service_fields_until_show_all() {
        let mut ns = Namespace::default();
        ns.metadata.name = Some("x".into());
        ns.metadata.uid = Some("u-1".into());
        let hidden = to_lines(&ns, false).join("\n");
        assert!(!hidden.contains("uid") && hidden.contains("name: x"));
        assert!(to_lines(&ns, true).join("\n").contains("uid: u-1"));
    }
}
