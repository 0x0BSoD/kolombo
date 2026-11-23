mod yaml_view;

use fuzzy_matcher::{FuzzyMatcher, skim::SkimMatcherV2};
use k8s_openapi::{api::core::v1::Namespace, jiff};
use futures::StreamExt;
use kube::{
    Api, Client,
    api::PostParams,
    runtime::{WatchStreamExt, reflector, reflector::Store, watcher},
};
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode},
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Cell, Clear, Paragraph, Row, Table, TableState},
};
use yaml_view::{Outcome, YamlView};
use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    process::Command,
    time::Duration,
};

struct Ns {
    name: String,
    phase: String,
    created: i64, // unix seconds
    labels: Vec<(String, String)>, // user labels only, sorted by key
}

/// True for `kubernetes.io/...` and `*.kubernetes.io/...` keys (system-managed).
fn is_system_label(key: &str) -> bool {
    key.split_once('/')
        .is_some_and(|(prefix, _)| prefix == "kubernetes.io" || prefix.ends_with(".kubernetes.io"))
}

/// True for the plain label key `name` (it just repeats the namespace name).
fn is_name_label(key: &str) -> bool {
    key == "name"
}

/// Key/value table for a labels/annotations map, with a count in the title.
fn map_table<'a>(title: &str, map: &'a Option<BTreeMap<String, String>>) -> Table<'a> {
    let dim = Style::new().fg(Color::DarkGray);
    let mut rows: Vec<Row> = map
        .iter()
        .flatten()
        .map(|(k, v)| Row::new([Cell::from(k.as_str()).style(Style::new().fg(Color::Cyan)), Cell::from(v.as_str())]))
        .collect();
    let count = rows.len();
    if rows.is_empty() {
        rows.push(Row::new([Cell::from("<none>").style(dim)]));
    }
    Table::new(rows, [Constraint::Percentage(40), Constraint::Fill(1)])
        .header(Row::new(["KEY", "VALUE"]).style(Style::new().add_modifier(Modifier::BOLD).fg(Color::Yellow)))
        .block(Block::bordered().border_style(dim).title(format!(" {title} ({count}) ")))
}

/// Fullscreen details: breadcrumb frame, info block, labels table, annotations table.
fn render_details(frame: &mut Frame, ns: &Namespace, message: &str) {
    let m = &ns.metadata;
    let name = m.name.clone().unwrap_or_default();
    let phase = ns.status.as_ref().and_then(|s| s.phase.clone()).unwrap_or_default();
    let phase_color = if phase == "Active" { Color::Green } else { Color::Yellow };
    let bold = Style::new().add_modifier(Modifier::BOLD);
    let dim = Style::new().fg(Color::DarkGray);
    let created = m.creation_timestamp.as_ref().map_or(String::new(), |t| {
        format!("{} ({} ago)", t.0, format_age(jiff::Timestamp::now().as_second() - t.0.as_second()))
    });

    // Breadcrumb: Namespaces // <name> // Details
    let crumbs = Line::from(vec![
        Span::styled(" Namespaces ", dim),
        Span::styled("// ", dim),
        Span::styled(name.clone(), bold.fg(Color::Cyan)),
        Span::styled(" // ", dim),
        Span::styled("Details ", bold),
        Span::styled("(Esc back) ", dim),
    ]);
    let outer = Block::bordered().title(crumbs).title_bottom(format!(" {message} "));
    let inner = outer.inner(frame.area());
    frame.render_widget(outer, frame.area());

    let [info, labels, annotations] =
        Layout::vertical([Constraint::Length(6), Constraint::Fill(1), Constraint::Fill(1)]).areas(inner);

    let field = |k: &str, v: Span<'static>| Line::from(vec![Span::styled(format!("{k:<10}"), dim), v]);
    let lines = vec![
        field("Name", Span::styled(name, bold)),
        field("Status", Span::styled(format!("● {phase}"), Style::new().fg(phase_color))),
        field("Created", Span::raw(created)),
        field("UID", Span::raw(m.uid.clone().unwrap_or_default())),
    ];
    frame.render_widget(
        Paragraph::new(lines).block(Block::bordered().border_style(dim).title(" Info ")),
        info,
    );
    frame.render_widget(map_table("Labels", &m.labels), labels);
    frame.render_widget(map_table("Annotations", &m.annotations), annotations);
}

/// One padded, colored "pill". Same text always gets the same color.
fn pill(key: &str, value: &str) -> Span<'static> {
    const PALETTE: [Color; 8] = [
        Color::Rgb(137, 180, 250),
        Color::Rgb(166, 227, 161),
        Color::Rgb(249, 226, 175),
        Color::Rgb(245, 194, 231),
        Color::Rgb(148, 226, 213),
        Color::Rgb(250, 179, 135),
        Color::Rgb(203, 166, 247),
        Color::Rgb(243, 139, 168),
    ];
    let mut h = DefaultHasher::new(); // fixed keys: stable across runs
    key.hash(&mut h);
    let color = PALETTE[(h.finish() % PALETTE.len() as u64) as usize];
    Span::styled(format!(" {key}={value} "), Style::new().bg(color).fg(Color::Black))
}

#[derive(Clone, Copy, PartialEq)]
enum Column {
    Name,
    Status,
    Age,
}

impl Column {
    const ALL: [Column; 3] = [Column::Name, Column::Status, Column::Age];

    fn title(self) -> &'static str {
        match self {
            Column::Name => "NAME",
            Column::Status => "STATUS",
            Column::Age => "AGE",
        }
    }

    /// Step through ALL with wrap-around. `delta` is +1 or -1.
    fn step(self, delta: isize) -> Column {
        let i = Column::ALL.iter().position(|&c| c == self).unwrap() as isize;
        Column::ALL[(i + delta).rem_euclid(Column::ALL.len() as isize) as usize]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Fetch before entering the TUI so errors print on a normal terminal.
    let client = Client::try_default().await?;
    let api: Api<Namespace> = Api::all(client);
    // Informer: the watcher streams events, the reflector mirrors them into `store`.
    let (store, writer) = reflector::store();
    let events = reflector(writer, watcher(api.clone(), watcher::Config::default()))
        .default_backoff() // retry with backoff on errors instead of ending
        .applied_objects();
    // The stream does nothing unless polled, so drive it in a background task.
    tokio::spawn(events.for_each(|_| async {}));

    let rt = tokio::runtime::Handle::current();
    // run() enters raw mode + alternate screen (fullscreen) and restores on exit/panic.
    // The TUI loop is blocking, so it runs on a blocking thread; there it may call rt.block_on.
    tokio::task::spawn_blocking(move || ratatui::run(|terminal| run_app(terminal, &store, &api, &rt))).await??;
    Ok(())
}

/// Put text on the system clipboard. The handle is kept so X11 can still serve the paste.
fn copy(clipboard: &mut Option<arboard::Clipboard>, text: &str) -> anyhow::Result<()> {
    if clipboard.is_none() {
        *clipboard = Some(arboard::Clipboard::new()?);
    }
    clipboard.as_mut().unwrap().set_text(text)?;
    Ok(())
}

/// Open the namespace as YAML in $EDITOR; if changed, send it to the cluster.
/// Returns a one-line message for the UI.
fn edit_ns(
    terminal: &mut DefaultTerminal,
    rt: &tokio::runtime::Handle,
    api: &Api<Namespace>,
    ns: &Namespace,
) -> anyhow::Result<String> {
    let name = ns.metadata.name.clone().unwrap_or_default();
    let mut obj = ns.clone();
    obj.metadata.managed_fields = None; // noise, kubectl hides it too
    let before = serde_yaml_ng::to_string(&obj)?;
    let path = std::env::temp_dir().join(format!("kolombo-{name}.yaml"));
    std::fs::write(&path, &before)?;

    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".into());
    let mut parts = editor.split_whitespace(); // EDITOR may carry args, e.g. "code -w"
    let prog = parts.next().unwrap_or("vi");
    ratatui::restore(); // give the terminal back to the editor
    let status = Command::new(prog).args(parts).arg(&path).status();
    *terminal = ratatui::init(); // and take it again
    let status = status?;
    let after = std::fs::read_to_string(&path);
    let _ = std::fs::remove_file(&path);
    if !status.success() {
        anyhow::bail!("editor exited with {status}");
    }
    let after = after?;
    if after == before {
        return Ok("no changes".into());
    }
    let edited: Namespace = serde_yaml_ng::from_str(&after)?;
    rt.block_on(api.replace(&name, &PostParams::default(), &edited))?;
    Ok(format!("{name} updated"))
}

/// Names matching `query`, best fuzzy score first. Empty query keeps original order.
fn filter<'a>(names: &'a [Ns], query: &str, matcher: &SkimMatcherV2) -> Vec<&'a Ns> {
    if query.is_empty() {
        return names.iter().collect();
    }
    let mut scored: Vec<(i64, &Ns)> = names
        .iter()
        .filter_map(|n| matcher.fuzzy_match(&n.name, query).map(|score| (score, n)))
        .collect();
    scored.sort_by(|a, b| b.0.cmp(&a.0)); // highest score first
    scored.into_iter().map(|(_, n)| n).collect()
}

/// kubectl-style age: biggest unit only (5d, 3h, 12m, 40s).
fn format_age(secs: i64) -> String {
    match secs.max(0) {
        s if s >= 86_400 => format!("{}d", s / 86_400),
        s if s >= 3_600 => format!("{}h", s / 3_600),
        s if s >= 60 => format!("{}m", s / 60),
        s => format!("{s}s"),
    }
}

fn run_app(
    terminal: &mut DefaultTerminal,
    store: &Store<Namespace>,
    api: &Api<Namespace>,
    rt: &tokio::runtime::Handle,
) -> std::io::Result<()> {
    let matcher = SkimMatcherV2::default();
    let mut state = TableState::default().with_selected(Some(0));
    let mut query = String::new();
    let mut typing = false; // true while the search popup is open
    let mut sort_col = Column::Name;
    let mut ascending = true;
    let mut viewing: Option<String> = None; // namespace shown in the details view
    let mut yaml: Option<YamlView> = None; // read-only YAML pane, when open
    let mut clipboard: Option<arboard::Clipboard> = None; // created on first copy, kept alive
    let mut message = String::new(); // result of the last action, cleared on next key

    loop {
        // Snapshot of the informer cache; changes as watch events arrive.
        let names: Vec<Ns> = store
            .state()
            .iter()
            .map(|ns| Ns {
                name: ns.metadata.name.clone().unwrap_or_default(),
                phase: ns.status.as_ref().and_then(|s| s.phase.clone()).unwrap_or_default(),
                created: ns.metadata.creation_timestamp.as_ref().map_or(0, |t| t.0.as_second()),
                labels: ns
                    .metadata
                    .labels
                    .iter()
                    .flatten() // Option<BTreeMap> -> entries (BTreeMap is already key-sorted)
                    .filter(|(k, _)| !is_system_label(k))
                    .filter(|(k, _)| !is_name_label(k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            })
            .collect();
        let mut visible = filter(&names, &query, &matcher);
        // With a query, fuzzy rank wins; otherwise apply the chosen sort.
        if query.is_empty() {
            visible.sort_by(|a, b| match sort_col {
                Column::Name => a.name.cmp(&b.name),
                Column::Status => a.phase.cmp(&b.phase),
                Column::Age => b.created.cmp(&a.created), // newer = younger = "smaller" age
            });
            if !ascending {
                visible.reverse();
            }
        }

        // Details are looked up live each frame; if the namespace vanished, go back to the list.
        let details = viewing.as_ref().and_then(|n| {
            store
                .state()
                .iter()
                .find(|ns| ns.metadata.name.as_ref() == Some(n))
                .cloned()
        });
        if details.is_none() {
            viewing = None;
        }

        terminal.draw(|frame| {
            if let Some(v) = &mut yaml {
                v.render(frame, &message);
                return;
            }
            if let Some(ns) = &details {
                render_details(frame, ns, &message);
                return;
            }
            let title = format!(" Namespaces {}/{} (/ search, d describe, e edit, y yaml, q quit) ", visible.len(), names.len());
            let now = jiff::Timestamp::now().as_second();
            let rows = visible
                .iter()
                .map(|n| {
                    let mut spans = Vec::new();
                    for (k, v) in &n.labels {
                        spans.push(pill(k, v));
                        spans.push(Span::raw(" "));
                    }
                    Row::new([
                        Cell::from(n.name.clone()),
                        Cell::from(n.phase.clone()),
                        Cell::from(format_age(now - n.created)),
                        Cell::from(Line::from(spans)),
                    ])
                });
            let widths = [
                Constraint::Fill(1),
                Constraint::Length(12),
                Constraint::Length(6),
                Constraint::Fill(2),
            ];
            let table = Table::new(rows, widths)
                .header(
                    Row::new(Column::ALL.map(|c| {
                        if c == sort_col && query.is_empty() {
                            format!("{} {}", c.title(), if ascending { "▲" } else { "▼" })
                        } else {
                            c.title().to_string()
                        }
                    })
                    .into_iter()
                    .chain(["LABELS".to_string()]))
                    .style(Style::new().add_modifier(Modifier::BOLD)),
                )
                .block(Block::bordered().title(title).title_bottom(format!(" {message} ")))
                .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED));
            frame.render_stateful_widget(table, frame.area(), &mut state);

            if typing {
                // Floating box: 50% wide, 3 rows tall, centered.
                let popup = frame.area().centered(Constraint::Percentage(50), Constraint::Length(3));
                frame.render_widget(Clear, popup); // wipe what's underneath
                frame.render_widget(
                    Paragraph::new(query.as_str()).block(Block::bordered().title(" Search ")),
                    popup,
                );
                // +1 skips the border; cursor sits after the typed text.
                frame.set_cursor_position((popup.x + 1 + query.chars().count() as u16, popup.y + 1));
            }
        })?;

        // Wait up to 250ms for a key; on timeout loop again so the UI redraws with fresh data.
        if !event::poll(Duration::from_millis(250))? {
            continue;
        }
        let Event::Key(key) = event::read()? else { continue };

        message.clear();
        if let Some(v) = &mut yaml {
            match v.handle_key(key) {
                Some(Outcome::Close) => yaml = None,
                Some(Outcome::Copy(text)) => {
                    message = match copy(&mut clipboard, &text) {
                        Ok(()) => format!("copied {} lines", text.lines().count()),
                        Err(e) => format!("copy failed: {e:#}"),
                    };
                }
                None => {}
            }
            continue;
        }
        let mut edit_target: Option<String> = None;
        let mut yaml_target: Option<String> = None;
        if viewing.is_some() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q' | 'd') => viewing = None,
                KeyCode::Char('e') => edit_target = viewing.clone(),
                KeyCode::Char('y') => yaml_target = viewing.clone(),
                _ => {}
            }
        } else if typing {
            match key.code {
                KeyCode::Char(c) => {
                    query.push(c);
                    state.select_first();
                }
                KeyCode::Backspace => {
                    query.pop();
                    state.select_first();
                }
                KeyCode::Enter => typing = false, // keep filter, go back to navigating
                KeyCode::Esc => {
                    query.clear();
                    typing = false;
                }
                _ => {}
            }
        } else {
            match key.code {
                KeyCode::Char('/') => typing = true,
                KeyCode::Char('>') => sort_col = sort_col.step(1),
                KeyCode::Char('<') => sort_col = sort_col.step(-1),
                KeyCode::Char('A') => ascending = !ascending,
                KeyCode::Char('d') => {
                    viewing = state.selected().and_then(|i| visible.get(i)).map(|n| n.name.clone());
                }
                KeyCode::Char('y') => {
                    yaml_target = state.selected().and_then(|i| visible.get(i)).map(|n| n.name.clone());
                }
                KeyCode::Char('e') => {
                    edit_target = state.selected().and_then(|i| visible.get(i)).map(|n| n.name.clone());
                }
                KeyCode::Char('q') => return Ok(()),
                KeyCode::Esc if query.is_empty() => return Ok(()),
                KeyCode::Esc => query.clear(), // first Esc drops the filter
                KeyCode::Char('j') | KeyCode::Down => state.select_next(),
                KeyCode::Char('k') | KeyCode::Up => state.select_previous(),
                _ => {}
            }
        }

        if let Some(name) = yaml_target {
            let ns = store.state().iter().find(|ns| ns.metadata.name.as_ref() == Some(&name)).cloned();
            yaml = ns.map(|ns| YamlView::new((*ns).clone()));
        }
        if let Some(name) = edit_target {
            let ns = store.state().iter().find(|ns| ns.metadata.name.as_ref() == Some(&name)).cloned();
            if let Some(ns) = ns {
                message = match edit_ns(terminal, rt, api, &ns) {
                    Ok(m) => m,
                    Err(e) => format!("edit failed: {e:#}"),
                };
            }
        }
    }
}
