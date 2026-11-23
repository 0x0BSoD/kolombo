use fuzzy_matcher::{FuzzyMatcher, skim::SkimMatcherV2};
use k8s_openapi::{api::core::v1::Namespace, jiff};
use futures::StreamExt;
use kube::{
    Api, Client,
    runtime::{WatchStreamExt, reflector, reflector::Store, watcher},
};
use ratatui::{
    DefaultTerminal,
    crossterm::event::{self, Event, KeyCode},
    layout::Constraint,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Cell, Clear, Paragraph, Row, Table, TableState},
};
use std::{
    hash::{DefaultHasher, Hash, Hasher},
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
    let events = reflector(writer, watcher(api, watcher::Config::default()))
        .default_backoff() // retry with backoff on errors instead of ending
        .applied_objects();
    // The stream does nothing unless polled, so drive it in a background task.
    tokio::spawn(events.for_each(|_| async {}));

    // run() enters raw mode + alternate screen (fullscreen) and restores on exit/panic.
    ratatui::run(|terminal| run_app(terminal, &store))?;
    Ok(())
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

fn run_app(terminal: &mut DefaultTerminal, store: &Store<Namespace>) -> std::io::Result<()> {
    let matcher = SkimMatcherV2::default();
    let mut state = TableState::default().with_selected(Some(0));
    let mut query = String::new();
    let mut typing = false; // true while the search popup is open
    let mut sort_col = Column::Name;
    let mut ascending = true;

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

        terminal.draw(|frame| {
            let title = format!(" Namespaces {}/{} (/ search, q quit) ", visible.len(), names.len());
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
                .block(Block::bordered().title(title))
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

        if typing {
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
                KeyCode::Char('q') => return Ok(()),
                KeyCode::Esc if query.is_empty() => return Ok(()),
                KeyCode::Esc => query.clear(), // first Esc drops the filter
                KeyCode::Char('j') | KeyCode::Down => state.select_next(),
                KeyCode::Char('k') | KeyCode::Up => state.select_previous(),
                _ => {}
            }
        }
    }
}
