mod yaml_view;

use fuzzy_matcher::{FuzzyMatcher, skim::SkimMatcherV2};
use k8s_openapi::{api::core::v1::Namespace, jiff};
use futures::StreamExt;
use kube::{
    Api, Client,
    api::{DeleteParams, PostParams},
    runtime::{WatchStreamExt, reflector, reflector::Store, watcher},
};
use ratatui::{
    DefaultTerminal, Frame,
    crossterm::event::{self, Event, KeyCode, KeyModifiers},
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Cell, Clear, Paragraph, Row, Table, TableState},
};
use yaml_view::{Outcome, YamlView};
use std::{
    collections::BTreeMap,
    hash::{DefaultHasher, Hash, Hasher},
    process::Command,
    sync::{
        Arc, OnceLock,
        mpsc::{self, Sender},
    },
    time::{Duration, Instant},
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

/// What the header shows about the connection.
struct HeaderInfo {
    context: String,
    version: Arc<OnceLock<String>>, // filled by a background task; None until the API answers
}

/// Name of the current kubeconfig context.
fn current_context() -> String {
    kube::config::Kubeconfig::read()
        .ok()
        .and_then(|c| c.current_context)
        .unwrap_or_else(|| "unknown".into())
}

const LIST_HINTS: &[(&str, &str)] = &[
    ("/", "search"),
    ("d", "describe"),
    ("e", "edit"),
    ("y", "yaml"),
    ("^n", "new"),
    ("^d", "delete"),
    ("< >", "sort column"),
    ("A", "asc/desc"),
    ("q", "quit"),
];
const DETAILS_HINTS: &[(&str, &str)] = &[("e", "edit"), ("y", "yaml"), ("^n", "new"), ("^d", "delete"), ("Esc", "back")];
const YAML_HINTS: &[(&str, &str)] = &[
    ("j/k", "move"),
    ("g/G", "top/bottom"),
    ("^d/^u", "page"),
    ("Space", "fold"),
    ("v", "select"),
    ("y", "copy"),
    ("Y", "copy all"),
    ("s", "service fields"),
    ("Esc", "back"),
];
const HEADER_HEIGHT: u16 = 2;

/// Fixed top bar on every screen: connection info and breadcrumbs. Keys live behind `?`.
fn render_header(frame: &mut Frame, area: Rect, info: &HeaderInfo, crumbs: &[String]) {
    let [bar, trail] = Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).areas(area);
    let bold = Style::new().add_modifier(Modifier::BOLD);
    let dim = Style::new().fg(Color::DarkGray);
    let bar_style = Style::new().bg(Color::Rgb(49, 50, 68));
    let label = bar_style.fg(Color::DarkGray);
    let value = bar_style.fg(Color::White).add_modifier(Modifier::BOLD);

    frame.render_widget(Block::new().style(bar_style), bar);
    let version = info.version.get().map_or("…", String::as_str);
    let left = Line::from(vec![
        Span::styled(" KOLOMBO ", bold.bg(Color::Cyan).fg(Color::Black)),
        Span::styled("  context ", label),
        Span::styled(info.context.clone(), value),
    ]);
    let right = Line::from(vec![
        Span::styled("k8s ", label),
        Span::styled(version, bar_style.fg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::styled(" ", bar_style),
    ])
    .right_aligned();
    frame.render_widget(Paragraph::new(left), bar);
    frame.render_widget(Paragraph::new(right), bar);

    let mut trail_spans = vec![Span::raw(" ")];
    for (i, c) in crumbs.iter().enumerate() {
        if i > 0 {
            trail_spans.push(Span::styled(" // ", dim));
        }
        let style = if i + 1 == crumbs.len() { bold.fg(Color::Cyan) } else { dim };
        trail_spans.push(Span::styled(c.clone(), style));
    }
    frame.render_widget(Paragraph::new(Line::from(trail_spans)), trail);

    let help = Line::from(vec![
        Span::styled("? ", bold.fg(Color::Yellow)),
        Span::styled("help ", dim),
    ])
    .right_aligned();
    frame.render_widget(Paragraph::new(help), trail);
}

/// Details pane: info block, labels table, annotations table.
fn render_details(frame: &mut Frame, area: Rect, ns: &Namespace, message: &str) {
    let m = &ns.metadata;
    let phase = ns.status.as_ref().and_then(|s| s.phase.clone()).unwrap_or_default();
    let phase_color = if phase == "Active" { Color::Green } else { Color::Yellow };
    let dim = Style::new().fg(Color::DarkGray);
    let created = m.creation_timestamp.as_ref().map_or(String::new(), |t| {
        format!("{} ({} ago)", t.0, format_age(jiff::Timestamp::now().as_second() - t.0.as_second()))
    });

    let outer = Block::bordered().title(" Details ").title_bottom(format!(" {message} "));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    let [info, labels, annotations] =
        Layout::vertical([Constraint::Length(5), Constraint::Fill(1), Constraint::Fill(1)]).areas(inner);

    let field = |k: &str, v: Span<'static>| Line::from(vec![Span::styled(format!("{k:<10}"), dim), v]);
    let lines = vec![
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
    let api: Api<Namespace> = Api::all(client.clone());
    let info = HeaderInfo { context: current_context(), version: Arc::new(OnceLock::new()) };
    {
        // Ask for the server version in the background so an unreachable cluster can't block startup.
        let version = info.version.clone();
        tokio::spawn(async move {
            let v = client.apiserver_version().await.map(|i| i.git_version);
            let _ = version.set(v.unwrap_or_else(|_| "unreachable".into()));
        });
    }
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
    tokio::task::spawn_blocking(move || ratatui::run(|terminal| run_app(terminal, &store, &api, &rt, &info))).await??;
    Ok(())
}

/// A popup that grabs all keys until closed.
enum Modal {
    Delete { name: String, yes: bool }, // `yes` = which button is focused
    New(String),                        // name typed so far
    Help(&'static [(&'static str, &'static str)]), // key list of the current pane
}

fn render_modal(frame: &mut Frame, modal: &Modal) {
    match modal {
        Modal::Delete { name, yes } => {
            let area = frame.area().centered(Constraint::Percentage(50), Constraint::Length(5));
            frame.render_widget(Clear, area);
            let button = |label: &str, focused: bool| {
                let style = if focused { Style::new().add_modifier(Modifier::REVERSED) } else { Style::new() };
                Span::styled(format!(" {label} "), style)
            };
            let text = vec![
                Line::from(format!("Delete namespace \"{name}\"?")),
                Line::raw(""),
                Line::from(vec![button("Yes", *yes), Span::raw("   "), button("No", !*yes)]),
            ];
            frame.render_widget(
                Paragraph::new(text)
                    .centered()
                    .block(Block::bordered().border_style(Style::new().fg(Color::Red)).title(" Delete (y/n, Enter) ")),
                area,
            );
        }
        Modal::Help(hints) => {
            let area = frame.area().centered(Constraint::Length(40), Constraint::Length(hints.len() as u16 + 2));
            frame.render_widget(Clear, area);
            let lines: Vec<Line> = hints
                .iter()
                .map(|(k, d)| {
                    Line::from(vec![
                        Span::styled(format!(" {k:<7}"), Style::new().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                        Span::styled(*d, Style::new().fg(Color::Gray)),
                    ])
                })
                .collect();
            frame.render_widget(
                Paragraph::new(lines).block(Block::bordered().title(" Keys (any key closes) ")),
                area,
            );
        }
        Modal::New(input) => {
            let area = frame.area().centered(Constraint::Percentage(50), Constraint::Length(3));
            frame.render_widget(Clear, area);
            frame.render_widget(
                Paragraph::new(input.as_str()).block(Block::bordered().title(" New namespace (Enter create, Esc cancel) ")),
                area,
            );
            frame.set_cursor_position((area.x + 1 + input.chars().count() as u16, area.y + 1));
        }
    }
}

async fn delete_ns(api: &Api<Namespace>, name: &str) -> String {
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => format!("{name} deleted (terminating)"), // namespaces go Terminating first
        Err(e) => format!("delete failed: {e}"),
    }
}

async fn create_ns(api: &Api<Namespace>, name: &str) -> String {
    let mut ns = Namespace::default();
    ns.metadata.name = Some(name.to_string());
    match api.create(&PostParams::default(), &ns).await {
        Ok(_) => format!("{name} created"),
        Err(e) => format!("create failed: {e}"),
    }
}

async fn replace_ns(api: &Api<Namespace>, name: &str, ns: &Namespace) -> String {
    match api.replace(name, &PostParams::default(), ns).await {
        Ok(_) => format!("{name} updated"),
        Err(e) => format!("update failed: {e}"),
    }
}

/// A background operation finished. `id` matches the Activity entry.
struct Done {
    id: u64,
    message: Option<String>,
}

/// Operations currently in flight; the newest label is shown in the corner.
struct Activity {
    items: Vec<(u64, String)>,
    next: u64,
}

impl Activity {
    fn start(&mut self, label: String) -> u64 {
        self.next += 1;
        self.items.push((self.next, label));
        self.next
    }

    fn finish(&mut self, id: u64) {
        self.items.retain(|(i, _)| *i != id);
    }

    /// "[=   ] deleting foo +1" while busy, else None. `tick` advances the animation.
    fn status(&self, tick: usize) -> Option<String> {
        const BAR: [&str; 6] = ["[=   ]", "[ =  ]", "[  = ]", "[   =]", "[  = ]", "[ =  ]"];
        let (_, label) = self.items.last()?;
        let more = if self.items.len() > 1 { format!(" +{}", self.items.len() - 1) } else { String::new() };
        Some(format!("{} {label}{more}", BAR[tick % BAR.len()]))
    }
}

/// Run `fut` on the tokio runtime; the TUI loop hears about the result through `tx`.
fn spawn_op(
    rt: &tokio::runtime::Handle,
    tx: &Sender<Done>,
    activity: &mut Activity,
    label: String,
    fut: impl Future<Output = String> + Send + 'static,
) {
    let id = activity.start(label);
    let tx = tx.clone();
    rt.spawn(async move {
        let message = fut.await;
        let _ = tx.send(Done { id, message: Some(message) });
    });
}

/// Put text on the system clipboard. The handle is kept so X11 can still serve the paste.
fn copy(clipboard: &mut Option<arboard::Clipboard>, text: &str) -> anyhow::Result<()> {
    if clipboard.is_none() {
        *clipboard = Some(arboard::Clipboard::new()?);
    }
    clipboard.as_mut().unwrap().set_text(text)?;
    Ok(())
}

/// Open the namespace as YAML in $EDITOR and parse the result.
/// Returns the edited object, or None when nothing changed.
fn edit_ns(terminal: &mut DefaultTerminal, ns: &Namespace) -> anyhow::Result<Option<(String, Namespace)>> {
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
        return Ok(None);
    }
    Ok(Some((name, serde_yaml_ng::from_str(&after)?)))
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
    info: &HeaderInfo,
) -> std::io::Result<()> {
    let matcher = SkimMatcherV2::default();
    let mut state = TableState::default().with_selected(Some(0));
    let mut query = String::new();
    let mut typing = false; // true while the search popup is open
    let mut sort_col = Column::Name;
    let mut ascending = true;
    let mut viewing: Option<String> = None; // namespace shown in the details view
    let (tx, rx) = mpsc::channel::<Done>();
    let mut activity = Activity { items: vec![(0, "loading namespaces".into())], next: 0 };
    {
        // id 0: the informer's first full list has arrived
        let (store, tx) = (store.clone(), tx.clone());
        rt.spawn(async move {
            let _ = store.wait_until_ready().await;
            let _ = tx.send(Done { id: 0, message: None });
        });
    }
    let started = Instant::now();
    let mut modal: Option<Modal> = None; // delete/new popup, when open
    let mut yaml: Option<YamlView> = None; // read-only YAML pane, when open
    let mut clipboard: Option<arboard::Clipboard> = None; // created on first copy, kept alive
    let mut message = String::new(); // result of the last action, cleared on next key

    loop {
        while let Ok(done) = rx.try_recv() {
            activity.finish(done.id);
            if let Some(m) = done.message {
                message = m;
            }
        }
        // Busy status wins over the last message; tick moves every 120ms.
        let tick = (started.elapsed().as_millis() / 120) as usize;
        let status = activity.status(tick).unwrap_or_else(|| message.clone());
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

        let (crumbs, hints): (Vec<String>, &'static [(&'static str, &'static str)]) = if let Some(v) = &yaml {
            (vec!["Namespaces".into(), v.name(), "YAML".into()], YAML_HINTS)
        } else if let Some(n) = &viewing {
            (vec!["Namespaces".into(), n.clone(), "Details".into()], DETAILS_HINTS)
        } else {
            (vec!["Namespaces".into()], LIST_HINTS)
        };

        terminal.draw(|frame| {
            let [head, body] = Layout::vertical([Constraint::Length(HEADER_HEIGHT), Constraint::Fill(1)]).areas(frame.area());
            render_header(frame, head, info, &crumbs);
            if let Some(v) = &mut yaml {
                v.render(frame, body, &status);
                if let Some(m) = &modal {
                    render_modal(frame, m);
                }
                return;
            }
            if let Some(ns) = &details {
                render_details(frame, body, ns, &status);
                if let Some(m) = &modal {
                    render_modal(frame, m);
                }
                return;
            }
            let title = format!(" {}/{} ", visible.len(), names.len());
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
                .block(Block::bordered().title(title).title_bottom(format!(" {status} ")))
                .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED));
            frame.render_stateful_widget(table, body, &mut state);

            if let Some(m) = &modal {
                render_modal(frame, m);
            }
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
        let wait = if activity.items.is_empty() { 250 } else { 80 }; // redraw faster while animating
        if !event::poll(Duration::from_millis(wait))? {
            continue;
        }
        let Event::Key(key) = event::read()? else { continue };

        message.clear();
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if let Some(m) = modal.take() {
            // take() empties `modal`; we put a popup back only if it stays open.
            modal = match m {
                Modal::Delete { name, yes } => match key.code {
                    code if code == KeyCode::Char('y') || (code == KeyCode::Enter && yes) => {
                        let api = api.clone();
                        let label = format!("deleting {name}");
                        spawn_op(rt, &tx, &mut activity, label, async move { delete_ns(&api, &name).await });
                        viewing = None;
                        None
                    }
                    KeyCode::Char('n') | KeyCode::Enter | KeyCode::Esc => None,
                    KeyCode::Left | KeyCode::Right | KeyCode::Tab | KeyCode::Char('h' | 'l') => {
                        Some(Modal::Delete { name, yes: !yes })
                    }
                    _ => Some(Modal::Delete { name, yes }),
                },
                Modal::Help(_) => None, // any key closes it
                Modal::New(mut input) => match key.code {
                    KeyCode::Enter if !input.is_empty() => {
                        let api = api.clone();
                        let label = format!("creating {input}");
                        spawn_op(rt, &tx, &mut activity, label, async move { create_ns(&api, &input).await });
                        None
                    }
                    KeyCode::Esc => None,
                    KeyCode::Char(c) if !ctrl => {
                        input.push(c);
                        Some(Modal::New(input))
                    }
                    KeyCode::Backspace => {
                        input.pop();
                        Some(Modal::New(input))
                    }
                    _ => Some(Modal::New(input)),
                },
            };
            continue;
        }
        if key.code == KeyCode::Char('?') && !typing {
            modal = Some(Modal::Help(hints));
            continue;
        }
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
                KeyCode::Char('d') if ctrl => {
                    modal = viewing.clone().map(|name| Modal::Delete { name, yes: false });
                }
                KeyCode::Char('n') if ctrl => modal = Some(Modal::New(String::new())),
                KeyCode::Esc | KeyCode::Char('q' | 'd') => viewing = None,
                KeyCode::Char('e') => edit_target = viewing.clone(),
                KeyCode::Char('y') => yaml_target = viewing.clone(),
                _ => {}
            }
        } else if typing {
            match key.code {
                KeyCode::Char(c) if !ctrl => {
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
                KeyCode::Char('d') if ctrl => {
                    modal = state
                        .selected()
                        .and_then(|i| visible.get(i))
                        .map(|n| Modal::Delete { name: n.name.clone(), yes: false });
                }
                KeyCode::Char('n') if ctrl => modal = Some(Modal::New(String::new())),
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
                match edit_ns(terminal, &ns) {
                    Ok(Some((name, edited))) => {
                        let api = api.clone();
                        let label = format!("updating {name}");
                        spawn_op(rt, &tx, &mut activity, label, async move { replace_ns(&api, &name, &edited).await });
                    }
                    Ok(None) => message = "no changes".into(),
                    Err(e) => message = format!("edit failed: {e:#}"),
                }
            }
        }
    }
}
