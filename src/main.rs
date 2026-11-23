use fuzzy_matcher::{FuzzyMatcher, skim::SkimMatcherV2};
use k8s_openapi::{api::core::v1::Namespace, jiff};
use kube::{Api, Client, api::ListParams};
use ratatui::{
    DefaultTerminal,
    crossterm::event::{self, Event, KeyCode},
    layout::Constraint,
    style::{Modifier, Style},
    widgets::{Block, Clear, Paragraph, Row, Table, TableState},
};

struct Ns {
    name: String,
    phase: String,
    created: i64, // unix seconds
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Fetch before entering the TUI so errors print on a normal terminal.
    let client = Client::try_default().await?;
    let api: Api<Namespace> = Api::all(client);
    let names: Vec<Ns> = api
        .list(&ListParams::default())
        .await?
        .into_iter()
        .map(|ns| Ns {
            name: ns.metadata.name.unwrap_or_default(),
            phase: ns.status.and_then(|s| s.phase).unwrap_or_default(),
            created: ns.metadata.creation_timestamp.map_or(0, |t| t.0.as_second()),
        })
        .collect();

    // run() enters raw mode + alternate screen (fullscreen) and restores on exit/panic.
    ratatui::run(|terminal| run_app(terminal, &names))?;
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

fn run_app(terminal: &mut DefaultTerminal, names: &[Ns]) -> std::io::Result<()> {
    let matcher = SkimMatcherV2::default();
    let mut state = TableState::default().with_selected(Some(0));
    let mut query = String::new();
    let mut typing = false; // true while the search popup is open

    loop {
        let visible = filter(names, &query, &matcher);

        terminal.draw(|frame| {
            let title = format!(" Namespaces {}/{} (/ search, q quit) ", visible.len(), names.len());
            let now = jiff::Timestamp::now().as_second();
            let rows = visible
                .iter()
                .map(|n| Row::new([n.name.clone(), n.phase.clone(), format_age(now - n.created)]));
            let widths = [Constraint::Fill(1), Constraint::Length(12), Constraint::Length(6)];
            let table = Table::new(rows, widths)
                .header(Row::new(["NAME", "STATUS", "AGE"]).style(Style::new().add_modifier(Modifier::BOLD)))
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
