//! Live pod logs: one follow stream per container, merged by timestamp.

use futures::{AsyncBufReadExt, StreamExt};
use k8s_openapi::{api::core::v1::Pod, jiff::Timestamp};
use kube::{Api, api::LogParams};
use ratatui::{
    Frame,
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Paragraph},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{runtime::Handle, task::JoinHandle};

const CAP: usize = 20_000; // lines kept; the oldest go first
const RETRY: Duration = Duration::from_secs(3);
const PALETTE: [Color; 6] = [Color::Cyan, Color::Magenta, Color::Yellow, Color::Green, Color::Blue, Color::LightRed];

#[derive(Clone, Copy, PartialEq, Debug)]
enum Range {
    Minutes(i64),
    All,
}

struct LogLine {
    ts: i128, // unix nanoseconds, the merge key
    clock: String,
    container: usize, // index into LogView::containers
    text: String,
    note: bool, // our own message (stream error), not pod output
}

#[derive(Default)]
struct Buf {
    lines: Vec<LogLine>, // sorted by ts
}

/// `2026-11-23T10:15:30.123456789Z message` -> timestamp + message. Lines without a timestamp
/// (should not happen with `timestamps: true`) keep the whole text and sort as "now".
fn parse(container: usize, raw: &str) -> LogLine {
    let (stamp, text) = raw.split_once(' ').unwrap_or((raw, ""));
    match stamp.parse::<Timestamp>() {
        Ok(t) => LogLine { ts: t.as_nanosecond(), clock: t.strftime("%H:%M:%S").to_string(), container, text: text.to_string(), note: false },
        Err(_) => note_or_plain(container, raw, false),
    }
}

fn note_or_plain(container: usize, text: &str, note: bool) -> LogLine {
    let now = Timestamp::now();
    LogLine { ts: now.as_nanosecond(), clock: now.strftime("%H:%M:%S").to_string(), container, text: text.to_string(), note }
}

fn push(shared: &Mutex<Buf>, line: LogLine) {
    let mut b = shared.lock().unwrap();
    let at = if b.lines.last().is_none_or(|l| l.ts <= line.ts) {
        b.lines.len()
    } else {
        b.lines.partition_point(|l| l.ts <= line.ts)
    };
    b.lines.insert(at, line);
    if b.lines.len() > CAP + 1000 {
        b.lines.drain(..1000); // drop in chunks so this is not O(n) per line
    }
}

/// Follow one container forever: reconnects after the stream ends or fails (restart, still
/// starting), asking only for lines newer than the last one seen.
async fn follow(api: Api<Pod>, pod: String, container: String, index: usize, range: Range, shared: Arc<Mutex<Buf>>) {
    let mut last: Option<i128> = None;
    let mut last_note = String::new();
    loop {
        let mut lp = LogParams { container: Some(container.clone()), follow: true, timestamps: true, ..Default::default() };
        match (last, range) {
            (Some(t), _) => lp.since_time = Timestamp::from_nanosecond(t + 1).ok(),
            (None, Range::Minutes(m)) => lp.since_seconds = Some(m * 60),
            (None, Range::All) => lp.tail_lines = Some(CAP as i64),
        }
        match api.log_stream(&pod, &lp).await {
            Ok(stream) => {
                last_note.clear();
                let mut lines = Box::pin(stream.lines());
                while let Some(item) = lines.next().await {
                    match item {
                        Ok(raw) => {
                            let line = parse(index, &raw);
                            if last.is_some_and(|l| line.ts <= l) {
                                continue; // already shown before the reconnect
                            }
                            last = Some(line.ts);
                            push(&shared, line);
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(e) => {
                let msg = format!("{e}");
                if msg != last_note {
                    push(&shared, note_or_plain(index, &msg, true));
                    last_note = msg;
                }
            }
        }
        tokio::time::sleep(RETRY).await;
    }
}

pub struct LogView {
    containers: Vec<String>,
    api: Api<Pod>,
    rt: Handle,
    pod: String,
    range: Range,
    follow: bool, // "autoscroll"
    wrap: bool,
    top: usize,     // first shown line while not following
    hscroll: usize, // columns skipped when not wrapping
    height: usize,  // body rows, learned at render time
    shared: Arc<Mutex<Buf>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for LogView {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort(); // a dropped JoinHandle would leave the streams running
        }
    }
}

impl LogView {
    pub fn new(api: Api<Pod>, rt: Handle, pod: String, containers: Vec<String>) -> Self {
        let mut v = Self {
            containers,
            api,
            rt,
            pod,
            range: Range::Minutes(1),
            follow: true,
            wrap: false,
            top: 0,
            hscroll: 0,
            height: 1,
            shared: Arc::default(),
            tasks: vec![],
        };
        v.start();
        v
    }

    fn start(&mut self) {
        // A fresh buffer per start: a task that is aborted mid-poll can still write once more,
        // and it must write into the old one.
        self.shared = Arc::default();
        self.tasks = (0..self.containers.len())
            .map(|i| {
                let (api, pod, name, shared) = (self.api.clone(), self.pod.clone(), self.containers[i].clone(), self.shared.clone());
                let range = self.range;
                self.rt.spawn(follow(api, pod, name, i, range, shared))
            })
            .collect();
    }

    fn set_range(&mut self, range: Range) {
        for t in self.tasks.drain(..) {
            t.abort();
        }
        self.range = range;
        self.follow = false; // choosing a range means "let me read it"
        self.top = 0;
        self.start();
    }

    pub fn pod(&self) -> &str {
        &self.pod
    }

    fn scroll_to(&mut self, to: usize, last: usize) {
        self.follow = false; // scrolling by hand always stops autoscroll
        self.top = to.min(last);
    }

    /// true = close the view.
    pub fn handle_key(&mut self, key: KeyEvent) -> bool {
        let n = self.shared.lock().unwrap().lines.len();
        let last = n.saturating_sub(1);
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let half = (self.height / 2).max(1);
        match key.code {
            KeyCode::Char('1') => self.set_range(Range::Minutes(1)),
            KeyCode::Char('2') => self.set_range(Range::Minutes(5)),
            KeyCode::Char('3') => self.set_range(Range::Minutes(15)),
            KeyCode::Char('0') => self.set_range(Range::All),
            KeyCode::Char('s') => self.follow = !self.follow,
            KeyCode::Char('w') => {
                self.wrap = !self.wrap;
                self.hscroll = 0;
            }
            KeyCode::Char('j') | KeyCode::Down => self.scroll_to(self.top + 1, last),
            KeyCode::Char('k') | KeyCode::Up => self.scroll_to(self.top.saturating_sub(1), last),
            KeyCode::Char('d') if ctrl => self.scroll_to(self.top + half, last),
            KeyCode::Char('u') if ctrl => self.scroll_to(self.top.saturating_sub(half), last),
            KeyCode::PageDown => self.scroll_to(self.top + self.height, last),
            KeyCode::PageUp => self.scroll_to(self.top.saturating_sub(self.height), last),
            KeyCode::Char('g') => self.scroll_to(0, last),
            KeyCode::Char('G') => self.follow = true,
            KeyCode::Char('l') | KeyCode::Right if !self.wrap => self.hscroll += 8,
            KeyCode::Char('h') | KeyCode::Left if !self.wrap => self.hscroll = self.hscroll.saturating_sub(8),
            KeyCode::Esc | KeyCode::Char('q') => return true,
            _ => {}
        }
        false
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, message: &str) {
        self.height = area.height.saturating_sub(2).max(1) as usize;
        let multi = self.containers.len() > 1;
        let name_w = if multi { self.containers.iter().map(|c| c.chars().count()).max().unwrap_or(0) } else { 0 };
        let fmt = Fmt { wrap: self.wrap, hscroll: self.hscroll, width: area.width.saturating_sub(2) as usize, multi, name_w };

        let buf = self.shared.lock().unwrap();
        let n = buf.lines.len();
        let rows: Vec<Line> = if self.follow {
            // Walk back from the newest line until the screen is full.
            let (mut acc, mut count, mut i) = (Vec::new(), 0, n);
            while i > 0 && count < self.height {
                i -= 1;
                let r = fmt.rows(&buf.lines[i], &self.containers);
                count += r.len();
                acc.push(r);
            }
            self.top = i; // so turning autoscroll off keeps what you see
            let mut flat: Vec<Line> = acc.into_iter().rev().flatten().collect();
            flat.drain(..flat.len().saturating_sub(self.height));
            flat
        } else {
            self.top = self.top.min(n.saturating_sub(1));
            let (mut out, mut i) = (Vec::new(), self.top);
            while i < n && out.len() < self.height {
                out.extend(fmt.rows(&buf.lines[i], &self.containers));
                i += 1;
            }
            out.truncate(self.height);
            out
        };
        drop(buf);

        let dim = Style::new().fg(Color::DarkGray);
        let on = Style::new().add_modifier(Modifier::BOLD).fg(Color::Yellow);
        let choice = |label: &str, range: Range| Span::styled(format!(" {label} "), if self.range == range { on.add_modifier(Modifier::REVERSED) } else { dim });
        let title = Line::from(vec![
            Span::raw(" Logs "),
            choice("1:1m", Range::Minutes(1)),
            choice("2:5m", Range::Minutes(5)),
            choice("3:15m", Range::Minutes(15)),
            choice("0:all", Range::All),
        ]);
        let flag = |name: &str, on_: bool| {
            if on_ { Span::styled(format!(" {name} ● "), Style::new().fg(Color::Green)) } else { Span::styled(format!(" {name} ○ "), dim) }
        };
        let tags = Line::from(vec![flag("autoscroll", self.follow), flag("wrap", self.wrap), Span::styled(format!(" {n} lines · UTC "), dim)]).right_aligned();
        let body = if rows.is_empty() { vec![Line::styled("no lines in this range yet…", dim)] } else { rows };
        frame.render_widget(
            Paragraph::new(body).block(Block::bordered().title(title).title(tags).title_bottom(format!(" {message} "))),
            area,
        );
    }
}

/// How to turn one log line into screen rows.
struct Fmt {
    wrap: bool,
    hscroll: usize,
    width: usize,
    multi: bool,
    name_w: usize,
}

impl Fmt {
    fn prefix_width(&self) -> usize {
        9 + if self.multi { self.name_w + 1 } else { 0 }
    }

    fn rows(&self, l: &LogLine, names: &[String]) -> Vec<Line<'static>> {
        let dim = Style::new().fg(Color::DarkGray);
        let text = clean(&l.text);
        let mut head = vec![Span::styled(format!("{} ", l.clock), dim)];
        if self.multi {
            let color = PALETTE[l.container % PALETTE.len()];
            head.push(Span::styled(format!("{:<w$} ", names[l.container], w = self.name_w), Style::new().fg(color)));
        }
        let style = text_style(&text, l.note);
        if !self.wrap {
            let shown: String = text.chars().skip(self.hscroll).collect();
            head.push(Span::styled(shown, style));
            return vec![Line::from(head)];
        }
        let room = self.width.saturating_sub(self.prefix_width()).max(10);
        let chars: Vec<char> = text.chars().collect();
        if chars.is_empty() {
            return vec![Line::from(head)];
        }
        chars
            .chunks(room)
            .enumerate()
            .map(|(i, c)| {
                let part = Span::styled(c.iter().collect::<String>(), style);
                if i == 0 {
                    let mut first = head.clone();
                    first.push(part);
                    Line::from(first)
                } else {
                    Line::from(vec![Span::raw(" ".repeat(self.prefix_width())), part]) // continuation rows line up under the text
                }
            })
            .collect()
    }
}

/// Raw container output can hold colors and control codes that would garble the terminal.
fn clean(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '\x1b' => {
                // skip an escape sequence: ESC [ ... final letter
                for n in chars.by_ref() {
                    if n.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            '\t' => out.push_str("    "),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

fn text_style(t: &str, note: bool) -> Style {
    if note {
        return Style::new().fg(Color::DarkGray).add_modifier(Modifier::ITALIC);
    }
    let has = |needles: &[&str]| needles.iter().any(|n| t.contains(n));
    if has(&["ERROR", "FATAL", "level=error", "\"level\":\"error\"", "panic:"]) {
        Style::new().fg(Color::Red)
    } else if has(&["WARN", "level=warn", "\"level\":\"warn"]) {
        Style::new().fg(Color::Yellow)
    } else {
        Style::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_timestamp_and_orders_lines() {
        let a = parse(0, "2026-11-23T10:00:05.5Z second");
        let b = parse(1, "2026-11-23T10:00:05.123456789Z first");
        assert_eq!((a.text.as_str(), a.clock.as_str()), ("second", "10:00:05"));
        assert!(b.ts < a.ts); // ".123…" is earlier than ".5", though "5Z" sorts before "1" as text
        let shared = Mutex::new(Buf::default());
        push(&shared, a);
        push(&shared, b);
        let texts: Vec<_> = shared.lock().unwrap().lines.iter().map(|l| l.text.clone()).collect();
        assert_eq!(texts, ["first", "second"]);
    }

    #[test]
    fn strips_escape_codes() {
        assert_eq!(clean("\x1b[31mred\x1b[0m\tx\r"), "red    x");
    }

    #[test]
    fn wrap_continues_under_the_text_and_names_show_for_many_containers() {
        let line = parse(1, "2026-11-23T10:00:00Z 0123456789abcdefghij");
        let names = vec!["web".to_string(), "sidecar".to_string()];
        let f = Fmt { wrap: true, hscroll: 0, width: 9 + 8 + 10, multi: true, name_w: 7 };
        let rows = f.rows(&line, &names);
        let text = |l: &Line| l.spans.iter().map(|s| s.content.to_string()).collect::<String>();
        assert_eq!(rows.len(), 2);
        assert_eq!(text(&rows[0]), "10:00:00 sidecar 0123456789");
        assert_eq!(text(&rows[1]), "                 abcdefghij");
    }
}
