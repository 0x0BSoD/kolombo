//! The start screen: cluster facts, CPU / memory / pod totals, workload health and the latest events.
//! Unlike the lists it keeps no watchers: one task re-lists everything every `POLL_SECS`.

use crate::{HeaderInfo, Usage, cpu_text, created_secs, format_age, mem_text, nodes, parse_cpu, parse_mem, status_color, sum_resource};
use futures::join;
use k8s_openapi::{
    api::{
        apps::v1::{DaemonSet, Deployment, StatefulSet},
        core::v1::{Event, Namespace, Node, Pod},
    },
    jiff,
    serde::de::DeserializeOwned,
};
use kube::{Api, Client, Resource, api::ListParams};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols::Marker,
    text::{Line, Span},
    widgets::{Axis, Block, Cell, Chart, Dataset, GraphType, Padding, Paragraph, Row, Table, TableState, Wrap},
};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::{Arc, Mutex},
    time::Duration,
};

const EVENTS: usize = 20;

/// Allocatable capacity, what pods request of it, and (with metrics-server) what they use.
#[derive(Clone, Copy, Default, Debug, PartialEq)]
pub struct Totals {
    pub alloc: u64,
    pub requested: u64,
    pub used: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Ev {
    id: String, // namespace/name of the Event object: keeps the selection stable across polls
    at: i64,
    ns: String,
    kind: String, // Normal / Warning
    reason: String,
    obj_kind: String,
    obj_name: String,
    message: String,
    count: i32,
}

/// Everything the screen shows, boiled down from the raw lists.
#[derive(Clone, Default, Debug)]
pub struct Snapshot {
    pub ready: bool, // the first poll has finished
    pub errors: Vec<String>, // lists that could not be read (RBAC, API down...)
    pub nodes: (usize, usize),
    pub namespaces: usize,
    pub cpu: Totals,
    pub mem: Totals,
    pub pods: (u64, u64), // holding a slot, slots the nodes offer
    pub workloads: Vec<(&'static str, usize, usize)>, // label, ready, total
    pub events: Vec<Ev>,
    pub pod_history: Vec<u64>, // ready pods, one sample per poll while the dashboard is open
}

#[derive(Default)]
struct Raw {
    nodes: Vec<Node>,
    pods: Vec<Pod>,
    namespaces: usize,
    deployments: Vec<Deployment>,
    statefulsets: Vec<StatefulSet>,
    daemonsets: Vec<DaemonSet>,
    events: Vec<Event>,
    usage: Option<HashMap<String, Usage>>,
}

fn finished(pod: &Pod) -> bool {
    matches!(pod.status.as_ref().and_then(|s| s.phase.as_deref()), Some("Succeeded" | "Failed"))
}

fn node_ready(n: &Node) -> bool {
    n.status
        .iter()
        .flat_map(|s| s.conditions.iter().flatten())
        .any(|c| c.type_ == "Ready" && c.status == "True")
}

fn allocatable<'a>(n: &'a Node, key: &str) -> Option<&'a str> {
    Some(n.status.as_ref()?.allocatable.as_ref()?.get(key)?.0.as_str())
}

fn summarize(raw: &Raw, errors: Vec<String>) -> Snapshot {
    let sum = |key: &str, parse: fn(&str) -> Option<u64>| raw.nodes.iter().filter_map(|n| parse(allocatable(n, key)?)).sum::<u64>();
    let requested = |key: &str, parse: fn(&str) -> Option<u64>| {
        raw.pods
            .iter()
            .filter(|p| !finished(p) && p.spec.as_ref().is_some_and(|s| s.node_name.is_some()))
            .map(|p| sum_resource(p, false, key, parse).unwrap_or(0))
            .sum::<u64>()
    };
    let slots = raw.pods.iter().filter(|p| !finished(p) && p.spec.as_ref().is_some_and(|s| s.node_name.is_some())).count() as u64;

    // pod ready = running with every container ready; completed pods are neither ready nor missing
    let live: Vec<&Pod> = raw.pods.iter().filter(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) != Some("Succeeded")).collect();
    let pods_ready = live.iter().filter(|p| p.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Running") && {
        let (ready, total) = crate::pod_ready(p);
        ready == total
    }).count();

    let replicas_ok = |want: Option<i32>, ready: Option<i32>| ready.unwrap_or(0) >= want.unwrap_or(1);
    let deploys = raw.deployments.iter().filter(|d| replicas_ok(d.spec.replicas, d.status.as_ref().and_then(|s| s.ready_replicas))).count();
    let sts = raw.statefulsets.iter().filter(|d| replicas_ok(d.spec.replicas, d.status.as_ref().and_then(|s| s.ready_replicas))).count();
    let ds = raw
        .daemonsets
        .iter()
        .filter(|d| d.status.as_ref().is_some_and(|s| s.number_ready >= s.desired_number_scheduled))
        .count();

    let mut events: Vec<Ev> = raw
        .events
        .iter()
        .map(|e| Ev {
            at: e
                .last_timestamp
                .as_ref()
                .map(|t| t.0.as_second())
                .or(e.event_time.as_ref().map(|t| t.0.as_second()))
                .unwrap_or_else(|| created_secs(&e.metadata)),
            id: format!("{}/{}", e.metadata.namespace.as_deref().unwrap_or_default(), e.metadata.name.as_deref().unwrap_or_default()),
            ns: e.metadata.namespace.clone().unwrap_or_default(),
            kind: e.type_.clone().unwrap_or_default(),
            reason: e.reason.clone().unwrap_or_default(),
            obj_kind: e.involved_object.kind.clone().unwrap_or_default(),
            obj_name: e.involved_object.name.clone().unwrap_or_default(),
            message: e.message.clone().unwrap_or_default().replace('\n', " "),
            count: e.count.unwrap_or(1),
        })
        .collect();
    events.sort_by_key(|e| std::cmp::Reverse(e.at));
    events.truncate(EVENTS);

    Snapshot {
        ready: true,
        errors,
        nodes: (raw.nodes.iter().filter(|n| node_ready(n)).count(), raw.nodes.len()),
        namespaces: raw.namespaces,
        cpu: Totals {
            alloc: sum("cpu", parse_cpu),
            requested: requested("cpu", parse_cpu),
            used: raw.usage.as_ref().map(|u| u.values().map(|u| u.cpu).sum()),
        },
        mem: Totals {
            alloc: sum("memory", parse_mem),
            requested: requested("memory", parse_mem),
            used: raw.usage.as_ref().map(|u| u.values().map(|u| u.mem).sum()),
        },
        pods: (slots, sum("pods", |p| p.parse().ok())),
        workloads: vec![
            ("Pods", pods_ready, live.len()),
            ("Deployments", deploys, raw.deployments.len()),
            ("StatefulSets", sts, raw.statefulsets.len()),
            ("DaemonSets", ds, raw.daemonsets.len()),
        ],
        events,
        pod_history: vec![],
    }
}

async fn list<K>(client: &Client, what: &str, errors: &mut Vec<String>) -> Vec<K>
where
    K: Resource<DynamicType = ()> + Clone + DeserializeOwned + Debug,
{
    match Api::<K>::all(client.clone()).list(&ListParams::default()).await {
        Ok(l) => l.items,
        Err(e) => {
            errors.push(format!("{what}: {e}"));
            vec![]
        }
    }
}

async fn fetch(client: &Client) -> Snapshot {
    let (mut e1, mut e2, mut e3, mut e4, mut e5, mut e6, mut e7) = Default::default();
    let (nodes, pods, namespaces, deployments, statefulsets, daemonsets, events, usage) = join!(
        list::<Node>(client, "nodes", &mut e1),
        list::<Pod>(client, "pods", &mut e2),
        list::<Namespace>(client, "namespaces", &mut e3),
        list::<Deployment>(client, "deployments", &mut e4),
        list::<StatefulSet>(client, "statefulsets", &mut e5),
        list::<DaemonSet>(client, "daemonsets", &mut e6),
        list::<Event>(client, "events", &mut e7),
        nodes::fetch_usage(client),
    );
    let errors = [e1, e2, e3, e4, e5, e6, e7].concat();
    let raw = Raw { nodes, pods, namespaces: namespaces.len(), deployments, statefulsets, daemonsets, events, usage: usage.ok() };
    summarize(&raw, errors)
}

const HISTORY: usize = 360; // one hour at one sample per 10s

pub struct Dashboard {
    snap: Arc<Mutex<Snapshot>>,
    task: tokio::task::JoinHandle<()>,
    view: View,
}

/// What the user moved: the highlighted event, by id so a refresh does not make it jump.
#[derive(Default)]
struct View {
    sel_id: Option<String>,
    table: TableState,
}

impl Dashboard {
    pub fn open(rt: &tokio::runtime::Handle, client: &Client) -> Self {
        let snap = Arc::new(Mutex::new(Snapshot::default()));
        let (out, client) = (snap.clone(), client.clone());
        let task = rt.spawn(async move {
            let mut history: VecDeque<u64> = VecDeque::new();
            loop {
                let mut fresh = fetch(&client).await;
                // a failed pod list would draw a false dip to zero
                if !fresh.errors.iter().any(|e| e.starts_with("pods")) {
                    history.push_back(fresh.workloads[0].1 as u64);
                    if history.len() > HISTORY {
                        history.pop_front();
                    }
                }
                fresh.pod_history = history.iter().copied().collect();
                *out.lock().unwrap() = fresh;
                tokio::time::sleep(Duration::from_secs(crate::POLL_SECS)).await;
            }
        });
        Self { snap, task, view: View::default() }
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, info: &HeaderInfo, status: &str) {
        let s = self.snap.lock().unwrap().clone();
        draw(frame, area, &s, info, status, &mut self.view);
    }

    /// Move the event highlight.
    pub fn step(&mut self, delta: isize) {
        let s = self.snap.lock().unwrap();
        let at = self.view.sel_id.as_ref().and_then(|id| s.events.iter().position(|e| &e.id == id)).unwrap_or(0);
        let to = (at as isize + delta).clamp(0, s.events.len().saturating_sub(1) as isize) as usize;
        self.view.sel_id = s.events.get(to).map(|e| e.id.clone());
    }
}

impl Drop for Dashboard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

const BLUE: Color = Color::Rgb(90, 170, 240);
const PURPLE: Color = Color::Rgb(190, 140, 240);
const ORANGE: Color = Color::Rgb(240, 165, 90);
const SELECTED: Color = Color::Rgb(60, 50, 120);

fn draw(frame: &mut Frame, area: Rect, s: &Snapshot, info: &HeaderInfo, status: &str, view: &mut View) {
    if !s.ready {
        frame.render_widget(Paragraph::new("loading cluster…").style(Style::new().fg(Color::DarkGray)), area);
        return;
    }
    let [top, mid, bottom] = Layout::vertical([Constraint::Length(4), Constraint::Length(7), Constraint::Fill(1)]).areas(area);
    let [cluster, meters] = Layout::horizontal([Constraint::Percentage(30), Constraint::Fill(1)]).areas(top);
    let [workloads, readiness] = Layout::horizontal([Constraint::Percentage(62), Constraint::Fill(1)]).areas(mid);
    let [events, detail] = Layout::horizontal([Constraint::Percentage(68), Constraint::Fill(1)]).areas(bottom);
    render_cluster(frame, cluster, s, info);
    render_meters(frame, meters, s);
    render_workloads(frame, workloads, s);
    render_readiness(frame, readiness, s);
    render_events(frame, events, s, status, view);
    render_detail(frame, detail, s, view);
}

fn panel(title: &str) -> Block<'static> {
    let b = Block::bordered().border_style(Style::new().fg(Color::DarkGray)).padding(Padding::horizontal(1));
    if title.is_empty() { b } else { b.title(Span::styled(format!(" {title} "), Style::new().add_modifier(Modifier::BOLD))) }
}

fn render_cluster(frame: &mut Frame, area: Rect, s: &Snapshot, info: &HeaderInfo) {
    let bold = Style::new().add_modifier(Modifier::BOLD);
    let dim = Style::new().fg(Color::DarkGray);
    let (ready, total) = s.nodes;
    let all_ready = s.workloads.iter().all(|(_, r, t)| r >= t);
    let health = if ready < total { "Unhealthy" } else if !s.errors.is_empty() || !all_ready { "Degraded" } else { "Healthy" };
    let sep = || Span::styled(" │ ", dim);
    let nodes_color = if ready == total { Color::Green } else { Color::Red };
    let lines = vec![
        Line::from(vec![
            Span::styled(info.context.clone(), bold.add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(format!("● {health}"), Style::new().fg(status_color(health)).add_modifier(Modifier::BOLD)),
        ]),
        Line::from(vec![
            Span::raw(info.version.get().cloned().unwrap_or_else(|| "…".into())),
            sep(),
            Span::styled("nodes ", dim),
            Span::styled(format!("{ready}/{total}"), Style::new().fg(nodes_color)),
            sep(),
            Span::styled("namespaces ", dim),
            Span::raw(s.namespaces.to_string()),
        ]),
    ];
    let mut b = panel("");
    if let Some(e) = s.errors.first() {
        b = b.border_style(Style::new().fg(Color::Yellow)).title(Span::styled(format!(" partial data: {e} "), Style::new().fg(Color::Yellow)));
    }
    frame.render_widget(Paragraph::new(lines).block(b), area);
}

/// Label and numbers on one line, a bar below. `█` is use, `▓` what pods only request, `░` free.
fn meter(frame: &mut Frame, area: Rect, label: &str, detail: &str, used: f64, requested: f64, color: Color) {
    let [head, bar] = Layout::vertical([Constraint::Length(1), Constraint::Length(1)]).areas(area);
    let bold = Style::new().fg(color).add_modifier(Modifier::BOLD);
    frame.render_widget(Paragraph::new(Span::styled(label.to_string(), bold)), head);
    let pct = format!("{:>3.0}%", used * 100.0);
    // numbers go first to make room: on a narrow column only the percentage stays
    let right = if label.len() + detail.len() + pct.len() + 3 <= head.width as usize { format!("{detail} {pct}") } else { pct };
    frame.render_widget(Paragraph::new(Line::from(right).right_aligned()), head);
    let w = bar.width as usize;
    let cells = |r: f64| ((r.clamp(0.0, 1.0) * w as f64).round() as usize).min(w);
    let (u, r) = (cells(used), cells(requested).max(cells(used)));
    let line = Line::from(vec![
        Span::styled("█".repeat(u), Style::new().fg(color)),
        Span::styled("▓".repeat(r - u), Style::new().fg(color).add_modifier(Modifier::DIM)),
        Span::styled("░".repeat(w - r), Style::new().fg(Color::DarkGray)),
    ]);
    frame.render_widget(Paragraph::new(line), bar);
}

fn render_meters(frame: &mut Frame, area: Rect, s: &Snapshot) {
    let inner = panel("").inner(area);
    frame.render_widget(panel(""), area);
    let [cpu, mem, pods] = Layout::horizontal([Constraint::Fill(1); 3]).spacing(3).areas(inner);
    let ratio = |n: u64, d: u64| if d == 0 { 0.0 } else { n as f64 / d as f64 };
    // without metrics-server the bar falls back to what pods request
    let resource = |frame: &mut Frame, at: Rect, name: &str, t: Totals, text: fn(Option<u64>) -> String, color: Color| {
        let (shown, tag) = match t.used {
            Some(u) => (u, ""),
            None => (t.requested, "req "),
        };
        meter(frame, at, name, &format!("{tag}{}/{}", text(Some(shown)), text(Some(t.alloc))), ratio(shown, t.alloc), ratio(t.requested, t.alloc), color);
    };
    resource(frame, cpu, "CPU", s.cpu, |v| cpu_text(v), BLUE);
    resource(frame, mem, "Memory", s.mem, mem_text, PURPLE);
    meter(frame, pods, "Pods", &format!("{}/{}", s.pods.0, s.pods.1), ratio(s.pods.0, s.pods.1), 0.0, ORANGE);
}

fn render_workloads(frame: &mut Frame, area: Rect, s: &Snapshot) {
    let b = panel("Workloads");
    let inner = b.inner(area);
    frame.render_widget(b, area);
    let cards = Layout::horizontal(vec![Constraint::Fill(1); s.workloads.len().max(1)]).spacing(1).split(inner);
    for ((name, ready, total), at) in s.workloads.iter().zip(cards.iter()) {
        let verdict = if *total == 0 {
            Span::styled("none", Style::new().fg(Color::DarkGray))
        } else if ready >= total {
            Span::styled("✓", Style::new().fg(Color::Green).add_modifier(Modifier::BOLD))
        } else {
            Span::styled(format!("{} not ready", total - ready), Style::new().fg(Color::Yellow))
        };
        let lines = vec![
            Line::from(Span::styled(name.to_string(), Style::new().add_modifier(Modifier::BOLD))),
            Line::from(format!("{ready} / {total}")),
            Line::from(verdict),
        ];
        frame.render_widget(Paragraph::new(lines).block(panel("").padding(Padding::horizontal(1))), *at);
    }
}

fn render_readiness(frame: &mut Frame, area: Rect, s: &Snapshot) {
    let n = s.pod_history.len();
    let span = format_age((n.saturating_sub(1) as u64 * crate::POLL_SECS) as i64);
    let b = panel(&format!("Pod readiness ({span})"));
    if n < 2 {
        frame.render_widget(Paragraph::new("collecting samples…").style(Style::new().fg(Color::DarkGray)).block(b), area);
        return;
    }
    let data: Vec<(f64, f64)> = s.pod_history.iter().enumerate().map(|(i, v)| (i as f64, *v as f64)).collect();
    let (lo, hi) = (*s.pod_history.iter().min().unwrap() as f64, *s.pod_history.iter().max().unwrap() as f64);
    let (lo, hi) = (lo - 1.0, hi + 1.0); // headroom so a flat line is not glued to the frame
    let dim = Style::new().fg(Color::DarkGray);
    let set = Dataset::default().marker(Marker::Braille).graph_type(GraphType::Line).style(Style::new().fg(Color::Green)).data(&data);
    let chart = Chart::new(vec![set])
        .block(b)
        .x_axis(Axis::default().style(dim).bounds([0.0, (n - 1) as f64]))
        .y_axis(Axis::default().style(dim).bounds([lo, hi]).labels([format!("{lo:.0}"), format!("{:.0}", (lo + hi) / 2.0), format!("{hi:.0}")]));
    frame.render_widget(chart, area);
}

fn render_events(frame: &mut Frame, area: Rect, s: &Snapshot, status: &str, view: &mut View) {
    let dim = Style::new().fg(Color::DarkGray);
    let now = jiff::Timestamp::now().as_second();
    let rows: Vec<Row> = s
        .events
        .iter()
        .map(|e| {
            let count = if e.count > 1 { format!(" x{}", e.count) } else { String::new() };
            let color = status_color(&e.kind);
            Row::new(vec![
                Cell::from(format!("{}{count}", format_age(now - e.at))),
                Cell::from(e.obj_name.clone()),
                Cell::from(Line::from(vec![Span::styled("● ", Style::new().fg(color)), Span::styled(e.kind.clone(), Style::new().fg(color))])),
                Cell::from(e.reason.clone()),
                Cell::from(e.message.clone()),
            ])
        })
        .collect();
    let widths = [Constraint::Length(9), Constraint::Length(28), Constraint::Length(10), Constraint::Length(22), Constraint::Fill(1)];
    let header = Row::new(["AGE", "OBJECT", "TYPE", "REASON", "MESSAGE"]).style(dim);
    let mut b = panel(&format!("Recent events ({})", s.events.len()));
    if !status.is_empty() {
        b = b.title_bottom(Line::from(format!(" {status} ")).right_aligned());
    }
    // the highlight follows the event, not the row number
    let at = view.sel_id.as_ref().and_then(|id| s.events.iter().position(|e| &e.id == id)).unwrap_or(0);
    view.sel_id = s.events.get(at).map(|e| e.id.clone());
    view.table.select((!s.events.is_empty()).then_some(at));
    let table = Table::new(rows, widths).header(header).block(b).row_highlight_style(Style::new().bg(SELECTED));
    frame.render_stateful_widget(table, area, &mut view.table);
}

fn render_detail(frame: &mut Frame, area: Rect, s: &Snapshot, view: &View) {
    let b = panel("Event");
    let inner = b.inner(area);
    frame.render_widget(b, area);
    let Some(e) = view.sel_id.as_ref().and_then(|id| s.events.iter().find(|e| &e.id == id)) else {
        frame.render_widget(Paragraph::new("no events").style(Style::new().fg(Color::DarkGray)), inner);
        return;
    };
    let dim = Style::new().fg(Color::DarkGray);
    let bold = Style::new().add_modifier(Modifier::BOLD);
    let clock = jiff::Timestamp::from_second(e.at)
        .map(|t| t.to_zoned(jiff::tz::TimeZone::system()).strftime("%H:%M:%S").to_string())
        .unwrap_or_default();
    let age = format_age(jiff::Timestamp::now().as_second() - e.at);
    let field = |k: &str, v: Span<'static>| Line::from(vec![Span::styled(format!("{k:<8}"), dim), v]);
    let mut fields = vec![
        field("Object", Span::raw(format!("{}/{}", e.obj_kind.to_lowercase(), e.obj_name))),
        field("Type", Span::styled(e.kind.clone(), Style::new().fg(status_color(&e.kind)))),
        field("Reason", Span::raw(e.reason.clone())),
        field("Age", Span::raw(format!("{age} ({clock})"))),
    ];
    if e.count > 1 {
        fields.push(field("Count", Span::raw(e.count.to_string())));
    }
    let [facts, msg_title, msg, obj_title, obj] = Layout::vertical([
        Constraint::Length(5),
        Constraint::Length(1),
        Constraint::Fill(1),
        Constraint::Length(1),
        Constraint::Length(5),
    ])
    .areas(inner);
    frame.render_widget(Paragraph::new(fields), facts);
    frame.render_widget(Paragraph::new(Span::styled("Message", bold)), msg_title);
    frame.render_widget(
        Paragraph::new(e.message.clone()).wrap(Wrap { trim: true }).block(Block::bordered().border_style(dim).padding(Padding::horizontal(1))),
        msg,
    );
    frame.render_widget(Paragraph::new(Span::styled("Involved Object", bold)), obj_title);
    let card = vec![
        Line::from(Span::styled(e.obj_kind.clone(), bold)),
        Line::from(e.obj_name.clone()),
        Line::from(vec![Span::styled("ns: ", dim), Span::raw(e.ns.clone())]),
    ];
    frame.render_widget(Paragraph::new(card).block(Block::bordered().border_style(dim).padding(Padding::horizontal(1))), obj);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse<T: DeserializeOwned>(json: &str) -> T {
        serde_yaml_ng::from_str(json).unwrap()
    }

    #[test]
    fn totals_workloads_and_newest_events_first() {
        let raw = Raw {
            nodes: vec![
                parse(r#"{"metadata": {"name": "a"}, "status": {"conditions": [{"type": "Ready", "status": "True"}], "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"}}}"#),
                parse(r#"{"metadata": {"name": "b"}, "status": {"conditions": [{"type": "Ready", "status": "False"}], "allocatable": {"cpu": "4", "memory": "8Gi", "pods": "110"}}}"#),
            ],
            pods: vec![
                parse(r#"{"metadata": {"name": "up"}, "spec": {"nodeName": "a", "containers": [{"name": "c", "resources": {"requests": {"cpu": "500m", "memory": "1Gi"}}}]},
                         "status": {"phase": "Running", "containerStatuses": [{"name": "c", "ready": true, "restartCount": 0, "image": "i", "imageID": "x"}]}}"#),
                parse(r#"{"metadata": {"name": "waiting"}, "spec": {"containers": [{"name": "c"}]}, "status": {"phase": "Pending"}}"#),
                parse(r#"{"metadata": {"name": "done"}, "spec": {"nodeName": "a", "containers": [{"name": "c", "resources": {"requests": {"cpu": "2"}}}]}, "status": {"phase": "Succeeded"}}"#),
            ],
            namespaces: 3,
            deployments: vec![
                parse(r#"{"metadata": {"name": "ok"}, "spec": {"replicas": 2, "selector": {}, "template": {"spec": {"containers": []}}}, "status": {"readyReplicas": 2}}"#),
                parse(r#"{"metadata": {"name": "bad"}, "spec": {"replicas": 2, "selector": {}, "template": {"spec": {"containers": []}}}, "status": {"readyReplicas": 1}}"#),
            ],
            daemonsets: vec![parse(
                r#"{"metadata": {"name": "d"}, "spec": {"selector": {}, "template": {}}, "status": {"currentNumberScheduled": 2, "desiredNumberScheduled": 2, "numberMisscheduled": 0, "numberReady": 2}}"#,
            )],
            events: vec![
                parse(r#"{"metadata": {"name": "e1", "namespace": "x"}, "involvedObject": {"kind": "Pod", "name": "up"}, "type": "Normal", "reason": "Old", "lastTimestamp": "2026-01-01T00:00:00Z"}"#),
                parse(r#"{"metadata": {"name": "e2", "namespace": "x"}, "involvedObject": {"kind": "Pod", "name": "up"}, "type": "Warning", "reason": "New", "message": "a\nb", "lastTimestamp": "2026-02-01T00:00:00Z"}"#),
            ],
            usage: Some(HashMap::from([("a".to_string(), Usage { cpu: 300, mem: 1 << 30 })])),
            ..Default::default()
        };
        let s = summarize(&raw, vec![]);
        assert_eq!(s.nodes, (1, 2));
        assert_eq!((s.cpu.alloc, s.cpu.requested, s.cpu.used), (8000, 500, Some(300))); // the finished pod requests nothing
        assert_eq!((s.mem.alloc, s.mem.requested), (16 << 30, 1 << 30));
        assert_eq!(s.pods, (1, 220)); // only the scheduled, unfinished pod holds a slot
        assert_eq!(s.workloads, vec![("Pods", 1, 2), ("Deployments", 1, 2), ("StatefulSets", 0, 0), ("DaemonSets", 1, 1)]);
        assert_eq!(s.events.iter().map(|e| e.reason.as_str()).collect::<Vec<_>>(), ["New", "Old"]);
        assert_eq!((s.events[0].obj_kind.as_str(), s.events[0].obj_name.as_str()), ("Pod", "up"));
        assert_eq!(s.events[0].message, "a b");
    }

    #[test]
    fn screen_renders_every_section() {
        let ev = |id: &str, kind: &str, reason: &str| Ev {
            id: id.into(),
            at: 0,
            ns: "kube-system".into(),
            kind: kind.into(),
            reason: reason.into(),
            obj_kind: "Pod".into(),
            obj_name: format!("pod-{id}"),
            message: "restarting the thing".into(),
            count: 3,
        };
        let snap = Snapshot {
            ready: true,
            nodes: (2, 3),
            namespaces: 7,
            cpu: Totals { alloc: 8000, requested: 5000, used: Some(2000) },
            mem: Totals { alloc: 16 << 30, requested: 4 << 30, used: None },
            pods: (40, 330),
            workloads: vec![("Pods", 38, 40), ("Deployments", 5, 5), ("StatefulSets", 0, 0), ("DaemonSets", 3, 3)],
            events: vec![ev("a", "Warning", "BackOff"), ev("b", "Normal", "Started")],
            pod_history: vec![36, 38, 40, 38],
            ..Default::default()
        };
        let info = HeaderInfo { context: "prod".into(), version: Default::default() };
        let mut view = View::default();
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(140, 30)).unwrap();
        terminal.draw(|f| draw(f, f.area(), &snap, &info, "", &mut view)).unwrap();
        let text: String = (0..30).map(|y| (0..140).map(|x| terminal.backend().buffer()[(x, y)].symbol().to_string()).collect::<String>() + "\n").collect();
        println!("{text}");
        for want in [
            "prod", "Unhealthy", "nodes 2/3", "namespaces 7", "2000m/8000m", "req 4.0Gi/16.0Gi", "40/330", "2 not ready", "✓", "none",
            "Pod readiness", "BackOff", "pod-a", "Involved Object", "ns: kube-system", "Count",
        ] {
            assert!(text.contains(want), "missing {want}");
        }
        assert_eq!(view.sel_id.as_deref(), Some("a"));
    }
}
