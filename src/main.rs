mod container_view;
mod yaml_view;

use futures::StreamExt;
use fuzzy_matcher::{FuzzyMatcher, skim::SkimMatcherV2};
use k8s_openapi::{
    api::core::v1::{Container, ContainerState, Event as KubeEvent, Namespace, Pod},
    apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time},
    jiff,
    serde::{Serialize, de::DeserializeOwned},
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{DeleteParams, ListParams, PostParams},
    core::{ApiResource, DynamicObject, GroupVersionKind},
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
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
    process::Command,
    sync::{
        Arc, Mutex, OnceLock,
        mpsc::{self, Sender},
    },
    time::{Duration, Instant},
};
use yaml_view::{Outcome, YamlView};

/// Everything the generic helpers need from a Kubernetes resource type.
/// One name instead of a six-line `where` clause on every function.
trait Kind:
    Resource<DynamicType = ()> + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
}
impl<K> Kind for K where
    K: Resource<DynamicType = ()> + Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
}

/// One row of a list, already boiled down from a Namespace or a Pod.
struct Item {
    name: String,
    status: String,
    created: i64,                  // unix seconds
    labels: Vec<(String, String)>, // user labels only, sorted by key
    pod: Option<PodCols>,          // pod-only columns; None for namespaces
}

/// True for `kubernetes.io/...` and `*.kubernetes.io/...` keys (system-managed).
fn is_system_label(key: &str) -> bool {
    key.split_once('/')
        .is_some_and(|(prefix, _)| prefix == "kubernetes.io" || prefix.ends_with(".kubernetes.io"))
}

/// User labels worth a pill. A namespace's `name` label just repeats its name, so it can be hidden.
fn labels_of(meta: &ObjectMeta, hide_name: bool) -> Vec<(String, String)> {
    meta.labels
        .iter()
        .flatten() // Option<BTreeMap> -> entries (BTreeMap is already key-sorted)
        .filter(|(k, _)| !is_system_label(k) && !(hide_name && *k == "name"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn created_secs(meta: &ObjectMeta) -> i64 {
    meta.creation_timestamp.as_ref().map_or(0, |t| t.0.as_second())
}

fn ns_item(ns: &Namespace) -> Item {
    Item {
        name: ns.name_any(),
        status: ns.status.as_ref().and_then(|s| s.phase.clone()).unwrap_or_default(),
        created: created_secs(&ns.metadata),
        labels: labels_of(&ns.metadata, true),
        pod: None,
    }
}

/// What `kubectl get pods` would put in STATUS. `status.phase` alone says "Running" for a
/// crash-looping pod, so look at deletion and container states first.
/// shortcut: init containers (Init:0/1) are not looked at, add if you miss them.
fn pod_status(pod: &Pod) -> String {
    if pod.metadata.deletion_timestamp.is_some() {
        return "Terminating".into();
    }
    let Some(status) = &pod.status else { return String::new() };
    let containers: Vec<_> = status.container_statuses.iter().flatten().collect();
    // `?` inside a closure returns None from the closure, which find_map skips.
    let waiting = containers
        .iter()
        .find_map(|c| c.state.as_ref()?.waiting.as_ref()?.reason.clone());
    if let Some(reason) = waiting {
        return reason;
    }
    let failed = containers.iter().find_map(|c| {
        let t = c.state.as_ref()?.terminated.as_ref()?;
        if t.exit_code != 0 { t.reason.clone() } else { None }
    });
    failed.or_else(|| status.phase.clone()).unwrap_or_default()
}

/// (ready containers, total containers)
fn pod_ready(pod: &Pod) -> (usize, usize) {
    let total = pod.spec.as_ref().map_or(0, |s| s.containers.len());
    let ready = pod
        .status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .map_or(0, |cs| cs.iter().filter(|c| c.ready).count());
    (ready, total)
}

fn pod_restarts(pod: &Pod) -> i32 {
    pod.status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .map_or(0, |cs| cs.iter().map(|c| c.restart_count).sum())
}

/// The container kubectl would pick: the `default-container` annotation, else the first one.
fn default_container(pod: &Pod) -> Option<&Container> {
    let spec = pod.spec.as_ref()?;
    let wanted = pod.annotations().get("kubectl.kubernetes.io/default-container");
    wanted
        .and_then(|n| spec.containers.iter().find(|c| &c.name == n))
        .or(spec.containers.first())
}

/// `registry.example.com/team/app:1.2` -> `team/app:1.2`, and a long digest cut short.
fn short_image(image: &str) -> String {
    let image = match image.split_once('/') {
        Some((host, rest)) if host.contains('.') || host.contains(':') || host == "localhost" => rest,
        _ => image,
    };
    match image.split_once("@sha256:") {
        Some((name, digest)) => format!("{name}@sha256:{}", &digest[..digest.len().min(7)]),
        None => image.to_string(),
    }
}

/// CPU quantity ("250m", "1", "0.5", "1234567n") -> millicores.
fn parse_cpu(q: &str) -> Option<u64> {
    let (num, per_milli) = if let Some(n) = q.strip_suffix('n') {
        (n, 1e-6)
    } else if let Some(n) = q.strip_suffix('u') {
        (n, 1e-3)
    } else if let Some(n) = q.strip_suffix('m') {
        (n, 1.0)
    } else {
        (q, 1000.0)
    };
    num.parse::<f64>().ok().map(|v| (v * per_milli).round() as u64)
}

/// Memory quantity ("128Mi", "1G", "1048576") -> bytes.
fn parse_mem(q: &str) -> Option<u64> {
    const UNITS: [(&str, f64); 12] = [
        ("Ki", 1024.0),
        ("Mi", 1048576.0),
        ("Gi", 1073741824.0),
        ("Ti", 1099511627776.0),
        ("Pi", 1125899906842624.0),
        ("Ei", 1152921504606846976.0),
        ("k", 1e3),
        ("M", 1e6),
        ("G", 1e9),
        ("T", 1e12),
        ("P", 1e15),
        ("E", 1e18),
    ];
    let (num, mult) = UNITS
        .iter()
        .find_map(|(suffix, m)| q.strip_suffix(suffix).map(|n| (n, *m)))
        .unwrap_or((q, 1.0));
    num.parse::<f64>().ok().map(|v| (v * mult) as u64)
}

fn cpu_text(milli: Option<u64>) -> String {
    milli.map_or("-".into(), |m| format!("{m}m"))
}

const BAR_CELLS: usize = 6;

/// Braille meter: each cell holds 8 dots, so 6 cells give 48 steps. Scale is the limit,
/// else the request; with neither (or no metrics) only the dim track shows.
fn usage_bar(used: Option<u64>, req: Option<u64>, lim: Option<u64>) -> Line<'static> {
    const LEVELS: [char; 9] = ['⣀', '⡀', '⣀', '⣄', '⣤', '⣦', '⣶', '⣷', '⣿'];
    let track = Style::new().fg(Color::DarkGray);
    let ratio = match (used, lim.or(req)) {
        (Some(u), Some(d)) if d > 0 => (u as f64 / d as f64).min(1.0),
        _ => return Line::from(Span::styled("⣀".repeat(BAR_CELLS), track)),
    };
    let color = if ratio >= 0.9 { Color::Red } else if ratio >= 0.7 { Color::Yellow } else { Color::Green };
    let dots = (ratio * (BAR_CELLS * 8) as f64).round() as usize;
    let spans = (0..BAR_CELLS).map(|i| {
        let n = dots.saturating_sub(i * 8).min(8);
        Span::styled(LEVELS[n].to_string(), if n == 0 { track } else { Style::new().fg(color) })
    });
    Line::from(spans.collect::<Vec<_>>())
}

fn mem_text(bytes: Option<u64>) -> String {
    match bytes {
        None => "-".into(),
        Some(b) if b >= 1 << 30 => format!("{:.1}Gi", b as f64 / (1u64 << 30) as f64),
        Some(b) if b >= 1 << 20 => format!("{}Mi", b >> 20),
        Some(b) => format!("{}Ki", b >> 10),
    }
}

/// Sum of one resource over all containers; None when no container sets it.
fn sum_resource(pod: &Pod, limits: bool, key: &str, parse: fn(&str) -> Option<u64>) -> Option<u64> {
    pod.spec
        .iter()
        .flat_map(|s| &s.containers)
        .filter_map(|c| {
            let r = c.resources.as_ref()?;
            let map = if limits { r.limits.as_ref() } else { r.requests.as_ref() }?;
            parse(&map.get(key)?.0)
        })
        .reduce(|a, b| a + b)
}

/// The pod-only columns of the list.
struct PodCols {
    ready: String,
    restarts: String,
    image: String, // of the default container
    cpu_req: Option<u64>, // millicores, summed over containers
    cpu_lim: Option<u64>,
    mem_req: Option<u64>, // bytes
    mem_lim: Option<u64>,
}

fn pod_cols(pod: &Pod) -> PodCols {
    let (ready, total) = pod_ready(pod);
    PodCols {
        ready: format!("{ready}/{total}"),
        restarts: pod_restarts(pod).to_string(),
        image: default_container(pod).and_then(|c| c.image.as_deref()).map(short_image).unwrap_or_default(),
        cpu_req: sum_resource(pod, false, "cpu", parse_cpu),
        cpu_lim: sum_resource(pod, true, "cpu", parse_cpu),
        mem_req: sum_resource(pod, false, "memory", parse_mem),
        mem_lim: sum_resource(pod, true, "memory", parse_mem),
    }
}

fn pod_item(pod: &Pod) -> Item {
    Item {
        name: pod.name_any(),
        status: pod_status(pod),
        created: created_secs(&pod.metadata),
        labels: Vec::new(), // the pod list shows image and resources instead
        pod: Some(pod_cols(pod)),
    }
}

/// Live usage of one pod, from metrics-server.
#[derive(Clone, Copy)]
struct Usage {
    cpu: u64, // millicores
    mem: u64, // bytes
}

/// What the background poller learned. `available` is None until the first answer.
#[derive(Clone, Default)]
struct MetricsState {
    available: Option<bool>,
    usage: HashMap<String, Usage>,
}

const POLL_SECS: u64 = 10;
const HISTORY: usize = 60; // samples kept per container: 10 minutes at one per 10s

/// Recent usage per (pod, container), oldest first. Kept apart from `MetricsState` because that
/// is cloned every frame and this is not.
type History = HashMap<(String, String), VecDeque<Usage>>;

/// Ask metrics-server (metrics.k8s.io) for every pod of a namespace: pod -> usage per container.
/// There are no typed structs for it in k8s-openapi, so read it as a dynamic object.
async fn fetch_metrics(client: &Client, ns: &str) -> Result<HashMap<String, Vec<(String, Usage)>>, kube::Error> {
    let gvk = GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics");
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), ns, &ApiResource::from_gvk_with_plural(&gvk, "pods"));
    let list = api.list(&ListParams::default()).await?;
    Ok(list
        .items
        .iter()
        .map(|o| {
            // Indexing a serde_json Value with a missing key gives Null, so no unwraps needed.
            let containers = o.data["containers"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|c| {
                    let cpu = c["usage"]["cpu"].as_str().and_then(parse_cpu).unwrap_or(0);
                    let mem = c["usage"]["memory"].as_str().and_then(parse_mem).unwrap_or(0);
                    (c["name"].as_str().unwrap_or_default().to_string(), Usage { cpu, mem })
                })
                .collect();
            (o.name_any(), containers)
        })
        .collect())
}

fn status_color(s: &str) -> Color {
    match s {
        "Running" | "Active" | "Succeeded" | "Completed" => Color::Green,
        "Pending" | "Terminating" | "ContainerCreating" | "PodInitializing" => Color::Yellow,
        _ if s.starts_with("Init:") => Color::Yellow,
        _ => Color::Red,
    }
}

/// What the details pane shows: the metadata (labels/annotations tables) plus kind-specific info rows.
struct Details {
    meta: ObjectMeta,
    info: Vec<Line<'static>>,
    pod: Option<PodExtra>, // pods also show containers, conditions and events
}

struct ContainerRow {
    raw_name: String, // without the init: prefix
    init: bool,
    name: String,
    state: String,
    color: Color,
    ready: bool,
    restarts: i32,
    last: String,
    image: String,
}

struct CondRow {
    kind: String,
    ok: bool,
    age: String,
    reason: String,
}

struct EventRow {
    age: String,
    kind: String,
    reason: String,
    message: String,
}

struct PodExtra {
    containers: Vec<ContainerRow>,
    conditions: Vec<CondRow>,
    events: Vec<EventRow>, // newest first
}

fn ago(t: &Time) -> String {
    format_age(jiff::Timestamp::now().as_second() - t.0.as_second())
}

/// Init containers first, then the main ones, each joined with its status by name.
fn container_rows(pod: &Pod) -> Vec<ContainerRow> {
    let Some(spec) = &pod.spec else { return vec![] };
    let status = pod.status.as_ref();
    let init = spec.init_containers.iter().flatten().map(|c| (c, true));
    let main = spec.containers.iter().map(|c| (c, false));
    init.chain(main)
        .map(|(c, is_init)| {
            let list = if is_init {
                status.and_then(|s| s.init_container_statuses.as_ref())
            } else {
                status.and_then(|s| s.container_statuses.as_ref())
            };
            let cs = list.and_then(|l| l.iter().find(|s| s.name == c.name));
            let (state, color) = match cs.and_then(|s| s.state.as_ref()) {
                Some(ContainerState { running: Some(r), .. }) => {
                    (format!("Running {}", r.started_at.as_ref().map_or(String::new(), ago)), Color::Green)
                }
                Some(ContainerState { waiting: Some(w), .. }) => {
                    let reason = w.reason.clone().unwrap_or_else(|| "Waiting".into());
                    let color = status_color(&reason);
                    (reason, color)
                }
                Some(ContainerState { terminated: Some(t), .. }) => {
                    let reason = t.reason.clone().unwrap_or_else(|| "Terminated".into());
                    (format!("{reason} ({})", t.exit_code), if t.exit_code == 0 { Color::Green } else { Color::Red })
                }
                _ => ("-".into(), Color::DarkGray),
            };
            let last = cs
                .and_then(|s| s.last_state.as_ref()?.terminated.as_ref())
                .map(|t| {
                    let when = t.finished_at.as_ref().map_or(String::new(), |t| format!(" {} ago", ago(t)));
                    format!("{} ({}){when}", t.reason.clone().unwrap_or_default(), t.exit_code)
                })
                .unwrap_or_default();
            ContainerRow {
                raw_name: c.name.clone(),
                init: is_init,
                name: if is_init { format!("init:{}", c.name) } else { c.name.clone() },
                state,
                color,
                ready: cs.is_some_and(|s| s.ready),
                restarts: cs.map_or(0, |s| s.restart_count),
                last,
                image: short_image(c.image.as_deref().unwrap_or_default()),
            }
        })
        .collect()
}

fn condition_rows(pod: &Pod) -> Vec<CondRow> {
    pod.status
        .iter()
        .flat_map(|s| s.conditions.iter().flatten())
        .map(|c| CondRow {
            kind: c.type_.clone(),
            ok: c.status == "True",
            age: c.last_transition_time.as_ref().map_or(String::new(), ago),
            reason: c.reason.clone().unwrap_or_default(),
        })
        .collect()
}

/// Events about this pod from the namespace's event informer, newest first.
fn pod_events(pod: &Pod, store: &Store<KubeEvent>) -> Vec<EventRow> {
    let (name, uid) = (pod.name_any(), pod.metadata.uid.as_deref());
    let mut found: Vec<(i64, EventRow)> = store
        .state()
        .iter()
        .filter(|e| {
            e.involved_object.name.as_deref() == Some(&name)
                && (uid.is_none() || e.involved_object.uid.as_deref() == uid)
        })
        .map(|e| {
            let at = e
                .last_timestamp
                .as_ref()
                .map(|t| t.0.as_second())
                .or(e.event_time.as_ref().map(|t| t.0.as_second()))
                .unwrap_or_else(|| created_secs(&e.metadata));
            let count = e.count.filter(|c| *c > 1).map_or(String::new(), |c| format!(" (x{c})"));
            let row = EventRow {
                age: format!("{}{count}", format_age(jiff::Timestamp::now().as_second() - at)),
                kind: e.type_.clone().unwrap_or_default(),
                reason: e.reason.clone().unwrap_or_default(),
                message: e.message.clone().unwrap_or_default().replace('\n', " "),
            };
            (at, row)
        })
        .collect();
    found.sort_by_key(|(at, _)| std::cmp::Reverse(*at));
    found.into_iter().map(|(_, r)| r).collect()
}

fn field(k: &str, v: Span<'static>) -> Line<'static> {
    Line::from(vec![Span::styled(format!("{k:<10}"), Style::new().fg(Color::DarkGray)), v])
}

fn status_span(status: &str) -> Span<'static> {
    Span::styled(format!("● {status}"), Style::new().fg(status_color(status)))
}

fn created_text(meta: &ObjectMeta) -> String {
    meta.creation_timestamp.as_ref().map_or(String::new(), |t| {
        format!("{} ({} ago)", t.0, format_age(jiff::Timestamp::now().as_second() - t.0.as_second()))
    })
}

fn ns_details(ns: &Namespace) -> Details {
    let status = ns.status.as_ref().and_then(|s| s.phase.clone()).unwrap_or_default();
    Details {
        meta: ns.metadata.clone(),
        info: vec![
            field("Status", status_span(&status)),
            field("Created", Span::raw(created_text(&ns.metadata))),
            field("UID", Span::raw(ns.metadata.uid.clone().unwrap_or_default())),
        ],
        pod: None,
    }
}

fn pod_details(pod: &Pod, events: &Store<KubeEvent>) -> Details {
    let cols = pod_cols(pod);
    let status = pod.status.as_ref();
    let spec = pod.spec.as_ref();
    let text = |v: Option<&String>| Span::raw(v.cloned().unwrap_or_default());
    let owner = pod
        .owner_references()
        .iter()
        .find(|o| o.controller == Some(true))
        .map_or(String::new(), |o| format!("{}/{}", o.kind, o.name));
    let ips = format!(
        "{} (host {})",
        status.and_then(|s| s.pod_ip.clone()).unwrap_or_default(),
        status.and_then(|s| s.host_ip.clone()).unwrap_or_default()
    );
    Details {
        meta: pod.metadata.clone(),
        info: vec![
            field("Status", status_span(&pod_status(pod))),
            field("Owner", Span::raw(owner)),
            field("Node", text(spec.and_then(|s| s.node_name.as_ref()))),
            field("IP", Span::raw(ips)),
            field("QoS", text(status.and_then(|s| s.qos_class.as_ref()))),
            field("Account", text(spec.and_then(|s| s.service_account_name.as_ref()))),
            field("CPU", Span::raw(format!("req {}  limit {}", cpu_text(cols.cpu_req), cpu_text(cols.cpu_lim)))),
            field("Memory", Span::raw(format!("req {}  limit {}", mem_text(cols.mem_req), mem_text(cols.mem_lim)))),
            field("Created", Span::raw(created_text(&pod.metadata))),
        ],
        pod: Some(PodExtra {
            containers: container_rows(pod),
            conditions: condition_rows(pod),
            events: pod_events(pod, events),
        }),
    }
}

/// Key/value table for a labels/annotations map, with a count in the title.
/// At most `limit` rows (the last one becomes "+N more"); the huge last-applied annotation is skipped.
fn map_table<'a>(title: &str, map: &'a Option<BTreeMap<String, String>>, limit: usize) -> Table<'a> {
    let dim = Style::new().fg(Color::DarkGray);
    let mut rows: Vec<Row> = map
        .iter()
        .flatten()
        .filter(|(k, _)| k.as_str() != "kubectl.kubernetes.io/last-applied-configuration")
        .map(|(k, v)| Row::new([Cell::from(k.as_str()).style(Style::new().fg(Color::Cyan)), Cell::from(v.as_str())]))
        .collect();
    let count = rows.len();
    if count > limit {
        rows.truncate(limit.saturating_sub(1));
        rows.push(Row::new([Cell::from(format!("… +{} more", count + 1 - limit)).style(dim)]));
    }
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

type Hints = &'static [(&'static str, &'static str)];

const LIST_HINTS: Hints = &[
    ("Enter", "pods"),
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
// No ^n for pods: a pod can't be made from a name alone.
const POD_HINTS: Hints = &[
    ("/", "search"),
    ("d", "describe"),
    ("e", "edit"),
    ("y", "yaml"),
    ("^d", "delete"),
    ("< >", "sort column"),
    ("A", "asc/desc"),
    ("Esc", "back to namespaces"),
    ("q", "quit"),
];
const DETAILS_HINTS: Hints = &[("e", "edit"), ("y", "yaml"), ("^n", "new"), ("^d", "delete"), ("Esc", "back")];
const POD_DETAILS_HINTS: Hints = &[("j/k", "select"), ("Enter", "container"), ("e", "edit"), ("y", "yaml"), ("^d", "delete"), ("Esc", "back")];
const CONTAINER_HINTS: Hints = &[("j/k", "scroll env"), ("y", "yaml"), ("Esc", "back")];
const YAML_HINTS: Hints = &[
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

    let help = Line::from(vec![Span::styled("? ", bold.fg(Color::Yellow)), Span::styled("help ", dim)]).right_aligned();
    frame.render_widget(Paragraph::new(help), trail);
}

/// Details pane. Namespaces: info + labels + annotations. Pods: info and conditions on top,
/// containers, events, and small labels/annotations tables at the bottom.
fn render_details(frame: &mut Frame, area: Rect, d: &Details, sel: &mut TableState, message: &str) {
    let dim = Style::new().fg(Color::DarkGray);
    let head = Style::new().add_modifier(Modifier::BOLD).fg(Color::Yellow);
    let outer = Block::bordered().title(" Details ").title_bottom(format!(" {message} "));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);
    let info_box = Paragraph::new(d.info.clone()).block(Block::bordered().border_style(dim).title(" Info "));

    let Some(x) = &d.pod else {
        let [info, labels, annotations] =
            Layout::vertical([Constraint::Length(d.info.len() as u16 + 2), Constraint::Fill(1), Constraint::Fill(1)])
                .areas(inner);
        frame.render_widget(info_box, info);
        frame.render_widget(map_table("Labels", &d.meta.labels, usize::MAX), labels);
        frame.render_widget(map_table("Annotations", &d.meta.annotations, usize::MAX), annotations);
        return;
    };

    let [top, containers, events, meta] = Layout::vertical([
        Constraint::Length(d.info.len().max(x.conditions.len() + 1) as u16 + 2),
        Constraint::Length(x.containers.len() as u16 + 3),
        Constraint::Fill(1),
        Constraint::Length(6),
    ])
    .areas(inner);
    let [info, conds] = Layout::horizontal([Constraint::Percentage(55), Constraint::Fill(1)]).areas(top);
    frame.render_widget(info_box, info);

    let cond_rows = x.conditions.iter().map(|c| {
        let mark = if c.ok { Span::styled("✓", Style::new().fg(Color::Green)) } else { Span::styled("✗", Style::new().fg(Color::Red)) };
        Row::new([Cell::from(Line::from(vec![mark, Span::raw(format!(" {}", c.kind))])), Cell::from(c.age.clone()), Cell::from(c.reason.clone())])
    });
    frame.render_widget(
        Table::new(cond_rows, [Constraint::Length(20), Constraint::Length(6), Constraint::Fill(1)])
            .header(Row::new(["CONDITION", "AGE", "REASON"]).style(head))
            .block(Block::bordered().border_style(dim).title(" Conditions ")),
        conds,
    );

    let container_rows: Vec<Row> = x.containers.iter().map(|c| {
        let ready = if c.ready { Span::styled("✓", Style::new().fg(Color::Green)) } else { Span::styled("✗", Style::new().fg(Color::Red)) };
        Row::new([
            Cell::from(c.name.clone()),
            Cell::from(c.state.clone()).style(Style::new().fg(c.color)),
            Cell::from(ready),
            Cell::from(c.restarts.to_string()),
            Cell::from(c.last.clone()),
            Cell::from(c.image.clone()),
        ])
    }).collect();
    frame.render_stateful_widget(
        Table::new(
            container_rows,
            [Constraint::Fill(2), Constraint::Length(24), Constraint::Length(5), Constraint::Length(8), Constraint::Fill(2), Constraint::Fill(3)],
        )
        .header(Row::new(["NAME", "STATE", "READY", "RESTARTS", "LAST STATE", "IMAGE"]).style(head))
        .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED))
        .block(Block::bordered().border_style(dim).title(format!(" Containers ({}) ", x.containers.len()))),
        containers,
        sel,
    );

    let mut event_rows: Vec<Row> = x
        .events
        .iter()
        .map(|e| {
            let color = if e.kind == "Warning" { Color::Red } else { Color::Reset };
            Row::new([Cell::from(e.age.clone()), Cell::from(e.kind.clone()), Cell::from(e.reason.clone()), Cell::from(e.message.clone())])
                .style(Style::new().fg(color))
        })
        .collect();
    if event_rows.is_empty() {
        event_rows.push(Row::new([Cell::from("<none>").style(dim)]));
    }
    frame.render_widget(
        Table::new(event_rows, [Constraint::Length(12), Constraint::Length(8), Constraint::Length(20), Constraint::Fill(1)])
            .header(Row::new(["AGE", "TYPE", "REASON", "MESSAGE"]).style(head))
            .block(Block::bordered().border_style(dim).title(format!(" Events ({}) ", x.events.len()))),
        events,
    );

    let [labels, annotations] = Layout::horizontal([Constraint::Percentage(50), Constraint::Fill(1)]).areas(meta);
    frame.render_widget(map_table("Labels", &d.meta.labels, 3), labels);
    frame.render_widget(map_table("Annotations", &d.meta.annotations, 3), annotations);
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

    /// Step through ALL with wrap-around. `delta` is +1 or -1.
    fn step(self, delta: isize) -> Column {
        let i = Column::ALL.iter().position(|&c| c == self).unwrap() as isize;
        Column::ALL[(i + delta).rem_euclid(Column::ALL.len() as isize) as usize]
    }
}

/// Cursor, search and sort of one list. Each level (namespaces, pods) keeps its own,
/// so a search typed for pods never filters namespaces and the namespace cursor survives a trip down.
struct ListView {
    table: TableState,
    query: String,
    sort_col: Column,
    ascending: bool,
}

impl ListView {
    fn new() -> Self {
        Self { table: TableState::default().with_selected(Some(0)), query: String::new(), sort_col: Column::Name, ascending: true }
    }
}

/// The pod list of one namespace: its own informer, torn down when you go back.
struct PodLevel {
    ns: String,
    api: Api<Pod>,
    store: Store<Pod>,
    task: tokio::task::JoinHandle<()>, // drives the watcher stream
    loading: u64,                      // Activity id of the "loading pods" spinner
    metrics: Arc<Mutex<MetricsState>>, // written by `metrics_task`, read every frame
    metrics_task: tokio::task::JoinHandle<()>,
    history: Arc<Mutex<History>>, // written by `metrics_task`, read by the container pane
    events: Store<KubeEvent>, // pod events of the namespace, for the details view
    events_task: tokio::task::JoinHandle<()>,
}

impl Drop for PodLevel {
    fn drop(&mut self) {
        // Dropping a JoinHandle only detaches the task; without abort the watch would run forever.
        self.task.abort();
        self.metrics_task.abort();
        self.events_task.abort();
    }
}

fn open_pods(
    client: &Client,
    rt: &tokio::runtime::Handle,
    tx: &Sender<Done>,
    activity: &mut Activity,
    ns: String,
) -> PodLevel {
    let api: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let (store, writer) = reflector::store();
    let stream = reflector(writer, watcher(api.clone(), watcher::Config::default()))
        .default_backoff()
        .applied_objects();
    let task = rt.spawn(stream.for_each(|_| async {}));
    // Second informer: only events about pods (server-side field selector).
    let (events, event_writer) = reflector::store();
    let event_stream = reflector(
        event_writer,
        watcher(Api::<KubeEvent>::namespaced(client.clone(), &ns), watcher::Config::default().fields("involvedObject.kind=Pod")),
    )
    .default_backoff()
    .applied_objects();
    let events_task = rt.spawn(event_stream.for_each(|_| async {}));
    let loading = activity.start(format!("loading pods in {ns}"));
    {
        let (store, tx) = (store.clone(), tx.clone());
        rt.spawn(async move {
            // Ok when the first list arrived, Err when the level was closed first. Either way, stop the spinner.
            let _ = store.wait_until_ready().await;
            let _ = tx.send(Done { id: loading, message: None });
        });
    }
    // metrics-server only samples every ~15-60s, so poll slowly. Errors mean "not available".
    let metrics = Arc::new(Mutex::new(MetricsState::default()));
    let history: Arc<Mutex<History>> = Arc::default();
    let metrics_task = {
        let (client, ns, metrics, history) = (client.clone(), ns.clone(), metrics.clone(), history.clone());
        rt.spawn(async move {
            loop {
                let result = fetch_metrics(&client, &ns).await;
                if let Ok(pods) = &result {
                    let mut h = history.lock().unwrap();
                    for (pod, containers) in pods {
                        for (name, u) in containers {
                            let samples = h.entry((pod.clone(), name.clone())).or_default();
                            if samples.len() == HISTORY {
                                samples.pop_front();
                            }
                            samples.push_back(*u);
                        }
                    }
                    // Forget pods and containers that are gone, or the map only ever grows.
                    h.retain(|(pod, name), _| pods.get(pod).is_some_and(|cs| cs.iter().any(|(n, _)| n == name)));
                }
                {
                    // The lock is released at the end of this block, before the sleep.
                    let mut m = metrics.lock().unwrap();
                    m.available = Some(result.is_ok());
                    m.usage = result
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(pod, cs)| {
                            let total = cs.iter().fold(Usage { cpu: 0, mem: 0 }, |a, (_, u)| Usage { cpu: a.cpu + u.cpu, mem: a.mem + u.mem });
                            (pod, total)
                        })
                        .collect();
                }
                tokio::time::sleep(Duration::from_secs(POLL_SECS)).await;
            }
        })
    };
    PodLevel { ns, api, store, task, loading, metrics, metrics_task, history, events, events_task }
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
        let client = client.clone();
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
    tokio::task::spawn_blocking(move || {
        ratatui::run(|terminal| run_app(terminal, &store, &api, &client, &rt, &info))
    })
    .await??;
    Ok(())
}

/// A popup that grabs all keys until closed.
enum Modal {
    Delete { name: String, kind: &'static str, yes: bool }, // `yes` = which button is focused
    New(String),                                            // name typed so far
    Help(Hints),                                            // key list of the current pane
}

fn render_modal(frame: &mut Frame, modal: &Modal) {
    match modal {
        Modal::Delete { name, kind, yes } => {
            let area = frame.area().centered(Constraint::Percentage(50), Constraint::Length(5));
            frame.render_widget(Clear, area);
            let button = |label: &str, focused: bool| {
                let style = if focused { Style::new().add_modifier(Modifier::REVERSED) } else { Style::new() };
                Span::styled(format!(" {label} "), style)
            };
            let text = vec![
                Line::from(format!("Delete {kind} \"{name}\"?")),
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

async fn delete_obj<K: Kind>(api: &Api<K>, name: &str) -> String {
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => format!("{name} deleting"), // namespaces go Terminating first, pods have a grace period
        Err(e) => format!("delete failed: {e}"),
    }
}

async fn replace_obj<K: Kind>(api: &Api<K>, name: &str, obj: &K) -> String {
    match api.replace(name, &PostParams::default(), obj).await {
        Ok(_) => format!("{name} updated"),
        Err(e) => format!("update failed: {e}"),
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

/// Open the object as YAML in $EDITOR and parse the result.
/// Returns the edited object, or None when nothing changed.
fn edit_obj<K: Kind>(terminal: &mut DefaultTerminal, obj: &K) -> anyhow::Result<Option<(String, K)>> {
    let name = obj.name_any();
    let mut copy = obj.clone();
    copy.meta_mut().managed_fields = None; // noise, kubectl hides it too
    let before = serde_yaml_ng::to_string(&copy)?;
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

/// Edit `name` from `store` in $EDITOR, then save it in the background.
fn edit_flow<K: Kind>(
    terminal: &mut DefaultTerminal,
    rt: &tokio::runtime::Handle,
    tx: &Sender<Done>,
    activity: &mut Activity,
    api: &Api<K>,
    store: &Store<K>,
    name: &str,
    message: &mut String,
) {
    let Some(obj) = find_in(store, name) else { return };
    match edit_obj(terminal, &*obj) {
        Ok(Some((name, edited))) => {
            let api = api.clone();
            let label = format!("updating {name}");
            spawn_op(rt, tx, activity, label, async move { replace_obj(&api, &name, &edited).await });
        }
        Ok(None) => *message = "no changes".into(),
        Err(e) => *message = format!("edit failed: {e:#}"),
    }
}

fn find_in<K: Kind>(store: &Store<K>, name: &str) -> Option<Arc<K>> {
    store.state().into_iter().find(|o| o.meta().name.as_deref() == Some(name))
}

/// Items matching `query`, best fuzzy score first. Empty query keeps original order.
fn filter<'a>(items: &'a [Item], query: &str, matcher: &SkimMatcherV2) -> Vec<&'a Item> {
    if query.is_empty() {
        return items.iter().collect();
    }
    let mut scored: Vec<(i64, &Item)> = items
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

/// The table for either level. Pods get READY and RESTARTS columns on top of the shared ones.
fn render_list(
    frame: &mut Frame,
    area: Rect,
    metrics: Option<&MetricsState>, // Some = pod list
    visible: &[&Item],
    total: usize,
    list: &mut ListView,
    status: &str,
) {
    let pods = metrics.is_some();
    let now = jiff::Timestamp::now().as_second();
    let labels_cell = |n: &Item| {
        let mut spans = Vec::new();
        for (k, v) in &n.labels {
            spans.push(pill(k, v));
            spans.push(Span::raw(" "));
        }
        Cell::from(Line::from(spans))
    };
    let status_cell = |n: &Item| Cell::from(n.status.clone()).style(Style::new().fg(status_color(&n.status)));

    // (title, which sort column it is, width)
    let columns: Vec<(&str, Option<Column>, Constraint)> = if pods {
        vec![
            ("NAME", Some(Column::Name), Constraint::Fill(3)),
            ("READY", None, Constraint::Length(6)),
            ("STATUS", Some(Column::Status), Constraint::Length(17)),
            ("RESTARTS", None, Constraint::Length(8)),
            ("CPU", None, Constraint::Length(BAR_CELLS as u16)),
            ("REQ", None, Constraint::Length(6)),
            ("LIM", None, Constraint::Length(6)),
            ("MEM", None, Constraint::Length(BAR_CELLS as u16)),
            ("REQ", None, Constraint::Length(7)),
            ("LIM", None, Constraint::Length(7)),
            ("IMAGE", None, Constraint::Fill(3)),
            ("AGE", Some(Column::Age), Constraint::Length(5)),
        ]
    } else {
        vec![
            ("NAME", Some(Column::Name), Constraint::Fill(1)),
            ("STATUS", Some(Column::Status), Constraint::Length(12)),
            ("AGE", Some(Column::Age), Constraint::Length(6)),
            ("LABELS", None, Constraint::Fill(2)),
        ]
    };
    let rows = visible.iter().map(|n| {
        let age = Cell::from(format_age(now - n.created));
        match (&n.pod, metrics) {
            (Some(p), Some(m)) => {
                let u = m.usage.get(&n.name);
                Row::new([
                    Cell::from(n.name.clone()),
                    Cell::from(p.ready.clone()),
                    status_cell(n),
                    Cell::from(p.restarts.clone()),
                    Cell::from(usage_bar(u.map(|u| u.cpu), p.cpu_req, p.cpu_lim)),
                    Cell::from(cpu_text(p.cpu_req)),
                    Cell::from(cpu_text(p.cpu_lim)),
                    Cell::from(usage_bar(u.map(|u| u.mem), p.mem_req, p.mem_lim)),
                    Cell::from(mem_text(p.mem_req)),
                    Cell::from(mem_text(p.mem_lim)),
                    Cell::from(p.image.clone()),
                    age,
                ])
            }
            _ => Row::new([Cell::from(n.name.clone()), status_cell(n), age, labels_cell(n)]),
        }
    });
    let header = Row::new(columns.iter().map(|(title, col, _)| match col {
        Some(c) if *c == list.sort_col && list.query.is_empty() => {
            format!("{title} {}", if list.ascending { "▲" } else { "▼" })
        }
        _ => title.to_string(),
    }))
    .style(Style::new().add_modifier(Modifier::BOLD));
    let widths: Vec<Constraint> = columns.iter().map(|(_, _, w)| *w).collect();
    // Pod lists say whether live usage is coming from metrics-server.
    let metrics_tag = match metrics.map(|m| m.available) {
        Some(Some(true)) => Some(Span::styled(" metrics ✓ ", Style::new().fg(Color::Green))),
        Some(Some(false)) => Some(Span::styled(" metrics n/a ", Style::new().fg(Color::DarkGray))),
        Some(None) => Some(Span::styled(" metrics … ", Style::new().fg(Color::DarkGray))),
        None => None,
    };
    let mut block = Block::bordered().title(format!(" {}/{} ", visible.len(), total)).title_bottom(format!(" {status} "));
    if let Some(tag) = metrics_tag {
        block = block.title(Line::from(tag).right_aligned());
    }
    let table = Table::new(rows, widths)
        .header(header)
        .block(block)
        .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED));
    frame.render_stateful_widget(table, area, &mut list.table);
}

fn run_app(
    terminal: &mut DefaultTerminal,
    store: &Store<Namespace>,
    api: &Api<Namespace>,
    client: &Client,
    rt: &tokio::runtime::Handle,
    info: &HeaderInfo,
) -> std::io::Result<()> {
    let matcher = SkimMatcherV2::default();
    // lists[0] is namespaces; a pod level pushes a second one and pops it on the way back.
    let mut lists = vec![ListView::new()];
    let mut pods: Option<PodLevel> = None;
    let mut typing = false; // true while the search popup is open
    let mut viewing: Option<String> = None; // object shown in the details view (of the active level)
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
    let mut modal: Option<Modal> = None; // delete/new/help popup, when open
    let mut container_sel = TableState::default(); // selected row of the details Containers table
    let mut cview: Option<(String, bool)> = None; // container pane: (name, is init)
    let mut env_sel = TableState::default(); // scroll position of the container's env table
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
        let in_pods = pods.is_some();
        let kind = if in_pods { "pod" } else { "namespace" };

        // Snapshot of the active level's informer cache; changes as watch events arrive.
        let items: Vec<Item> = match &pods {
            Some(p) => p.store.state().iter().map(|o| pod_item(o)).collect(),
            None => store.state().iter().map(|o| ns_item(o)).collect(),
        };
        // Copy of the latest usage numbers (cloned so the lock isn't held while drawing).
        let metrics: Option<MetricsState> = pods.as_ref().map(|p| p.metrics.lock().unwrap().clone());
        let list = lists.last_mut().unwrap();
        let mut visible = filter(&items, &list.query, &matcher);
        // With a query, fuzzy rank wins; otherwise apply the chosen sort.
        if list.query.is_empty() {
            visible.sort_by(|a, b| match list.sort_col {
                Column::Name => a.name.cmp(&b.name),
                Column::Status => a.status.cmp(&b.status),
                Column::Age => b.created.cmp(&a.created), // newer = younger = "smaller" age
            });
            if !list.ascending {
                visible.reverse();
            }
        }
        let selected = list.table.selected().and_then(|i| visible.get(i)).map(|n| n.name.clone());

        // Details are looked up live each frame; if the object vanished, go back to the list.
        let details: Option<Details> = viewing.as_ref().and_then(|n| match &pods {
            Some(p) => find_in(&p.store, n).map(|o| pod_details(&o, &p.events)),
            None => find_in(store, n).map(|o| ns_details(&o)),
        });
        if details.is_none() {
            viewing = None;
        }
        let cdetails = match (&cview, &viewing, &pods) {
            (Some((c, init)), Some(v), Some(p)) => find_in(&p.store, v).and_then(|o| container_view::build(&o, c, *init)),
            _ => None,
        };
        if cdetails.is_none() {
            cview = None;
        }
        // Usage history of the open container; None when metrics-server has nothing for it.
        let samples: Option<Vec<Usage>> = match (&cview, &viewing, &pods) {
            (Some((c, _)), Some(v), Some(p)) => {
                p.history.lock().unwrap().get(&(v.clone(), c.clone())).map(|d| d.iter().copied().collect())
            }
            _ => None,
        };

        let mut crumbs: Vec<String> = vec!["Namespaces".into()];
        if let Some(p) = &pods {
            crumbs.push(p.ns.clone());
            crumbs.push("Pods".into());
        }
        let hints: Hints = if let Some(v) = &yaml {
            crumbs.push(v.name());
            crumbs.push("YAML".into());
            YAML_HINTS
        } else if let Some(n) = &viewing {
            crumbs.push(n.clone());
            crumbs.push("Details".into());
            if let Some((c, _)) = &cview {
                crumbs.push(c.clone());
            }
            if cview.is_some() {
                CONTAINER_HINTS
            } else if in_pods { POD_DETAILS_HINTS } else { DETAILS_HINTS }
        } else if in_pods {
            POD_HINTS
        } else {
            LIST_HINTS
        };

        terminal.draw(|frame| {
            let [head, body] =
                Layout::vertical([Constraint::Length(HEADER_HEIGHT), Constraint::Fill(1)]).areas(frame.area());
            render_header(frame, head, info, &crumbs);
            if let Some(v) = &mut yaml {
                v.render(frame, body, &status);
            } else if let Some(cd) = &cdetails {
                container_view::render(frame, body, cd, samples.as_deref(), &mut env_sel, &status);
            } else if let Some(d) = &details {
                render_details(frame, body, d, &mut container_sel, &status);
            } else {
                render_list(frame, body, metrics.as_ref(), &visible, items.len(), list, &status);
            }
            if let Some(m) = &modal {
                render_modal(frame, m);
            }
            if typing && yaml.is_none() && details.is_none() {
                // Floating box: 50% wide, 3 rows tall, centered.
                let popup = frame.area().centered(Constraint::Percentage(50), Constraint::Length(3));
                frame.render_widget(Clear, popup); // wipe what's underneath
                frame.render_widget(
                    Paragraph::new(list.query.as_str()).block(Block::bordered().title(" Search ")),
                    popup,
                );
                // +1 skips the border; cursor sits after the typed text.
                frame.set_cursor_position((popup.x + 1 + list.query.chars().count() as u16, popup.y + 1));
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
                Modal::Delete { name, kind, yes } => match key.code {
                    code if code == KeyCode::Char('y') || (code == KeyCode::Enter && yes) => {
                        let label = format!("deleting {name}");
                        match &pods {
                            Some(p) => {
                                let api = p.api.clone();
                                spawn_op(rt, &tx, &mut activity, label, async move { delete_obj(&api, &name).await });
                            }
                            None => {
                                let api = api.clone();
                                spawn_op(rt, &tx, &mut activity, label, async move { delete_obj(&api, &name).await });
                            }
                        }
                        viewing = None;
                        None
                    }
                    KeyCode::Char('n') | KeyCode::Enter | KeyCode::Esc => None,
                    KeyCode::Left | KeyCode::Right | KeyCode::Tab | KeyCode::Char('h' | 'l') => {
                        Some(Modal::Delete { name, kind, yes: !yes })
                    }
                    _ => Some(Modal::Delete { name, kind, yes }),
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
        let mut enter_pods: Option<String> = None;
        let mut go_back = false;
        if cview.is_some() {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => cview = None,
                KeyCode::Char('j') | KeyCode::Down => env_sel.select_next(),
                KeyCode::Char('k') | KeyCode::Up => env_sel.select_previous(),
                KeyCode::Char('g') => env_sel.select_first(),
                KeyCode::Char('G') => env_sel.select_last(),
                KeyCode::Char('y') => {
                    if let (Some(cd), Some(pod), Some((c, _))) = (&cdetails, &viewing, &cview) {
                        yaml = Some(YamlView::new(format!("{pod}/{c}"), cd.yaml.clone()));
                    }
                }
                _ => {}
            }
        } else if viewing.is_some() {
            match key.code {
                KeyCode::Char('j') | KeyCode::Down if in_pods => container_sel.select_next(),
                KeyCode::Char('k') | KeyCode::Up if in_pods => container_sel.select_previous(),
                KeyCode::Enter if in_pods => {
                    let rows = details.as_ref().and_then(|d| d.pod.as_ref()).map(|x| &x.containers);
                    let i = container_sel.selected().unwrap_or(0);
                    // select_next can run past the end; the table clamps it only when drawing.
                    if let Some(r) = rows.and_then(|r| r.get(i.min(r.len().saturating_sub(1)))) {
                        cview = Some((r.raw_name.clone(), r.init));
                        env_sel = TableState::default();
                    }
                }
                KeyCode::Char('d') if ctrl => {
                    modal = viewing.clone().map(|name| Modal::Delete { name, kind, yes: false });
                }
                KeyCode::Char('n') if ctrl && !in_pods => modal = Some(Modal::New(String::new())),
                KeyCode::Esc | KeyCode::Char('q' | 'd') => viewing = None,
                KeyCode::Char('e') => edit_target = viewing.clone(),
                KeyCode::Char('y') => yaml_target = viewing.clone(),
                _ => {}
            }
        } else if typing {
            match key.code {
                KeyCode::Char(c) if !ctrl => {
                    list.query.push(c);
                    list.table.select_first();
                }
                KeyCode::Backspace => {
                    list.query.pop();
                    list.table.select_first();
                }
                KeyCode::Enter => typing = false, // keep filter, go back to navigating
                KeyCode::Esc => {
                    list.query.clear();
                    typing = false;
                }
                _ => {}
            }
        } else {
            match key.code {
                KeyCode::Char('d') if ctrl => {
                    modal = selected.clone().map(|name| Modal::Delete { name, kind, yes: false });
                }
                KeyCode::Char('n') if ctrl && !in_pods => modal = Some(Modal::New(String::new())),
                KeyCode::Char('/') => typing = true,
                KeyCode::Char('>') => list.sort_col = list.sort_col.step(1),
                KeyCode::Char('<') => list.sort_col = list.sort_col.step(-1),
                KeyCode::Char('A') => list.ascending = !list.ascending,
                KeyCode::Char('d') => {
                    viewing = selected.clone();
                    container_sel.select_first();
                }
                KeyCode::Char('y') => yaml_target = selected.clone(),
                KeyCode::Char('e') => edit_target = selected.clone(),
                KeyCode::Enter if !in_pods => enter_pods = selected.clone(),
                KeyCode::Char('q') => return Ok(()),
                KeyCode::Esc if list.query.is_empty() => {
                    if in_pods {
                        go_back = true;
                    } else {
                        return Ok(());
                    }
                }
                KeyCode::Esc => list.query.clear(), // first Esc drops the filter
                KeyCode::Char('j') | KeyCode::Down => list.table.select_next(),
                KeyCode::Char('k') | KeyCode::Up => list.table.select_previous(),
                _ => {}
            }
        }

        if go_back {
            if let Some(p) = pods.take() {
                activity.finish(p.loading); // dropping `p` aborts its watcher
            }
            lists.pop();
        }
        if let Some(ns) = enter_pods {
            pods = Some(open_pods(client, rt, &tx, &mut activity, ns));
            lists.push(ListView::new());
        }
        if let Some(name) = yaml_target {
            let value = match &pods {
                Some(p) => find_in(&p.store, &name).and_then(|o| serde_yaml_ng::to_value(&*o).ok()),
                None => find_in(store, &name).and_then(|o| serde_yaml_ng::to_value(&*o).ok()),
            };
            yaml = value.map(|v| YamlView::new(name, v));
        }
        if let Some(name) = edit_target {
            match &pods {
                Some(p) => edit_flow(terminal, rt, &tx, &mut activity, &p.api, &p.store, &name, &mut message),
                None => edit_flow(terminal, rt, &tx, &mut activity, api, store, &name, &mut message),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateWaiting, ContainerStatus, PodStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;

    fn pod_with(phase: &str, waiting: Option<&str>) -> Pod {
        let state = waiting.map(|r| ContainerState {
            waiting: Some(ContainerStateWaiting { reason: Some(r.into()), ..Default::default() }),
            ..Default::default()
        });
        Pod {
            status: Some(PodStatus {
                phase: Some(phase.into()),
                container_statuses: Some(vec![ContainerStatus { state, ..Default::default() }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn pod_status_prefers_container_reason_over_phase() {
        assert_eq!(pod_status(&pod_with("Running", None)), "Running");
        assert_eq!(pod_status(&pod_with("Running", Some("CrashLoopBackOff"))), "CrashLoopBackOff");

        let mut p = pod_with("Running", None);
        p.metadata.deletion_timestamp = Some(Time(jiff::Timestamp::now()));
        assert_eq!(pod_status(&p), "Terminating");
    }

    #[test]
    fn bar_fills_by_limit_then_request() {
        let text = |l: Line| l.spans.iter().map(|s| s.content.to_string()).collect::<String>();
        assert_eq!(text(usage_bar(Some(500), None, Some(500))), "⣿⣿⣿⣿⣿⣿");
        assert_eq!(text(usage_bar(Some(250), Some(100), Some(500))), "⣿⣿⣿⣀⣀⣀");
        assert_eq!(text(usage_bar(None, Some(1), Some(1))), "⣀⣀⣀⣀⣀⣀");
        assert_eq!(text(usage_bar(Some(5), None, None)), "⣀⣀⣀⣀⣀⣀");
    }

    #[test]
    fn container_rows_show_state_and_last_termination() {
        let pod: Pod = serde_yaml_ng::from_str(r#"{
            "metadata": {"name": "p"},
            "spec": {"containers": [{"name": "app", "image": "reg.io/team/app:1"}]},
            "status": {"containerStatuses": [{
                "name": "app", "ready": false, "restartCount": 4, "image": "x", "imageID": "x",
                "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                "lastState": {"terminated": {"exitCode": 137, "reason": "OOMKilled"}}
            }]}
        }"#)
        .unwrap();
        let rows = container_rows(&pod);
        assert_eq!(rows[0].state, "CrashLoopBackOff");
        assert_eq!(rows[0].last, "OOMKilled (137)");
        assert_eq!((rows[0].restarts, rows[0].image.as_str()), (4, "team/app:1"));
    }

    #[test]
    fn container_view_collects_spec_fields() {
        let pod: Pod = serde_yaml_ng::from_str(r#"{
            "metadata": {"name": "p"},
            "spec": {"containers": [{
                "name": "app", "image": "reg.io/app:1", "command": ["sh", "-c"], "args": ["echo hi there"],
                "env": [{"name": "A", "value": "1"}, {"name": "S", "valueFrom": {"secretKeyRef": {"name": "db", "key": "pw"}}}],
                "ports": [{"containerPort": 8080}],
                "resources": {"requests": {"memory": "64Mi", "cpu": "100m"}, "limits": {"memory": "128Mi"}},
                "livenessProbe": {"httpGet": {"path": "/h", "port": 8080}}
            }]}
        }"#)
        .unwrap();
        let d = container_view::build(&pod, "app", false).unwrap();
        assert!(container_view::build(&pod, "nope", false).is_none());
        assert_eq!(d.yaml["name"].as_str(), Some("app"));
        d.assert_basics();
    }

    #[test]
    fn quantities_parse_and_format() {
        assert_eq!(parse_cpu("250m"), Some(250));
        assert_eq!(parse_cpu("1"), Some(1000));
        assert_eq!(parse_cpu("0.5"), Some(500));
        assert_eq!(parse_cpu("2500000n"), Some(3)); // metrics-server reports nanocores
        assert_eq!(parse_mem("128Mi"), Some(128 << 20));
        assert_eq!(parse_mem("1G"), Some(1_000_000_000));
        assert_eq!(parse_mem("2048"), Some(2048));
        assert_eq!(parse_mem("abc"), None);
        assert_eq!(mem_text(Some(1536 << 20)), "1.5Gi");
        assert_eq!(mem_text(Some(64 << 20)), "64Mi");
        assert_eq!(cpu_text(None), "-");
        assert_eq!(short_image("registry.io:5000/team/app:1.2"), "team/app:1.2");
        assert_eq!(short_image("nginx:1.25"), "nginx:1.25");
    }
}
