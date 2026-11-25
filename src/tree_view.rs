//! Resource tree of an Argo CD Application or a Helm release: the managed resources, and under the
//! workloads what they own (Deployment -> ReplicaSet -> Pod). The ownership part comes straight
//! from the cluster, so the tree needs no Argo CD API access.

use crate::{pod_ready, pod_status, status_color};
use futures::future::join_all;
use k8s_openapi::{
    api::{
        apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet},
        batch::v1::Job,
        core::v1::Pod,
    },
    serde::Deserialize,
    serde_json::Value,
};
use kube::{
    Api, Client, ResourceExt,
    api::ListParams,
    core::{ApiResource, DynamicObject, GroupVersionKind},
};
use ratatui::{
    Frame,
    crossterm::event::{KeyCode, KeyEvent},
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, List, ListItem, ListState, Padding, Paragraph},
};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

/// What to draw a tree of.
#[derive(Clone, Debug)]
pub enum Source {
    Argo { ns: String, name: String },
    /// A Helm release: everything is already decoded, the tree only adds live state.
    Helm { ns: String, name: String, chart: String, status: String, revision: u32, manifest: String },
}

#[derive(Clone, Debug, Default)]
pub struct Node {
    pub kind: String,
    pub name: String,
    pub ns: String,
    pub status: String, // health, coloured by `status_color`
    pub note: String,   // sync state, ready counts...
    pub note_color: Color,
    pub children: Vec<Node>,
}

#[derive(Clone, Debug, Default)]
pub struct Tree {
    pub header: Vec<(String, String)>,
    pub root: Node,
}

// ---------- what the cluster holds ----------

#[derive(Default)]
pub struct World {
    pub pods: Vec<Pod>,
    pub replica_sets: Vec<ReplicaSet>,
    pub jobs: Vec<Job>,
    pub deployments: Vec<Deployment>,
    pub stateful_sets: Vec<StatefulSet>,
    pub daemon_sets: Vec<DaemonSet>,
}

async fn all<K>(client: &Client, ns: &str) -> Vec<K>
where
    K: kube::Resource<DynamicType = ()> + Clone + k8s_openapi::serde::de::DeserializeOwned + std::fmt::Debug,
    K: kube::Resource<Scope = kube::core::NamespaceResourceScope>,
{
    Api::<K>::namespaced(client.clone(), ns).list(&ListParams::default()).await.map(|l| l.items).unwrap_or_default()
}

impl World {
    /// Lists in each namespace the tree touches. Failures (RBAC) just leave a kind out.
    pub async fn load(client: &Client, namespaces: &BTreeSet<String>) -> World {
        let parts = join_all(namespaces.iter().map(|ns| async move {
            (
                all::<Pod>(client, ns).await,
                all::<ReplicaSet>(client, ns).await,
                all::<Job>(client, ns).await,
                all::<Deployment>(client, ns).await,
                all::<StatefulSet>(client, ns).await,
                all::<DaemonSet>(client, ns).await,
            )
        }))
        .await;
        let mut w = World::default();
        for (pods, rs, jobs, deployments, sts, ds) in parts {
            w.pods.extend(pods);
            w.replica_sets.extend(rs);
            w.jobs.extend(jobs);
            w.deployments.extend(deployments);
            w.stateful_sets.extend(sts);
            w.daemon_sets.extend(ds);
        }
        w
    }
}

fn node(kind: &str, name: &str, ns: &str, status: &str, note: String) -> Node {
    Node { kind: kind.into(), name: name.into(), ns: ns.into(), status: status.into(), note, note_color: Color::DarkGray, children: vec![] }
}

fn pod_node(p: &Pod) -> Node {
    let (r, t) = pod_ready(p);
    node("Pod", &p.name_any(), &p.namespace().unwrap_or_default(), &pod_status(p), format!("{r}/{t}"))
}

fn rs_node(r: &ReplicaSet) -> Node {
    let want = r.spec.replicas.unwrap_or(1);
    let ready = r.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0);
    let status = if want == 0 { "" } else if ready >= want { "Healthy" } else { "Progressing" };
    node("ReplicaSet", &r.name_any(), &r.namespace().unwrap_or_default(), status, format!("{ready}/{want}"))
}

fn job_node(j: &Job) -> Node {
    let cond = |kind: &str| j.status.iter().flat_map(|s| s.conditions.iter().flatten()).any(|c| c.type_ == kind && c.status == "True");
    let st = j.status.as_ref();
    let status = if cond("Failed") {
        "Failed"
    } else if cond("Complete") {
        "Complete"
    } else if st.and_then(|s| s.active).unwrap_or(0) > 0 {
        "Running"
    } else {
        "Pending"
    };
    node("Job", &j.name_any(), &j.namespace().unwrap_or_default(), status, format!("{}/{}", st.and_then(|s| s.succeeded).unwrap_or(0), j.spec.as_ref().and_then(|s| s.completions).map_or("-".into(), |c| c.to_string())))
}

type Owner = (String, String, String); // namespace, kind, name

/// Who owns what, by the owner's (namespace, kind, name): ownerReferences carry kind and name, so the
/// owner itself need not be fetched.
struct Index {
    owned: HashMap<Owner, Vec<Node>>,
}

fn index(w: &World) -> Index {
    let mut owned: HashMap<Owner, Vec<Node>> = HashMap::new();
    let mut add = |ns: String, owners: &[k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference], n: Node| {
        for o in owners {
            owned.entry((ns.clone(), o.kind.clone(), o.name.clone())).or_default().push(n.clone());
        }
    };
    for p in &w.pods {
        add(p.namespace().unwrap_or_default(), p.owner_references(), pod_node(p));
    }
    for r in &w.replica_sets {
        add(r.namespace().unwrap_or_default(), r.owner_references(), rs_node(r));
    }
    for j in &w.jobs {
        add(j.namespace().unwrap_or_default(), j.owner_references(), job_node(j));
    }
    Index { owned }
}

/// Attach what each node owns, all the way down.
fn grow(n: &mut Node, idx: &Index) {
    let mut kids = idx.owned.get(&(n.ns.clone(), n.kind.clone(), n.name.clone())).cloned().unwrap_or_default();
    for k in &mut kids {
        grow(k, idx);
    }
    // old ReplicaSets scaled to zero are rollout history, not state
    kids.retain(|k| !(k.kind == "ReplicaSet" && k.children.is_empty() && k.note == "0/0"));
    kids.sort_by(|a, b| a.name.cmp(&b.name));
    n.children = kids;
}

/// Live state of a workload named in a Helm manifest; "Missing" when it is not in the cluster.
fn live(w: &World, kind: &str, ns: &str, name: &str) -> Option<(String, String)> {
    let counts = |want: i32, ready: i32| {
        let status = if ready >= want { "Healthy" } else if ready == 0 { "Degraded" } else { "Progressing" };
        (status.to_string(), format!("{ready}/{want}"))
    };
    let missing = || ("Missing".to_string(), String::new());
    let same = |n: &str, nm: &str| n == ns && nm == name;
    match kind {
        "Deployment" => Some(w.deployments.iter().find(|d| same(&d.namespace().unwrap_or_default(), &d.name_any())).map_or_else(missing, |d| {
            counts(d.spec.replicas.unwrap_or(1), d.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0))
        })),
        "StatefulSet" => Some(w.stateful_sets.iter().find(|d| same(&d.namespace().unwrap_or_default(), &d.name_any())).map_or_else(missing, |d| {
            counts(d.spec.replicas.unwrap_or(1), d.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0))
        })),
        "DaemonSet" => Some(w.daemon_sets.iter().find(|d| same(&d.namespace().unwrap_or_default(), &d.name_any())).map_or_else(missing, |d| {
            let (want, ready) = d.status.as_ref().map_or((0, 0), |s| (s.desired_number_scheduled, s.number_ready));
            counts(want, ready)
        })),
        "Job" => Some(w.jobs.iter().find(|j| same(&j.namespace().unwrap_or_default(), &j.name_any())).map_or_else(missing, |j| {
            let n = job_node(j);
            (n.status, n.note)
        })),
        _ => None, // Services, ConfigMaps...: nothing to say without fetching each
    }
}

// ---------- building ----------

fn text(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn sync_color(sync: &str) -> Color {
    match sync {
        "Synced" => Color::Green,
        "OutOfSync" => Color::Yellow,
        _ => Color::DarkGray,
    }
}

pub fn argo_namespaces(app: &DynamicObject) -> BTreeSet<String> {
    let mut set: BTreeSet<String> = app.data["status"]["resources"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|r| text(&r["namespace"]))
        .filter(|n| !n.is_empty())
        .collect();
    if let Some(ns) = app.data["spec"]["destination"]["namespace"].as_str().filter(|n| !n.is_empty()) {
        set.insert(ns.into());
    }
    set
}

/// The Application as root, its managed resources below, what they own below those.
pub fn argo_tree(app: &DynamicObject, w: &World) -> Tree {
    let (spec, st) = (&app.data["spec"], &app.data["status"]);
    let dest = &spec["destination"];
    let default_ns = text(&dest["namespace"]);
    let idx = index(w);
    let mut kids: Vec<Node> = st["resources"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|r| {
            let ns = r["namespace"].as_str().map(String::from).unwrap_or_else(|| default_ns.clone());
            let sync = text(&r["status"]);
            let mut n = node(&text(&r["kind"]), &text(&r["name"]), &ns, &text(&r["health"]["status"]), sync.clone());
            n.note_color = sync_color(&sync);
            grow(&mut n, &idx);
            n
        })
        .collect();
    kids.sort_by(|a, b| (&a.kind, &a.name).cmp(&(&b.kind, &b.name)));
    let sync = text(&st["sync"]["status"]);
    let mut root = node("Application", &app.name_any(), &app.namespace().unwrap_or_default(), &text(&st["health"]["status"]), sync.clone());
    root.note_color = sync_color(&sync);
    root.children = kids;
    let rev = text(&st["sync"]["revision"]);
    Tree {
        header: vec![
            ("Project".into(), text(&spec["project"])),
            ("Destination".into(), format!("{} / {default_ns}", if dest["name"].is_string() { text(&dest["name"]) } else { text(&dest["server"]) })),
            ("Revision".into(), if rev.len() >= 40 { rev[..7].to_string() } else { rev }),
        ],
        root,
    }
}

/// (kind, name, namespace) of every object in a rendered Helm manifest.
pub fn manifest_refs(manifest: &str, release_ns: &str) -> Vec<(String, String, String)> {
    serde_yaml_ng::Deserializer::from_str(manifest)
        .filter_map(|doc| Value::deserialize(doc).ok())
        .filter(|v| v["kind"].is_string() && v["metadata"]["name"].is_string())
        .map(|v| (text(&v["kind"]), text(&v["metadata"]["name"]), v["metadata"]["namespace"].as_str().unwrap_or(release_ns).to_string()))
        .collect()
}

pub fn helm_namespaces(ns: &str, manifest: &str) -> BTreeSet<String> {
    manifest_refs(manifest, ns).into_iter().map(|(_, _, n)| n).collect()
}

pub fn helm_tree(src: &Source, w: &World) -> Tree {
    let Source::Helm { ns, name, chart, status, revision, manifest } = src else { return Tree::default() };
    let idx = index(w);
    let mut kids: Vec<Node> = manifest_refs(manifest, ns)
        .into_iter()
        .map(|(kind, name, n_ns)| {
            let (st, note) = live(w, &kind, &n_ns, &name).unwrap_or_default();
            let mut n = node(&kind, &name, &n_ns, &st, note);
            grow(&mut n, &idx);
            n
        })
        .collect();
    kids.sort_by(|a, b| (&a.kind, &a.name).cmp(&(&b.kind, &b.name)));
    let mut root = node("Release", name, ns, "", format!("rev {revision}"));
    root.status = match status.as_str() {
        "deployed" => "Healthy".into(),
        s if s.starts_with("pending") => "Progressing".into(),
        "failed" => "Failed".into(),
        s => s.into(),
    };
    root.children = kids;
    Tree { header: vec![("Chart".into(), chart.clone()), ("Status".into(), status.clone()), ("Namespace".into(), ns.clone())], root }
}

async fn build(client: &Client, src: &Source) -> Result<Tree, String> {
    match src {
        Source::Argo { ns, name } => {
            let gvk = GroupVersionKind::gvk("argoproj.io", "v1alpha1", "Application");
            let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), ns, &ApiResource::from_gvk_with_plural(&gvk, "applications"));
            let app = api.get(name).await.map_err(|e| e.to_string())?;
            let world = World::load(client, &argo_namespaces(&app)).await;
            Ok(argo_tree(&app, &world))
        }
        Source::Helm { ns, manifest, .. } => Ok(helm_tree(src, &World::load(client, &helm_namespaces(ns, manifest)).await)),
    }
}

// ---------- screen ----------

pub enum Act {
    Close,
    Quit,
    Command,
}

pub struct TreeView {
    shared: Arc<Mutex<Option<Result<Tree, String>>>>,
    task: tokio::task::JoinHandle<()>,
    title: String,
    cursor: usize,
    folded: HashSet<String>,
    state: ListState,
}

impl Drop for TreeView {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Line_ {
    id: String,
    prefix: String,
    folded: bool,
    has_kids: bool,
    node: Node,
}

/// The visible nodes in order, each with its box-drawing prefix. A folded node hides its children.
fn flatten(tree: &Tree, folded: &HashSet<String>) -> Vec<Line_> {
    fn walk(n: &Node, id: String, trail: &str, connector: &str, folded: &HashSet<String>, out: &mut Vec<Line_>) {
        let is_folded = folded.contains(&id);
        out.push(Line_ { id: id.clone(), prefix: format!("{trail}{connector}"), folded: is_folded, has_kids: !n.children.is_empty(), node: Node { children: vec![], ..n.clone() } });
        if is_folded {
            return;
        }
        let next = format!("{trail}{}", match connector { "├─ " => "│  ", "└─ " => "   ", _ => "" });
        for (i, c) in n.children.iter().enumerate() {
            let conn = if i + 1 == n.children.len() { "└─ " } else { "├─ " };
            walk(c, format!("{id}/{}/{}/{}", c.kind, c.ns, c.name), &next, conn, folded, out);
        }
    }
    let mut out = Vec::new();
    walk(&tree.root, "root".into(), "", "", folded, &mut out);
    out
}

impl TreeView {
    pub fn open(rt: &tokio::runtime::Handle, client: &Client, src: Source) -> Self {
        let title = match &src {
            Source::Argo { name, .. } | Source::Helm { name, .. } => name.clone(),
        };
        let shared = Arc::new(Mutex::new(None));
        let (out, client) = (shared.clone(), client.clone());
        let task = rt.spawn(async move {
            loop {
                *out.lock().unwrap() = Some(build(&client, &src).await);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
        Self { shared, task, title, cursor: 0, folded: HashSet::new(), state: ListState::default() }
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    fn lines(&self) -> Vec<Line_> {
        match &*self.shared.lock().unwrap() {
            Some(Ok(t)) => flatten(t, &self.folded),
            _ => vec![],
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Option<Act> {
        let lines = self.lines();
        match key.code {
            KeyCode::Esc => return Some(Act::Close),
            KeyCode::Char('q') => return Some(Act::Quit),
            KeyCode::Char(':') => return Some(Act::Command),
            KeyCode::Char('j') | KeyCode::Down => self.cursor = (self.cursor + 1).min(lines.len().saturating_sub(1)),
            KeyCode::Char('k') | KeyCode::Up => self.cursor = self.cursor.saturating_sub(1),
            KeyCode::Char('g') => self.cursor = 0,
            KeyCode::Char('G') => self.cursor = lines.len().saturating_sub(1),
            KeyCode::Char(' ') | KeyCode::Enter => {
                if let Some(l) = lines.get(self.cursor).filter(|l| l.has_kids) {
                    if !self.folded.remove(&l.id) {
                        self.folded.insert(l.id.clone());
                    }
                }
            }
            _ => {}
        }
        None
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, status: &str) {
        let guard = self.shared.lock().unwrap().clone();
        match guard {
            None => crate::spinner::render(frame, area, "loading resource tree…"),
            Some(Err(e)) => frame.render_widget(Paragraph::new(format!("cannot build the tree: {e}")).style(Style::new().fg(Color::Red)), area),
            Some(Ok(t)) => draw(frame, area, &t, &self.folded, &mut self.cursor, &mut self.state, status),
        }
    }
}

fn draw(frame: &mut Frame, area: Rect, tree: &Tree, folded: &HashSet<String>, cursor: &mut usize, state: &mut ListState, status: &str) {
    let dim = Style::new().fg(Color::DarkGray);
    let lines = flatten(tree, folded);
    *cursor = (*cursor).min(lines.len().saturating_sub(1));
    let [head, body] = Layout::vertical([Constraint::Length(tree.header.len() as u16 + 2), Constraint::Fill(1)]).areas(area);

    let facts: Vec<Line> = tree.header.iter().map(|(k, v)| Line::from(vec![Span::styled(format!("{k:<12}"), dim), Span::raw(v.clone())])).collect();
    let title = format!(" {} ", tree.root.name);
    frame.render_widget(
        Paragraph::new(facts).block(Block::bordered().border_style(dim).padding(Padding::horizontal(1)).title(Span::styled(title, Style::new().add_modifier(Modifier::BOLD)))),
        head,
    );

    // names first, then one aligned column of health and notes
    let label = |l: &Line_| format!("{}{} {} {}", l.prefix, if l.has_kids { if l.folded { "▸" } else { "▾" } } else { " " }, l.node.kind, l.node.name);
    let width = lines.iter().map(|l| label(l).chars().count()).max().unwrap_or(0).min(body.width as usize * 2 / 3);
    let items: Vec<ListItem> = lines
        .iter()
        .map(|l| {
            let text = label(l);
            let pad = width.saturating_sub(text.chars().count()) + 2;
            let split = l.prefix.chars().count() + 2; // prefix and fold marker stay dim
            let (head_part, rest) = text.split_at(text.char_indices().nth(split).map_or(text.len(), |(i, _)| i));
            let kind_len = l.node.kind.chars().count() + 1;
            let (kind_part, name_part) = rest.split_at(rest.char_indices().nth(kind_len).map_or(rest.len(), |(i, _)| i));
            let mut spans = vec![Span::styled(head_part.to_string(), dim), Span::styled(kind_part.to_string(), dim), Span::raw(name_part.to_string()), Span::raw(" ".repeat(pad))];
            if !l.node.status.is_empty() {
                spans.push(Span::styled(format!("● {}", l.node.status), Style::new().fg(status_color(&l.node.status))));
            }
            if !l.node.note.is_empty() {
                spans.push(Span::styled(format!("  {}", l.node.note), Style::new().fg(l.node.note_color)));
            }
            ListItem::new(Line::from(spans))
        })
        .collect();
    let mut block = Block::bordered().border_style(dim).title(format!(" Resource tree ({}) ", lines.len()));
    if !status.is_empty() {
        block = block.title_bottom(Line::from(format!(" {status} ")).right_aligned());
    }
    state.select((!lines.is_empty()).then_some(*cursor));
    frame.render_stateful_widget(List::new(items).block(block).highlight_style(Style::new().bg(Color::Rgb(60, 50, 120))), body, state);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj<T: k8s_openapi::serde::de::DeserializeOwned>(json: &str) -> T {
        serde_yaml_ng::from_str(json).unwrap()
    }

    fn owned_by(kind: &str, owner: &str) -> String {
        format!(r#""ownerReferences": [{{"apiVersion": "apps/v1", "kind": "{kind}", "name": "{owner}", "uid": "u"}}]"#)
    }

    fn world() -> World {
        World {
            replica_sets: vec![
                obj(&format!(r#"{{"metadata": {{"name": "web-new", "namespace": "web", {}}}, "spec": {{"replicas": 2, "selector": {{}}}}, "status": {{"replicas": 2, "readyReplicas": 1}}}}"#, owned_by("Deployment", "web"))),
                obj(&format!(r#"{{"metadata": {{"name": "web-old", "namespace": "web", {}}}, "spec": {{"replicas": 0, "selector": {{}}}}}}"#, owned_by("Deployment", "web"))),
            ],
            pods: vec![
                obj(&format!(r#"{{"metadata": {{"name": "web-new-a", "namespace": "web", {}}}, "spec": {{"containers": [{{"name": "c"}}]}}, "status": {{"phase": "Running", "containerStatuses": [{{"name": "c", "ready": true, "restartCount": 0, "image": "i", "imageID": "x"}}]}}}}"#, owned_by("ReplicaSet", "web-new"))),
                obj(&format!(r#"{{"metadata": {{"name": "web-new-b", "namespace": "web", {}}}, "spec": {{"containers": [{{"name": "c"}}]}}, "status": {{"phase": "Pending"}}}}"#, owned_by("ReplicaSet", "web-new"))),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn argo_tree_nests_replicasets_and_pods_under_the_deployment() {
        let app: DynamicObject = obj(r#"{"apiVersion": "argoproj.io/v1alpha1", "kind": "Application", "metadata": {"name": "web", "namespace": "argocd"},
            "spec": {"project": "p", "destination": {"server": "https://kubernetes.default.svc", "namespace": "web"}},
            "status": {"health": {"status": "Progressing"}, "sync": {"status": "OutOfSync"},
                       "resources": [{"kind": "Service", "name": "web", "namespace": "web", "status": "Synced", "health": {"status": "Healthy"}},
                                     {"kind": "Deployment", "name": "web", "namespace": "web", "status": "OutOfSync", "health": {"status": "Progressing"}}]}}"#);
        assert_eq!(argo_namespaces(&app).into_iter().collect::<Vec<_>>(), ["web"]);
        let tree = argo_tree(&app, &world());
        let rows: Vec<String> = flatten(&tree, &HashSet::new()).iter().map(|l| format!("{}{} {}", l.prefix, l.node.kind, l.node.name)).collect();
        assert_eq!(
            rows,
            ["Application web", "├─ Deployment web", "│  └─ ReplicaSet web-new", "│     ├─ Pod web-new-a", "│     └─ Pod web-new-b", "└─ Service web"] // web-old (0 replicas) is hidden
        );
        let lines = flatten(&tree, &HashSet::new());
        let pod = lines.iter().find(|l| l.node.name == "web-new-b").unwrap();
        assert_eq!((pod.node.status.as_str(), pod.node.note.as_str()), ("Pending", "0/1"));
        // folding the Deployment hides what is below it
        let folded: HashSet<String> = [lines[1].id.clone()].into();
        assert_eq!(flatten(&tree, &folded).len(), 3);
    }

    #[test]
    fn helm_tree_reads_the_manifest_and_adds_live_workload_state() {
        let manifest = "---\n# Source: c/templates/svc.yaml\napiVersion: v1\nkind: Service\nmetadata:\n  name: web\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: web\n  namespace: web\nspec: {}\n---\napiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: db\n";
        assert_eq!(manifest_refs(manifest, "web").len(), 3);
        let src = Source::Helm { ns: "web".into(), name: "shop".into(), chart: "shop-1.0.0".into(), status: "deployed".into(), revision: 3, manifest: manifest.into() };
        let tree = helm_tree(&src, &world());
        assert_eq!(tree.root.status, "Healthy");
        let kinds: Vec<(&str, &str)> = tree.root.children.iter().map(|c| (c.kind.as_str(), c.status.as_str())).collect();
        assert_eq!(kinds, [("Deployment", "Missing"), ("Service", ""), ("StatefulSet", "Missing")]); // the test world holds no Deployment object
    }

    #[test]
    fn tree_renders_aligned_with_health_and_notes() {
        let app: DynamicObject = obj(r#"{"apiVersion": "argoproj.io/v1alpha1", "kind": "Application", "metadata": {"name": "web", "namespace": "argocd"},
            "spec": {"project": "p", "destination": {"server": "https://kubernetes.default.svc", "namespace": "web"}},
            "status": {"health": {"status": "Progressing"}, "sync": {"status": "OutOfSync"},
                       "resources": [{"kind": "Deployment", "name": "web", "namespace": "web", "status": "OutOfSync", "health": {"status": "Progressing"}}]}}"#);
        let tree = argo_tree(&app, &world());
        let mut term = ratatui::Terminal::new(ratatui::backend::TestBackend::new(90, 14)).unwrap();
        let (mut cursor, mut state) = (0, ListState::default());
        term.draw(|f| draw(f, f.area(), &tree, &HashSet::new(), &mut cursor, &mut state, "")).unwrap();
        let text: String = (0..14).map(|y| (0..90).map(|x| term.backend().buffer()[(x, y)].symbol().to_string()).collect::<String>() + "\n").collect();
        println!("{text}");
        for want in ["Project", "Application web", "▾ Deployment web", "● Progressing  OutOfSync", "ReplicaSet web-new", "● Pending  0/1"] {
            assert!(text.contains(want), "missing {want}");
        }
    }
}
