//! A list for any resource type, CRDs included. Objects are read as untyped `DynamicObject`s, so
//! the columns are the ones every object has: name, a guessed status, age and labels.

use crate::{
    Details, ListKind, MetricsState, GENERIC_DETAILS_HINTS, GENERIC_DETAILS_HINTS_NS, GENERIC_HINTS, GENERIC_HINTS_NS, Hints, NODE_DETAILS_HINTS, NODE_HINTS, PodFilter, WORKLOAD_HINTS, WORKLOAD_HINTS_NS, HOP_HINTS, HOP_HINTS_NS, ROUTES_HINTS, ROUTES_HINTS_NS, Item, ListView, YAML_HINTS, created_secs, created_text,
    field, labels_of, render_details, render_list, status_span, visible_items, yaml_view::{Outcome, YamlView},
};
use crate::resources::ResourceInfo;
use fuzzy_matcher::skim::SkimMatcherV2;
use futures::StreamExt;
use kube::{
    Api, Client, ResourceExt,
    core::{ApiResource, DynamicObject, GroupVersionKind},
    runtime::{WatchStreamExt, reflector, reflector::Store, watcher},
};
use ratatui::{
    Frame,
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    layout::{Constraint, Rect},
    text::Span,
    widgets::{Block, Clear, Paragraph, TableState},
};
use k8s_openapi::api::core::v1::Pod;
use std::{collections::HashMap, sync::{Arc, Mutex}, time::Duration};
use crate::{net_panes, nodes, panes};
use tokio::task::JoinHandle;

/// What the main loop must do after a key.
pub enum Action {
    Close,
    Quit,
    Pods { scope: Option<String>, filter: PodFilter }, // Enter on a node or workload: its pods
    Open { targets: Vec<net_panes::Target>, path: Vec<String> }, // Enter on a Gateway, route...: the next list down
    Cordon(String),
    Uncordon(String),
    Drain(String), // opens the drain options popup
    Command, // open the `:` picker
    PickNamespace, // open the namespace picker
    Copy(String),
    Delete(String), // list key
    Edit(String),
}

/// Hides every object that is not a child of the one we drilled down from. Done on our side because
/// the relations (parentRefs, backendRefs...) live in `spec`, which field selectors cannot reach.
#[derive(Clone)]
pub struct Filter {
    pub crumbs: Vec<String>, // the path that led here
    pub keep: net_panes::Keep,
}

pub struct GenericView {
    pub info: ResourceInfo,
    pub scope: Option<String>, // namespace; None = all namespaces (or a cluster-scoped type)
    client: Client,
    resource: ApiResource,
    store: Store<DynamicObject>,
    task: JoinHandle<()>,
    usage: Option<Arc<Mutex<MetricsState>>>, // live node usage, only for the node list
    usage_task: Option<JoinHandle<()>>,
    pods: Option<Store<Pod>>, // every pod of the cluster, only to count them per node
    pods_task: Option<JoinHandle<()>>,
    pub loading: u64, // Activity id of the "loading" spinner
    error: Arc<Mutex<String>>, // last watch error, shown in the status line
    pub list: ListView,
    viewing: Option<String>,
    yaml: Option<YamlView>,
    typing: bool,
    pub filter: Option<Filter>, // set when this list is a drill-down
}

impl Drop for GenericView {
    fn drop(&mut self) {
        self.task.abort();
        for t in self.usage_task.iter().chain(&self.pods_task) {
            t.abort();
        }
    }
}

/// Best-effort status for an object we know nothing about: `status.phase`, else the Ready-like
/// condition, else `status.state`. Empty when the type has no status at all.
pub fn dyn_status(o: &DynamicObject) -> String {
    let status = &o.data["status"];
    if let Some(p) = status["phase"].as_str() {
        return p.to_string();
    }
    let conditions = status["conditions"].as_array();
    let pick = conditions.and_then(|cs| {
        ["Ready", "Available", "Healthy", "Synced", "Established", "Succeeded"]
            .iter()
            .find_map(|want| cs.iter().find(|c| c["type"].as_str() == Some(want)))
    });
    if let Some(c) = pick {
        let kind = c["type"].as_str().unwrap_or_default();
        return match c["status"].as_str() {
            Some("True") => kind.to_string(),
            Some("False") => format!("Not{kind}"),
            _ => "Unknown".to_string(),
        };
    }
    status["state"].as_str().map(String::from).unwrap_or_default()
}

/// `nodes` = pod counts per node name when this is the node list.
fn dyn_item(o: &DynamicObject, all: bool, nodes: Option<&HashMap<String, usize>>, workload: Option<panes::Pane>) -> Item {
    Item {
        name: if all { format!("{}/{}", o.namespace().unwrap_or_default(), o.name_any()) } else { o.name_any() },
        status: match (nodes, workload) {
            (Some(_), _) => nodes::status(o),
            (_, Some(w)) => panes::status(w, o),
            _ => dyn_status(o),
        },
        created: created_secs(&o.metadata),
        labels: labels_of(&o.metadata, false),
        pod: None,
        extra: workload.map(|w| panes::cells(w, o)),
        node: nodes.map(|counts| nodes::cols(o, counts.get(&o.name_any()).copied().unwrap_or(0))),
    }
}

fn details_of(info: &ResourceInfo, o: &DynamicObject, usage: Option<&crate::Usage>, pods: usize) -> Details {
    if nodes::is_nodes(info) {
        return Details { meta: o.metadata.clone(), info: nodes::info_rows(o, usage, pods), pod: None };
    }
    if let Some(w) = panes::kind_of(info) {
        let mut rows = panes::info_rows(w, o);
        if let Some(ns) = o.namespace() {
            rows.insert(0, field("Namespace", Span::raw(ns)));
        }
        return Details { meta: o.metadata.clone(), info: rows, pod: None };
    }
    let mut rows = vec![field("Kind", Span::raw(format!("{} ({})", info.kind, info.api_version())))];
    if let Some(ns) = o.namespace() {
        rows.push(field("Namespace", Span::raw(ns)));
    }
    let status = dyn_status(o);
    if !status.is_empty() {
        rows.push(field("Status", status_span(&status)));
    }
    rows.push(field("Created", Span::raw(created_text(&o.metadata))));
    rows.push(field("UID", Span::raw(o.metadata.uid.clone().unwrap_or_default())));
    Details { meta: o.metadata.clone(), info: rows, pod: None }
}

impl GenericView {
    pub fn open(rt: &tokio::runtime::Handle, client: &Client, info: ResourceInfo, scope: Option<String>, sort: crate::state::Sort, loading: u64) -> Self {
        let gvk = GroupVersionKind::gvk(&info.group, &info.version, &info.kind);
        let resource = ApiResource::from_gvk_with_plural(&gvk, &info.name);
        let scope = if info.namespaced { scope } else { None };
        let api: Api<DynamicObject> = match &scope {
            Some(ns) => Api::namespaced_with(client.clone(), ns, &resource),
            None => Api::all_with(client.clone(), &resource),
        };
        let writer = reflector::store::Writer::<DynamicObject>::new(resource.clone());
        let store = writer.as_reader();
        let error: Arc<Mutex<String>> = Arc::default();
        let stream = reflector(writer, watcher(api, watcher::Config::default())).default_backoff().applied_objects();
        let err = error.clone();
        let task = rt.spawn(stream.for_each(move |item| {
            // Keep the newest watch error (RBAC, type without watch support) for the status line.
            *err.lock().unwrap() = item.err().map(|e| e.to_string()).unwrap_or_default();
            async {}
        }));
        let usage = nodes::is_nodes(&info).then(Arc::<Mutex<MetricsState>>::default);
        let usage_task = usage.clone().map(|shared| {
            let client = client.clone();
            rt.spawn(async move {
                loop {
                    let result = nodes::fetch_usage(&client).await;
                    {
                        let mut m = shared.lock().unwrap();
                        m.available = Some(result.is_ok());
                        m.usage = result.unwrap_or_default();
                    }
                    tokio::time::sleep(Duration::from_secs(crate::POLL_SECS)).await;
                }
            })
        });
        let (pods, pods_task) = if nodes::is_nodes(&info) {
            let (store, writer) = reflector::store::<Pod>();
            let stream = reflector(writer, watcher(Api::<Pod>::all(client.clone()), watcher::Config::default())).default_backoff().applied_objects();
            (Some(store), Some(rt.spawn(stream.for_each(|_| async {}))))
        } else {
            (None, None)
        };
        Self {
            info,
            scope,
            client: client.clone(),
            resource,
            store: store.clone(),
            task,
            usage,
            usage_task,
            pods,
            pods_task,
            loading,
            error,
            list: ListView::sorted(sort),
            viewing: None,
            yaml: None,
            typing: false,
            filter: None,
        }
    }

    /// The Api for one list key: in an all-namespaces list the object's own namespace.
    pub fn api_for(&self, key: &str) -> Api<DynamicObject> {
        match key.split_once('/') {
            Some((ns, _)) => Api::namespaced_with(self.client.clone(), ns, &self.resource),
            None => match &self.scope {
                Some(ns) => Api::namespaced_with(self.client.clone(), ns, &self.resource),
                None => Api::all_with(self.client.clone(), &self.resource),
            },
        }
    }

    pub fn store(&self) -> &Store<DynamicObject> {
        &self.store
    }

    /// One row per object; node rows carry their pod count.
    fn items(&self) -> Vec<Item> {
        let all = self.all();
        let counts = self.pods.as_ref().map(nodes::pod_counts).or_else(|| nodes::is_nodes(&self.info).then(HashMap::new));
        let keep = |o: &&Arc<DynamicObject>| self.filter.as_ref().is_none_or(|f| (f.keep)(o));
        self.store.state().iter().filter(keep).map(|o| dyn_item(o, all, counts.as_ref(), panes::kind_of(&self.info))).collect()
    }

    fn all(&self) -> bool {
        self.info.namespaced && self.scope.is_none()
    }

    /// Where this list sits: the path that led here, or its namespace, then the type.
    fn base_crumbs(&self) -> Vec<String> {
        let mut c = match &self.filter {
            Some(f) => f.crumbs.clone(),
            None => self.scope.iter().cloned().collect(),
        };
        c.push(self.info.name.clone());
        c
    }

    pub fn crumbs(&self) -> Vec<String> {
        let mut c = self.base_crumbs();
        if let Some(n) = &self.viewing {
            c.push(n.clone());
            c.push("Details".into());
        }
        if let Some(y) = &self.yaml {
            c.push(y.name());
            c.push("YAML".into());
        }
        c
    }

    /// Namespace picker only on a plain list: a drill-down is pinned to what it came from.
    fn can_pick_namespace(&self) -> bool {
        self.info.namespaced && self.filter.is_none()
    }

    pub fn hints(&self) -> Hints {
        let ns = self.can_pick_namespace();
        let pane = panes::kind_of(&self.info);
        if self.yaml.is_some() {
            YAML_HINTS
        } else if self.viewing.is_some() {
            if nodes::is_nodes(&self.info) {
                NODE_DETAILS_HINTS
            } else if ns {
                GENERIC_DETAILS_HINTS_NS
            } else {
                GENERIC_DETAILS_HINTS
            }
        } else if nodes::is_nodes(&self.info) {
            NODE_HINTS
        } else if pane.is_some_and(panes::has_pods) {
            if ns { WORKLOAD_HINTS_NS } else { WORKLOAD_HINTS }
        } else if pane.is_some_and(net_panes::has_routes) {
            if ns { ROUTES_HINTS_NS } else { ROUTES_HINTS }
        } else if pane.is_some_and(net_panes::has_hop) {
            if ns { HOP_HINTS_NS } else { HOP_HINTS }
        } else if ns {
            GENERIC_HINTS_NS
        } else {
            GENERIC_HINTS
        }
    }

    pub fn render(&mut self, frame: &mut Frame, area: Rect, status: &str) {
        let items = self.items();
        let error = self.error.lock().unwrap().clone();
        let status = if error.is_empty() { status.to_string() } else { format!("watch error: {error}") };
        if let Some(y) = &mut self.yaml {
            y.render(frame, area, &status);
            return;
        }
        if let Some(key) = &self.viewing {
            match crate::find_in(&self.store, key) {
                Some(o) => {
                    let usage = self.usage.as_ref().and_then(|u| u.lock().unwrap().usage.get(key).copied());
                    let pods = self.pods.as_ref().map_or(0, |p| nodes::pod_counts(p).get(key).copied().unwrap_or(0));
                    return render_details(frame, area, &details_of(&self.info, &o, usage.as_ref(), pods), &mut TableState::default(), &status);
                }
                None => self.viewing = None, // deleted while open
            }
        }
        let matcher = SkimMatcherV2::default();
        let visible = visible_items(&items, &self.list, &matcher);
        let snapshot = self.usage.as_ref().map(|u| u.lock().unwrap().clone());
        let kind = match (snapshot.as_ref(), panes::kind_of(&self.info)) {
            (Some(m), _) => ListKind::Nodes(m),
            (None, Some(w)) => ListKind::Pane(w),
            _ => ListKind::Namespaces,
        };
        render_list(frame, area, kind, &visible, items.len(), &mut self.list, &status);
        if self.typing {
            let popup = frame.area().centered(Constraint::Percentage(50), Constraint::Length(3));
            frame.render_widget(Clear, popup);
            frame.render_widget(Paragraph::new(self.list.query.as_str()).block(Block::bordered().title(" Search ")), popup);
            frame.set_cursor_position((popup.x + 1 + self.list.query.chars().count() as u16, popup.y + 1));
        }
    }

    /// The key of the highlighted row, in the order the list shows.
    fn selected(&self) -> Option<String> {
        let items = self.items();
        let matcher = SkimMatcherV2::default();
        let visible = visible_items(&items, &self.list, &matcher);
        self.list.table.selected().and_then(|i| visible.get(i)).map(|n| n.name.clone())
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Option<Action> {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if let Some(y) = &mut self.yaml {
            match y.handle_key(key) {
                Some(Outcome::Close) => self.yaml = None,
                Some(Outcome::Copy(t)) => return Some(Action::Copy(t)),
                None => {}
            }
            return None;
        }
        let node = nodes::is_nodes(&self.info);
        if let Some(k) = self.viewing.clone() {
            match key.code {
                KeyCode::Char('d') if ctrl => return Some(Action::Delete(k)),
                KeyCode::Char('e') => return Some(Action::Edit(k)),
                KeyCode::Char('y') => self.open_yaml(&k),
                KeyCode::Char('n') if ctrl && self.can_pick_namespace() => return Some(Action::PickNamespace),
                KeyCode::Char('c') if node => return Some(Action::Cordon(k)),
                KeyCode::Char('u') if node => return Some(Action::Uncordon(k)),
                KeyCode::Char('C') if node => return Some(Action::Drain(k)),
                KeyCode::Char(':') => return Some(Action::Command),
                KeyCode::Esc | KeyCode::Char('q' | 'd') => self.viewing = None,
                _ => {}
            }
            return None;
        }
        if self.typing {
            match key.code {
                KeyCode::Char(c) if !ctrl => {
                    self.list.query.push(c);
                    self.list.table.select_first();
                }
                KeyCode::Backspace => {
                    self.list.query.pop();
                    self.list.table.select_first();
                }
                KeyCode::Enter => self.typing = false,
                KeyCode::Esc => {
                    self.list.query.clear();
                    self.typing = false;
                }
                _ => {}
            }
            return None;
        }
        let selected = self.selected();
        match key.code {
            KeyCode::Char('d') if ctrl => return selected.map(Action::Delete),
            KeyCode::Char('n') if ctrl && self.can_pick_namespace() => return Some(Action::PickNamespace),
            KeyCode::Char('/') => self.typing = true,
            KeyCode::Char(':') => return Some(Action::Command),
            KeyCode::Char('>') => self.list.sort_col = self.list.sort_col.step(1),
            KeyCode::Char('<') => self.list.sort_col = self.list.sort_col.step(-1),
            KeyCode::Char('A') => self.list.ascending = !self.list.ascending,
            KeyCode::Enter if node => {
                return selected.map(|n| Action::Pods {
                    scope: None,
                    filter: PodFilter { crumbs: self.path_to(&n), fields: Some(format!("spec.nodeName={n}")), labels: None },
                });
            }
            KeyCode::Enter if panes::kind_of(&self.info).is_some_and(panes::has_pods) => {
                if let Some(k) = selected {
                    match self.workload_pods(&k) {
                        Some(action) => return Some(action),
                        None => self.viewing = Some(k), // no usable selector: describe instead
                    }
                }
            }
            KeyCode::Enter if panes::kind_of(&self.info).is_some_and(net_panes::has_hop) => {
                if let Some(k) = selected {
                    match self.hop(&k, false) {
                        Some(action) => return Some(action),
                        None => self.viewing = Some(k),
                    }
                }
            }
            KeyCode::Char('r') if panes::kind_of(&self.info).is_some_and(net_panes::has_routes) => {
                return selected.and_then(|k| self.hop(&k, true));
            }
            KeyCode::Char('c') if node => return selected.map(Action::Cordon),
            KeyCode::Char('u') if node => return selected.map(Action::Uncordon),
            KeyCode::Char('C') if node => return selected.map(Action::Drain),
            KeyCode::Char('d') | KeyCode::Enter => self.viewing = selected,
            KeyCode::Char('y') => {
                if let Some(k) = selected {
                    self.open_yaml(&k);
                }
            }
            KeyCode::Char('e') => return selected.map(Action::Edit),
            KeyCode::Char('q') => return Some(Action::Quit),
            KeyCode::Esc if self.list.query.is_empty() => return Some(Action::Close),
            KeyCode::Esc => self.list.query.clear(),
            KeyCode::Char('j') | KeyCode::Down => self.list.table.select_next(),
            KeyCode::Char('k') | KeyCode::Up => self.list.table.select_previous(),
            _ => {}
        }
        None
    }

    /// Crumbs for a list opened from `key` of this one.
    fn path_to(&self, key: &str) -> Vec<String> {
        let mut path = self.base_crumbs();
        path.push(key.to_string());
        path
    }

    /// The next list down from `key`: Enter, or `r` for the routes of a Gateway or ListenerSet.
    fn hop(&self, key: &str, routes: bool) -> Option<Action> {
        let w = panes::kind_of(&self.info)?;
        let o = crate::find_in(&self.store, key)?;
        let hop = if routes { net_panes::routes(w, &o) } else { net_panes::enter(w, &o) }?;
        let path = self.path_to(key);
        Some(match hop {
            net_panes::Hop::Open(targets) => Action::Open { targets, path },
            net_panes::Hop::Pods(labels) => Action::Pods { scope: o.namespace(), filter: PodFilter { crumbs: path, fields: None, labels } },
        })
    }

    /// The pods a workload selects: its own namespace plus its `spec.selector`.
    fn workload_pods(&self, key: &str) -> Option<Action> {
        let o = crate::find_in(&self.store, key)?;
        let labels = Some(panes::label_selector(&o)).filter(|s| !s.is_empty())?;
        Some(Action::Pods {
            scope: o.namespace(),
            filter: PodFilter { crumbs: self.path_to(key), fields: None, labels: Some(labels) },
        })
    }

    fn open_yaml(&mut self, key: &str) {
        let value = crate::find_in(&self.store, key).and_then(|o| serde_yaml_ng::to_value(&*o).ok());
        self.yaml = value.map(|v| YamlView::new(key.to_string(), v));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(json: &str) -> DynamicObject {
        serde_yaml_ng::from_str(json).unwrap()
    }

    #[test]
    fn status_is_guessed_from_phase_conditions_or_state() {
        let base = r#"{"apiVersion": "x/v1", "kind": "Thing", "metadata": {"name": "a", "namespace": "n"}"#;
        assert_eq!(dyn_status(&object(&format!(r#"{base}, "status": {{"phase": "Bound"}}}}"#))), "Bound");
        let ready = r#", "status": {"conditions": [{"type": "Other", "status": "False"}, {"type": "Ready", "status": "True"}]}}"#;
        assert_eq!(dyn_status(&object(&format!("{base}{ready}"))), "Ready");
        let not = r#", "status": {"conditions": [{"type": "Ready", "status": "False"}]}}"#;
        assert_eq!(dyn_status(&object(&format!("{base}{not}"))), "NotReady");
        assert_eq!(dyn_status(&object(&format!(r#"{base}, "status": {{"state": "Pending"}}}}"#))), "Pending");
        assert_eq!(dyn_status(&object(&format!("{base}}}"))), "");
    }

    #[test]
    fn all_namespaces_keys_carry_the_namespace() {
        let o = object(r#"{"apiVersion": "x/v1", "kind": "Thing", "metadata": {"name": "a", "namespace": "n"}}"#);
        assert_eq!(dyn_item(&o, true, None, None).name, "n/a");
        assert_eq!(dyn_item(&o, false, None, None).name, "a");
    }
}
