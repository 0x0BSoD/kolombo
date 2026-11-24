//! apps/v1 workloads (Deployment, ReplicaSet, StatefulSet, DaemonSet, ControllerRevision) read from
//! untyped objects: the list columns, a status word, and the rows of the describe view.

use crate::{created_text, field, short_image, status_span};
use k8s_openapi::serde_json::Value;
use kube::{ResourceExt, core::DynamicObject};
use ratatui::{
    layout::Constraint,
    style::{Color, Style},
    text::{Line, Span},
};

use crate::resources::ResourceInfo;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Workload {
    Deployment,
    ReplicaSet,
    StatefulSet,
    DaemonSet,
    ControllerRevision,
}

pub fn kind_of(info: &ResourceInfo) -> Option<Workload> {
    if info.group != "apps" {
        return None;
    }
    match info.name.as_str() {
        "deployments" => Some(Workload::Deployment),
        "replicasets" => Some(Workload::ReplicaSet),
        "statefulsets" => Some(Workload::StatefulSet),
        "daemonsets" => Some(Workload::DaemonSet),
        "controllerrevisions" => Some(Workload::ControllerRevision),
        _ => None,
    }
}

/// Columns between STATUS and AGE (NAME, STATUS and AGE are the same everywhere).
pub fn columns(w: Workload) -> Vec<(&'static str, Constraint)> {
    let n = |t, w| (t, Constraint::Length(w));
    let images = ("IMAGES", Constraint::Fill(2));
    match w {
        Workload::Deployment => vec![n("READY", 7), n("UP-TO-DATE", 10), n("AVAILABLE", 9), n("STRATEGY", 14), images],
        Workload::ReplicaSet => vec![n("READY", 7), n("DESIRED", 7), n("OWNER", 28), images],
        Workload::StatefulSet => vec![n("READY", 7), n("UP-TO-DATE", 10), n("STRATEGY", 14), ("SERVICE", Constraint::Fill(1)), images],
        Workload::DaemonSet => vec![
            n("DESIRED", 7),
            n("CURRENT", 7),
            n("READY", 7),
            n("UP-TO-DATE", 10),
            n("AVAILABLE", 9),
            ("NODE SELECTOR", Constraint::Fill(1)),
            images,
        ],
        Workload::ControllerRevision => vec![n("REVISION", 8), ("CONTROLLER", Constraint::Fill(2))],
    }
}

fn num(v: &Value) -> u64 {
    v.as_u64().unwrap_or(0)
}

fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => String::new(),
    }
}

fn desired(o: &DynamicObject) -> u64 {
    // the API server defaults a missing spec.replicas to 1
    o.data["spec"]["replicas"].as_u64().unwrap_or(1)
}

/// Green when everything is up, yellow when partly, grey when nothing is wanted, red when nothing runs.
fn ratio(ready: u64, want: u64) -> Color {
    match (ready, want) {
        (_, 0) => Color::DarkGray,
        (r, w) if r >= w => Color::Green,
        (0, _) => Color::Red,
        _ => Color::Yellow,
    }
}

fn pod_containers(o: &DynamicObject) -> Vec<(String, String)> {
    o.data["spec"]["template"]["spec"]["containers"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| (text(&c["name"]), text(&c["image"])))
        .collect()
}

/// `nginx:1.25` or `nginx:1.25 +2`
fn images(o: &DynamicObject) -> String {
    let all = pod_containers(o);
    match all.split_first() {
        None => String::new(),
        Some(((_, first), rest)) if rest.is_empty() => short_image(first),
        Some(((_, first), rest)) => format!("{} +{}", short_image(first), rest.len()),
    }
}

fn selector(o: &DynamicObject) -> String {
    let labels = &o.data["spec"]["selector"]["matchLabels"];
    labels
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, v)| format!("{k}={}", text(v)))
        .collect::<Vec<_>>()
        .join(",")
}

/// `spec.selector` as a label-selector string for the API (`app=web,tier in (a,b),!canary`).
pub fn label_selector(o: &DynamicObject) -> String {
    let sel = &o.data["spec"]["selector"];
    let mut parts: Vec<String> = sel["matchLabels"].as_object().into_iter().flatten().map(|(k, v)| format!("{k}={}", text(v))).collect();
    for e in sel["matchExpressions"].as_array().into_iter().flatten() {
        let key = text(&e["key"]);
        let values = e["values"].as_array().into_iter().flatten().map(text).collect::<Vec<_>>().join(",");
        match e["operator"].as_str() {
            Some("In") => parts.push(format!("{key} in ({values})")),
            Some("NotIn") => parts.push(format!("{key} notin ({values})")),
            Some("Exists") => parts.push(key),
            Some("DoesNotExist") => parts.push(format!("!{key}")),
            _ => {}
        }
    }
    parts.join(",")
}

/// Whether Enter on this kind should open its pods.
pub fn has_pods(w: Workload) -> bool {
    w != Workload::ControllerRevision
}

fn strategy(o: &DynamicObject, key: &str) -> String {
    text(&o.data["spec"][key]["type"])
}

fn owner(o: &DynamicObject) -> String {
    o.owner_references().first().map(|r| format!("{}/{}", r.kind.to_lowercase(), r.name)).unwrap_or_default()
}

fn condition<'a>(o: &'a DynamicObject, kind: &str) -> Option<&'a Value> {
    o.data["status"]["conditions"].as_array()?.iter().find(|c| c["type"].as_str() == Some(kind))
}

/// One word for the STATUS column.
pub fn status(w: Workload, o: &DynamicObject) -> String {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let want = desired(o);
    match w {
        Workload::Deployment => {
            if spec["paused"].as_bool() == Some(true) {
                "Paused".into()
            } else if condition(o, "ReplicaFailure").is_some_and(|c| c["status"] == "True") {
                "ReplicaFailure".into()
            } else if condition(o, "Progressing").is_some_and(|c| c["reason"] == "ProgressDeadlineExceeded") {
                "ProgressDeadlineExceeded".into()
            } else if want == 0 {
                "ScaledToZero".into()
            } else if num(&st["updatedReplicas"]) < want || num(&st["replicas"]) > want {
                "Updating".into() // a rollout is moving pods
            } else if num(&st["availableReplicas"]) >= want {
                "Available".into()
            } else {
                "Unavailable".into()
            }
        }
        Workload::ReplicaSet | Workload::StatefulSet => {
            let ready = num(&st["readyReplicas"]);
            if want == 0 {
                "ScaledToZero".into()
            } else if w == Workload::StatefulSet && num(&st["updatedReplicas"]) < want && ready >= want {
                "Updating".into()
            } else if ready >= want {
                "Ready".into()
            } else {
                "NotReady".into()
            }
        }
        Workload::DaemonSet => {
            let want = num(&st["desiredNumberScheduled"]);
            if want == 0 {
                "NoNodes".into()
            } else if num(&st["numberReady"]) >= want {
                "Ready".into()
            } else {
                "NotReady".into()
            }
        }
        Workload::ControllerRevision => String::new(),
    }
}

/// Text and color for each of `columns(w)`.
pub fn cells(w: Workload, o: &DynamicObject) -> Vec<(String, Color)> {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let plain = |s: String| (s, Color::Reset);
    let count = |v: &Value| (num(v).to_string(), Color::Reset);
    match w {
        Workload::Deployment | Workload::StatefulSet => {
            let (want, ready) = (desired(o), num(&st["readyReplicas"]));
            let mut v = vec![(format!("{ready}/{want}"), ratio(ready, want)), count(&st["updatedReplicas"])];
            if w == Workload::Deployment {
                let available = num(&st["availableReplicas"]);
                v.push((available.to_string(), ratio(available, want)));
                v.push(plain(strategy(o, "strategy")));
            } else {
                v.push(plain(strategy(o, "updateStrategy")));
                v.push(plain(text(&spec["serviceName"])));
            }
            v.push(plain(images(o)));
            v
        }
        Workload::ReplicaSet => {
            let (want, ready) = (desired(o), num(&st["readyReplicas"]));
            vec![(format!("{ready}/{want}"), ratio(ready, want)), (want.to_string(), Color::Reset), plain(owner(o)), plain(images(o))]
        }
        Workload::DaemonSet => {
            let want = num(&st["desiredNumberScheduled"]);
            let (ready, available) = (num(&st["numberReady"]), num(&st["numberAvailable"]));
            let selector = spec["template"]["spec"]["nodeSelector"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(k, v)| format!("{k}={}", text(v)))
                .collect::<Vec<_>>()
                .join(",");
            vec![
                (want.to_string(), Color::Reset),
                count(&st["currentNumberScheduled"]),
                (ready.to_string(), ratio(ready, want)),
                count(&st["updatedNumberScheduled"]),
                (available.to_string(), ratio(available, want)),
                plain(if selector.is_empty() { "<none>".into() } else { selector }),
                plain(images(o)),
            ]
        }
        Workload::ControllerRevision => vec![plain(text(&o.data["revision"])), plain(owner(o))],
    }
}

fn mark(ok: bool) -> Span<'static> {
    if ok { Span::styled("✓", Style::new().fg(Color::Green)) } else { Span::styled("✗", Style::new().fg(Color::Red)) }
}

/// A label on the first line only, so multi-line values stay aligned.
fn lines(label: &str, values: Vec<Vec<Span<'static>>>) -> Vec<Line<'static>> {
    values
        .into_iter()
        .enumerate()
        .map(|(i, spans)| {
            let mut line = field(if i == 0 { label } else { "" }, Span::raw(""));
            line.spans.pop(); // drop the empty value `field` made
            line.spans.extend(spans);
            line
        })
        .collect()
}

/// Rows of the describe view: what the controller wants, what it has, how it rolls, what it runs.
pub fn info_rows(w: Workload, o: &DynamicObject) -> Vec<Line<'static>> {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let dim = Style::new().fg(Color::DarkGray);
    let mut rows = Vec::new();
    let status = status(w, o);
    if !status.is_empty() {
        rows.push(field("Status", status_span(&status)));
    }
    let counts = |pairs: &[(&str, u64)]| pairs.iter().map(|(k, v)| format!("{v} {k}")).collect::<Vec<_>>().join(" · ");
    match w {
        Workload::Deployment => {
            rows.push(field(
                "Replicas",
                Span::raw(counts(&[
                    ("desired", desired(o)),
                    ("updated", num(&st["updatedReplicas"])),
                    ("ready", num(&st["readyReplicas"])),
                    ("available", num(&st["availableReplicas"])),
                    ("unavailable", num(&st["unavailableReplicas"])),
                ])),
            ));
            let rolling = &spec["strategy"]["rollingUpdate"];
            let detail = if rolling.is_object() {
                format!(" (maxSurge {}, maxUnavailable {})", text(&rolling["maxSurge"]), text(&rolling["maxUnavailable"]))
            } else {
                String::new()
            };
            rows.push(field("Strategy", Span::raw(format!("{}{detail}", strategy(o, "strategy")))));
            rows.push(field(
                "Rollout",
                Span::styled(
                    format!(
                        "keeps {} old ReplicaSets · progress deadline {}s · min ready {}s{}",
                        text(&spec["revisionHistoryLimit"]),
                        text(&spec["progressDeadlineSeconds"]),
                        num(&spec["minReadySeconds"]),
                        if spec["paused"] == true { " · PAUSED" } else { "" }
                    ),
                    dim,
                ),
            ));
        }
        Workload::ReplicaSet => {
            rows.push(field("Replicas", Span::raw(counts(&[("desired", desired(o)), ("current", num(&st["replicas"])), ("ready", num(&st["readyReplicas"])), ("available", num(&st["availableReplicas"]))]))));
            rows.push(field("Owner", Span::raw(owner(o))));
            if let Some(rev) = o.annotations().get("deployment.kubernetes.io/revision") {
                rows.push(field("Revision", Span::raw(rev.clone())));
            }
        }
        Workload::StatefulSet => {
            rows.push(field("Replicas", Span::raw(counts(&[("desired", desired(o)), ("current", num(&st["currentReplicas"])), ("updated", num(&st["updatedReplicas"])), ("ready", num(&st["readyReplicas"]))]))));
            rows.push(field("Strategy", Span::raw(strategy(o, "updateStrategy"))));
            rows.push(field("Pods", Span::raw(format!("{} start/stop order", text(&spec["podManagementPolicy"])))));
            rows.push(field("Service", Span::raw(text(&spec["serviceName"]))));
            let claims: Vec<String> = spec["volumeClaimTemplates"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|c| format!("{} ({})", text(&c["metadata"]["name"]), text(&c["spec"]["resources"]["requests"]["storage"])))
                .collect();
            rows.push(field("Storage", if claims.is_empty() { Span::styled("<none>", dim) } else { Span::raw(claims.join(", ")) }));
        }
        Workload::DaemonSet => {
            rows.push(field(
                "Nodes",
                Span::raw(counts(&[
                    ("desired", num(&st["desiredNumberScheduled"])),
                    ("current", num(&st["currentNumberScheduled"])),
                    ("ready", num(&st["numberReady"])),
                    ("updated", num(&st["updatedNumberScheduled"])),
                    ("available", num(&st["numberAvailable"])),
                    ("misscheduled", num(&st["numberMisscheduled"])),
                ])),
            ));
            rows.push(field("Strategy", Span::raw(strategy(o, "updateStrategy"))));
        }
        Workload::ControllerRevision => {
            rows.push(field("Revision", Span::raw(text(&o.data["revision"]))));
            rows.push(field("Controller", Span::raw(owner(o))));
        }
    }
    if w != Workload::ControllerRevision {
        rows.push(field("Selector", Span::raw(selector(o))));
        let containers = pod_containers(o);
        let imgs = containers.iter().map(|(n, i)| vec![Span::styled(format!("{n}  "), dim), Span::raw(i.clone())]).collect();
        rows.extend(lines("Images", imgs));
        let conditions: Vec<Vec<Span>> = st["conditions"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|c| {
                let ok = c["status"] == "True";
                let reason = text(&c["reason"]);
                vec![mark(ok), Span::raw(format!(" {}  ", text(&c["type"]))), Span::styled(reason, dim)]
            })
            .collect();
        rows.extend(lines("Conditions", conditions));
    }
    rows.push(field("Created", Span::raw(created_text(&o.metadata))));
    rows.push(field("UID", Span::raw(o.metadata.uid.clone().unwrap_or_default())));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(json: &str) -> DynamicObject {
        serde_yaml_ng::from_str(json).unwrap()
    }

    fn cell_text(w: Workload, o: &DynamicObject) -> Vec<String> {
        cells(w, o).into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn deployment_row_shows_rollout_state() {
        let d = obj(r#"{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "web"},
            "spec": {"replicas": 3, "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": "25%", "maxUnavailable": 1}},
                     "selector": {"matchLabels": {"app": "web"}},
                     "template": {"spec": {"containers": [{"name": "a", "image": "reg.io/team/web:1"}, {"name": "b", "image": "envoy:1"}]}}},
            "status": {"replicas": 3, "readyReplicas": 2, "updatedReplicas": 3, "availableReplicas": 2,
                       "conditions": [{"type": "Available", "status": "True", "reason": "MinimumReplicasAvailable"}]}}"#);
        assert_eq!(cell_text(Workload::Deployment, &d), ["2/3", "3", "2", "RollingUpdate", "team/web:1 +1"]);
        assert_eq!(columns(Workload::Deployment).len(), cells(Workload::Deployment, &d).len());
        assert_eq!(status(Workload::Deployment, &d), "Unavailable"); // 2 of 3 available
        let text: String = info_rows(Workload::Deployment, &d).iter().map(|l| l.to_string()).collect::<Vec<_>>().join("\n");
        assert!(text.contains("maxSurge 25%, maxUnavailable 1") && text.contains("app=web") && text.contains("MinimumReplicasAvailable"));

        let scaled = obj(r#"{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "x"}, "spec": {"replicas": 0}}"#);
        assert_eq!(status(Workload::Deployment, &scaled), "ScaledToZero");
        let rolling = obj(r#"{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "x"}, "spec": {"replicas": 3}, "status": {"replicas": 4, "updatedReplicas": 1, "availableReplicas": 3}}"#);
        assert_eq!(status(Workload::Deployment, &rolling), "Updating");
    }

    #[test]
    fn selector_covers_labels_and_expressions() {
        let o = obj(r#"{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "w"}, "spec": {"selector": {
            "matchLabels": {"app": "web"},
            "matchExpressions": [{"key": "tier", "operator": "In", "values": ["a", "b"]}, {"key": "x", "operator": "NotIn", "values": ["y"]},
                                 {"key": "has", "operator": "Exists"}, {"key": "canary", "operator": "DoesNotExist"}]}}}"#);
        assert_eq!(label_selector(&o), "app=web,tier in (a,b),x notin (y),has,!canary");
    }

    #[test]
    fn every_kind_has_one_cell_per_column() {
        let o = obj(r#"{"apiVersion": "apps/v1", "kind": "X", "metadata": {"name": "x"}}"#);
        for w in [Workload::Deployment, Workload::ReplicaSet, Workload::StatefulSet, Workload::DaemonSet, Workload::ControllerRevision] {
            assert_eq!(columns(w).len(), cells(w, &o).len(), "{w:?}");
            assert!(!info_rows(w, &o).is_empty());
        }
    }

    #[test]
    fn daemonset_and_replicaset_status() {
        let ds = obj(r#"{"apiVersion": "apps/v1", "kind": "DaemonSet", "metadata": {"name": "d"},
            "status": {"desiredNumberScheduled": 5, "currentNumberScheduled": 5, "numberReady": 4, "updatedNumberScheduled": 5, "numberAvailable": 4}}"#);
        assert_eq!(status(Workload::DaemonSet, &ds), "NotReady");
        assert_eq!(cell_text(Workload::DaemonSet, &ds)[..5], ["5", "5", "4", "5", "4"]);
        let rs = obj(r#"{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": {"name": "r", "ownerReferences": [{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web", "uid": "u", "controller": true}]},
            "spec": {"replicas": 0}}"#);
        assert_eq!(status(Workload::ReplicaSet, &rs), "ScaledToZero");
        assert_eq!(cell_text(Workload::ReplicaSet, &rs)[2], "deployment/web");
    }
}
