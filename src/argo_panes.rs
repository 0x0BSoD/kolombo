//! Argo CD (argoproj.io): Application, ApplicationSet, AppProject. Enter walks
//! AppProject -> ApplicationSet -> Application -> resource tree.

use crate::net_panes::{Hop, target};
use crate::panes::{Pane, lines, mark, text};
use crate::resources::ResourceInfo;
use crate::tree_view::Source;
use crate::{created_text, field, format_age, status_span};
use k8s_openapi::{jiff, serde_json::Value};
use kube::{ResourceExt, core::DynamicObject};
use ratatui::{
    layout::Constraint,
    style::{Color, Style},
    text::{Line, Span},
};

pub const GROUP: &str = "argoproj.io";

pub fn kind_of(info: &ResourceInfo) -> Option<Pane> {
    match (info.group.as_str(), info.name.as_str()) {
        (GROUP, "applications") => Some(Pane::Application),
        (GROUP, "applicationsets") => Some(Pane::ApplicationSet),
        (GROUP, "appprojects") => Some(Pane::AppProject),
        _ => None,
    }
}

fn s(v: &Value) -> &str {
    v.as_str().unwrap_or_default()
}

fn arr(v: &Value) -> impl Iterator<Item = &Value> {
    v.as_array().into_iter().flatten()
}

fn join<I: IntoIterator<Item = String>>(items: I, sep: &str) -> String {
    items.into_iter().collect::<Vec<_>>().join(sep)
}

fn dim() -> Style {
    Style::new().fg(Color::DarkGray)
}

/// The first source: `spec.source`, or the first of `spec.sources` (multi-source apps).
fn source(spec: &Value) -> &Value {
    if spec["source"].is_object() { &spec["source"] } else { &spec["sources"][0] }
}

/// `org/repo` out of a git URL, so a long URL still fits a column.
fn short_repo(url: &str) -> String {
    let trimmed = url.trim_end_matches('/').trim_end_matches(".git");
    let parts: Vec<&str> = trimmed.rsplit(['/', ':']).take(2).collect();
    match parts.as_slice() {
        [name, org] => format!("{org}/{name}"),
        _ => trimmed.to_string(),
    }
}

/// `chart@1.2.3` for a Helm repo, `org/repo/path@main` for git.
fn source_text(src: &Value) -> String {
    let rev = s(&src["targetRevision"]);
    let rev = if rev.is_empty() { String::new() } else { format!("@{rev}") };
    if let Some(chart) = src["chart"].as_str() {
        return format!("{chart}{rev}");
    }
    let path = src["path"].as_str().filter(|p| *p != ".").map(|p| format!("/{p}")).unwrap_or_default();
    format!("{}{path}{rev}", short_repo(s(&src["repoURL"])))
}

fn destination(d: &Value) -> String {
    let server = s(&d["server"]);
    let target = match d["name"].as_str() {
        Some(n) => n.to_string(),
        None if server == "https://kubernetes.default.svc" => "in-cluster".into(),
        None => server.trim_start_matches("https://").to_string(),
    };
    format!("{target}/{}", s(&d["namespace"]))
}

fn revision(o: &DynamicObject) -> String {
    let sync = &o.data["status"]["sync"];
    let rev = if sync["revision"].is_string() { s(&sync["revision"]) } else { s(&sync["revisions"][0]) };
    if rev.len() >= 40 { rev[..7].to_string() } else { rev.to_string() } // a commit sha: the short form
}

fn sync_color(sync: &str) -> Color {
    match sync {
        "Synced" => Color::Green,
        "OutOfSync" => Color::Yellow,
        _ => Color::Reset,
    }
}

pub fn columns(w: Pane) -> Vec<(&'static str, Constraint)> {
    let n = |t, w| (t, Constraint::Length(w));
    let fill = |t, k| (t, Constraint::Fill(k));
    match w {
        Pane::Application => vec![n("SYNC", 9), n("PROJECT", 14), n("REVISION", 9), fill("DESTINATION", 1), fill("SOURCE", 2)],
        Pane::ApplicationSet => vec![n("PROJECT", 14), fill("GENERATORS", 1), n("APPS", 5), fill("SOURCE", 2)],
        Pane::AppProject => vec![fill("DESCRIPTION", 2), n("SOURCES", 7), n("DESTS", 5), n("ROLES", 5)],
        _ => vec![],
    }
}

/// Health for an Application ("Syncing" while an operation runs), the conditions for an ApplicationSet.
pub fn status(w: Pane, o: &DynamicObject) -> String {
    let st = &o.data["status"];
    match w {
        Pane::Application => {
            if s(&st["operationState"]["phase"]) == "Running" {
                "Syncing".into()
            } else {
                text(&st["health"]["status"])
            }
        }
        Pane::ApplicationSet => {
            let flag = |kind: &str| arr(&st["conditions"]).find(|c| s(&c["type"]) == kind).map(|c| s(&c["status"]) == "True");
            match (flag("ErrorOccurred"), flag("ResourcesUpToDate")) {
                (Some(true), _) => "Error".into(),
                (_, Some(true)) => "Healthy".into(),
                (_, Some(false)) => "Updating".into(),
                _ => String::new(),
            }
        }
        _ => String::new(),
    }
}

fn generators(o: &DynamicObject) -> String {
    join(arr(&o.data["spec"]["generators"]).flat_map(|g| g.as_object().into_iter().flat_map(|m| m.keys().cloned())), ",")
}

pub fn cells(w: Pane, o: &DynamicObject) -> Vec<(String, Color)> {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let plain = |t: String| (t, Color::Reset);
    match w {
        Pane::Application => {
            let sync = text(&st["sync"]["status"]);
            let color = sync_color(&sync);
            vec![(sync, color), plain(text(&spec["project"])), plain(revision(o)), plain(destination(&spec["destination"])), plain(source_text(source(spec)))]
        }
        Pane::ApplicationSet => {
            let template = &spec["template"]["spec"];
            vec![
                plain(text(&template["project"])),
                plain(generators(o)),
                plain(arr(&st["applicationStatus"]).count().to_string()),
                plain(source_text(source(template))),
            ]
        }
        Pane::AppProject => {
            let sources = arr(&spec["sourceRepos"]).map(|r| s(r)).collect::<Vec<_>>();
            vec![
                plain(text(&spec["description"])),
                plain(if sources.contains(&"*") { "*".into() } else { sources.len().to_string() }),
                plain(arr(&spec["destinations"]).count().to_string()),
                plain(arr(&spec["roles"]).count().to_string()),
            ]
        }
        _ => vec![],
    }
}

fn policy_text(policy: &Value) -> String {
    let auto = &policy["automated"];
    if !auto.is_object() {
        return "manual".into();
    }
    let on = |k: &str| if auto[k] == true { "on" } else { "off" };
    format!("automated (prune {}, self-heal {})", on("prune"), on("selfHeal"))
}

fn sources_lines(spec: &Value) -> Vec<Vec<Span<'static>>> {
    let all: Vec<&Value> = if spec["sources"].is_array() { arr(&spec["sources"]).collect() } else { vec![&spec["source"]] };
    all.into_iter()
        .filter(|x| x.is_object())
        .map(|x| {
            let from = if x["chart"].is_string() { format!("chart {} from {}", s(&x["chart"]), s(&x["repoURL"])) } else { format!("{} {}", s(&x["repoURL"]), s(&x["path"])) };
            vec![Span::raw(from), Span::styled(format!("  @ {}", s(&x["targetRevision"])), dim())]
        })
        .collect()
}

fn condition_lines(conds: &Value) -> Vec<Vec<Span<'static>>> {
    arr(conds).map(|c| vec![Span::styled(format!("{}  ", s(&c["type"])), Style::new().fg(Color::Yellow)), Span::raw(s(&c["message"]).replace('\n', " "))]).collect()
}

fn ago(v: &Value) -> String {
    v.as_str().and_then(|t| t.parse::<jiff::Timestamp>().ok()).map_or("-".into(), |t| format_age(jiff::Timestamp::now().as_second() - t.as_second()) + " ago")
}

pub fn info_rows(w: Pane, o: &DynamicObject) -> Vec<Line<'static>> {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let none = || vec![vec![Span::styled("<none>", dim())]];
    let nonempty = |v: Vec<Vec<Span<'static>>>| if v.is_empty() { none() } else { v };
    let mut rows = Vec::new();
    let status = status(w, o);
    if !status.is_empty() {
        rows.push(field("Status", status_span(&status)));
    }
    match w {
        Pane::Application => {
            let sync = text(&st["sync"]["status"]);
            rows.push(field("Sync", Span::styled(format!("{sync}  {}", revision(o)), Style::new().fg(sync_color(&sync)))));
            let msg = s(&st["health"]["message"]);
            if !msg.is_empty() {
                rows.push(field("Health", Span::raw(msg.replace('\n', " "))));
            }
            rows.push(field("Project", Span::raw(text(&spec["project"]))));
            rows.push(field("Destination", Span::raw(destination(&spec["destination"]))));
            rows.extend(lines("Source", nonempty(sources_lines(spec))));
            rows.push(field("Sync policy", Span::raw(policy_text(&spec["syncPolicy"]))));
            let op = &st["operationState"];
            if op.is_object() {
                let ok = s(&op["phase"]) == "Succeeded";
                rows.push(field(
                    "Last sync",
                    Span::styled(format!("{} {} · {}", s(&op["phase"]), ago(&op["finishedAt"]), s(&op["message"]).replace('\n', " ")), Style::new().fg(if ok { Color::Green } else { Color::Yellow })),
                ));
            }
            let all = arr(&st["resources"]).count();
            let off: Vec<Vec<Span>> = arr(&st["resources"])
                .filter(|r| s(&r["status"]) == "OutOfSync")
                .map(|r| vec![mark(false), Span::raw(format!(" {}/{}", s(&r["kind"]), s(&r["name"])))])
                .collect();
            rows.push(field("Resources", Span::raw(format!("{all} managed · {} out of sync", off.len()))));
            rows.extend(lines("OutOfSync", off));
            rows.extend(lines("Conditions", condition_lines(&st["conditions"])));
        }
        Pane::ApplicationSet => {
            let t = &spec["template"]["spec"];
            rows.push(field("Project", Span::raw(text(&t["project"]))));
            rows.push(field("Generators", Span::raw(generators(o))));
            rows.push(field("Destination", Span::raw(destination(&t["destination"]))));
            rows.extend(lines("Source", nonempty(sources_lines(t))));
            rows.push(field("Sync policy", Span::raw(policy_text(&t["syncPolicy"]))));
            let apps: Vec<Vec<Span>> = arr(&st["applicationStatus"])
                .map(|a| vec![Span::raw(format!("{}  ", s(&a["application"]))), Span::styled(text(&a["status"]), dim())])
                .collect();
            rows.extend(lines("Applications", nonempty(apps)));
            rows.extend(lines("Conditions", condition_lines(&st["conditions"])));
        }
        Pane::AppProject => {
            rows.push(field("Description", Span::raw(text(&spec["description"]))));
            rows.extend(lines("Sources", nonempty(arr(&spec["sourceRepos"]).map(|r| vec![Span::raw(s(r).to_string())]).collect())));
            let dests = arr(&spec["destinations"]).map(|d| vec![Span::raw(destination(d))]).collect();
            rows.extend(lines("Destinations", nonempty(dests)));
            let kinds = |v: &Value| join(arr(v).map(|k| format!("{}/{}", s(&k["group"]), s(&k["kind"]))), ", ");
            for (label, key) in [("Cluster allow", "clusterResourceWhitelist"), ("Cluster deny", "clusterResourceBlacklist"), ("Namespaced allow", "namespaceResourceWhitelist"), ("Namespaced deny", "namespaceResourceBlacklist")] {
                let k = kinds(&spec[key]);
                if !k.is_empty() {
                    rows.push(field(label, Span::raw(k)));
                }
            }
            let roles = arr(&spec["roles"]).map(|r| vec![Span::raw(text(&r["name"])), Span::styled(format!("  {} policies", arr(&r["policies"]).count()), dim())]).collect();
            rows.extend(lines("Roles", nonempty(roles)));
        }
        _ => {}
    }
    rows.push(field("Created", Span::raw(created_text(&o.metadata))));
    rows.push(field("UID", Span::raw(o.metadata.uid.clone().unwrap_or_default())));
    rows
}

// ---------- hops ----------

pub fn has_hop(w: Pane) -> bool {
    matches!(w, Pane::AppProject | Pane::ApplicationSet | Pane::Application)
}

pub fn enter(w: Pane, o: &DynamicObject) -> Option<Hop> {
    let (name, ns, uid) = (o.name_any(), o.namespace().unwrap_or_default(), o.metadata.uid.clone().unwrap_or_default());
    Some(match w {
        // an ApplicationSet names its project in the template (not when it is itself templated)
        Pane::AppProject => Hop::Open(vec![target(GROUP, "applicationsets", move |a| s(&a.data["spec"]["template"]["spec"]["project"]) == name)]),
        // the Applications a set generated are owned by it
        Pane::ApplicationSet => Hop::Open(vec![target(GROUP, "applications", move |a| a.owner_references().iter().any(|r| r.uid == uid))]),
        Pane::Application => Hop::Tree(Source::Argo { ns, name }),
        _ => return None,
    })
}

/// `r` on a project: its Applications, with or without an ApplicationSet in between.
pub fn related(w: Pane, o: &DynamicObject) -> Option<Hop> {
    let name = o.name_any();
    (w == Pane::AppProject).then(|| Hop::Open(vec![target(GROUP, "applications", move |a| s(&a.data["spec"]["project"]) == name)]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(json: &str) -> DynamicObject {
        serde_yaml_ng::from_str(json).unwrap()
    }

    fn cell_text(w: Pane, o: &DynamicObject) -> Vec<String> {
        cells(w, o).into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn every_kind_has_one_cell_per_column() {
        let o = obj(r#"{"apiVersion": "argoproj.io/v1alpha1", "kind": "X", "metadata": {"name": "x"}}"#);
        for w in [Pane::Application, Pane::ApplicationSet, Pane::AppProject] {
            assert_eq!(columns(w).len(), cells(w, &o).len(), "{w:?}");
            assert_eq!(crate::panes::columns(w).len(), crate::panes::cells(w, &o).len(), "{w:?}");
            assert!(!info_rows(w, &o).is_empty());
        }
    }

    #[test]
    fn application_row() {
        let app = obj(r#"{"apiVersion": "argoproj.io/v1alpha1", "kind": "Application", "metadata": {"name": "web", "namespace": "argocd"},
            "spec": {"project": "team-a", "destination": {"server": "https://kubernetes.default.svc", "namespace": "web"},
                     "source": {"repoURL": "git@github.com:acme/deploy.git", "path": "apps/web", "targetRevision": "main"},
                     "syncPolicy": {"automated": {"prune": true}}},
            "status": {"health": {"status": "Degraded"}, "sync": {"status": "OutOfSync", "revision": "0123456789abcdef0123456789abcdef01234567"},
                       "resources": [{"kind": "Deployment", "name": "web", "status": "OutOfSync"}]}}"#);
        assert_eq!(cell_text(Pane::Application, &app), ["OutOfSync", "team-a", "0123456", "in-cluster/web", "acme/deploy/apps/web@main"]);
        assert_eq!(status(Pane::Application, &app), "Degraded");
        let text: String = info_rows(Pane::Application, &app).iter().map(|l| l.to_string()).collect::<Vec<_>>().join("\n");
        assert!(text.contains("prune on, self-heal off") && text.contains("1 managed · 1 out of sync"), "{text}");
        let chart = obj(r#"{"apiVersion": "argoproj.io/v1alpha1", "kind": "Application", "metadata": {"name": "c"},
            "spec": {"sources": [{"repoURL": "https://charts.io", "chart": "nginx", "targetRevision": "1.2.3"}]}}"#);
        assert_eq!(cell_text(Pane::Application, &chart)[4], "nginx@1.2.3");
        assert!(matches!(enter(Pane::Application, &app), Some(Hop::Tree(Source::Argo { .. }))));
    }

    #[test]
    fn project_to_appset_to_application() {
        let project = obj(r#"{"apiVersion": "argoproj.io/v1alpha1", "kind": "AppProject", "metadata": {"name": "team-a", "namespace": "argocd"}}"#);
        let Some(Hop::Open(sets)) = enter(Pane::AppProject, &project) else { panic!() };
        assert_eq!(sets[0].plural, "applicationsets");
        assert!((sets[0].keep)(&obj(r#"{"apiVersion": "x/v1", "kind": "S", "metadata": {"name": "s"}, "spec": {"template": {"spec": {"project": "team-a"}}}}"#)));
        assert!(!(sets[0].keep)(&obj(r#"{"apiVersion": "x/v1", "kind": "S", "metadata": {"name": "s"}, "spec": {"template": {"spec": {"project": "{{ .team }}"}}}}"#)));

        let Some(Hop::Open(apps)) = related(Pane::AppProject, &project) else { panic!() };
        assert!((apps[0].keep)(&obj(r#"{"apiVersion": "x/v1", "kind": "A", "metadata": {"name": "a"}, "spec": {"project": "team-a"}}"#)));

        let set = obj(r#"{"apiVersion": "argoproj.io/v1alpha1", "kind": "ApplicationSet", "metadata": {"name": "s", "uid": "set-1"}}"#);
        let Some(Hop::Open(owned)) = enter(Pane::ApplicationSet, &set) else { panic!() };
        let app = |uid: &str| obj(&format!(r#"{{"apiVersion": "x/v1", "kind": "A", "metadata": {{"name": "a", "ownerReferences": [{{"apiVersion": "argoproj.io/v1alpha1", "kind": "ApplicationSet", "name": "s", "uid": "{uid}"}}]}}}}"#));
        assert!((owned[0].keep)(&app("set-1")) && !(owned[0].keep)(&app("x")));
    }

    #[test]
    fn appset_status_and_generators() {
        let set = obj(r#"{"apiVersion": "argoproj.io/v1alpha1", "kind": "ApplicationSet", "metadata": {"name": "s"},
            "spec": {"generators": [{"git": {}}, {"list": {}}], "template": {"spec": {"project": "p", "source": {"repoURL": "https://github.com/a/b", "targetRevision": "HEAD"}}}},
            "status": {"conditions": [{"type": "ErrorOccurred", "status": "False"}, {"type": "ResourcesUpToDate", "status": "True"}], "applicationStatus": [{"application": "x"}]}}"#);
        assert_eq!(status(Pane::ApplicationSet, &set), "Healthy");
        assert_eq!(cell_text(Pane::ApplicationSet, &set), ["p", "git,list", "1", "a/b@HEAD"]);
    }
}
