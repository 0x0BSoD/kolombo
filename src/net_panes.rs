//! networking.k8s.io (Ingress, IngressClass, NetworkPolicy) and Gateway API kinds: list cells,
//! describe rows, and the hops Enter takes down the hierarchy
//! GatewayClass -> Gateway -> ListenerSet -> HTTPRoute -> Service -> Pod.

use crate::panes::{Pane, lines, mark, num, selector_string, text};
use crate::resources::ResourceInfo;
use crate::{created_text, field, status_span};
use k8s_openapi::serde_json::Value;
use kube::{ResourceExt, core::DynamicObject};
use ratatui::{
    layout::Constraint,
    style::{Color, Style},
    text::{Line, Span},
};
use std::{collections::HashSet, sync::Arc};

pub const GATEWAY_GROUP: &str = "gateway.networking.k8s.io";
const GATEWAY_X_GROUP: &str = "gateway.networking.x-k8s.io"; // ListenerSet started out here as XListenerSet

fn is_gateway(group: &str) -> bool {
    group == GATEWAY_GROUP || group == GATEWAY_X_GROUP
}

pub fn kind_of(info: &ResourceInfo) -> Option<Pane> {
    let n = info.name.as_str();
    match (info.group.as_str(), n) {
        ("networking.k8s.io", "ingresses") => Some(Pane::Ingress),
        ("networking.k8s.io", "ingressclasses") => Some(Pane::IngressClass),
        ("networking.k8s.io", "networkpolicies") => Some(Pane::NetworkPolicy),
        (g, _) if is_gateway(g) => match n {
            "gatewayclasses" => Some(Pane::GatewayClass),
            "gateways" => Some(Pane::Gateway),
            "listenersets" | "xlistenersets" => Some(Pane::ListenerSet),
            "httproutes" | "grpcroutes" | "tlsroutes" | "tcproutes" | "udproutes" => {
                Some(Pane::Route)
            }
            "referencegrants" => Some(Pane::ReferenceGrant),
            "backendtlspolicies" => Some(Pane::BackendTlsPolicy),
            _ => None,
        },
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

/// `Some(true/false)` for a condition that exists, None when it is missing.
fn flag(conds: &Value, kind: &str) -> Option<bool> {
    arr(conds)
        .find(|c| s(&c["type"]) == kind)
        .map(|c| s(&c["status"]) == "True")
}

// ---------- references ----------

/// `[ns/]name[:section]` of a parentRef; other kinds than Gateway get a `kind/` prefix.
fn parent_label(p: &Value) -> String {
    let kind = p["kind"]
        .as_str()
        .filter(|k| *k != "Gateway")
        .map(|k| format!("{}/", k.to_lowercase()))
        .unwrap_or_default();
    let ns = p["namespace"]
        .as_str()
        .map(|n| format!("{n}/"))
        .unwrap_or_default();
    let section = p["sectionName"]
        .as_str()
        .map(|n| format!(":{n}"))
        .unwrap_or_default();
    format!("{kind}{ns}{}{section}", s(&p["name"]))
}

/// `[kind/][ns/]name[:port]` of a backendRef; Services (the default kind) need no prefix.
fn backend_label(b: &Value) -> String {
    let kind = b["kind"]
        .as_str()
        .filter(|k| *k != "Service")
        .map(|k| format!("{}/", k.to_lowercase()))
        .unwrap_or_default();
    let ns = b["namespace"]
        .as_str()
        .map(|n| format!("{n}/"))
        .unwrap_or_default();
    let port = if b["port"].is_null() {
        String::new()
    } else {
        format!(":{}", text(&b["port"]))
    };
    format!("{kind}{ns}{}{port}", s(&b["name"]))
}

fn backends(o: &DynamicObject) -> Vec<&Value> {
    let rules = &o.data["spec"]["rules"];
    arr(rules)
        .flat_map(|r| {
            arr(&r["backendRefs"])
                .chain(arr(&r["filters"]).map(|f| &f["requestMirror"]["backendRef"]))
        })
        .filter(|b| b.is_object())
        .collect()
}

/// The Services a route sends traffic to, as (namespace, name). A backendRef without `kind` is a
/// Service, without `group` the core group, without `namespace` the route's own.
fn route_services(o: &DynamicObject) -> HashSet<(String, String)> {
    let own = o.namespace().unwrap_or_default();
    backends(o)
        .into_iter()
        .filter(|b| {
            b["kind"].as_str().unwrap_or("Service") == "Service" && s(&b["group"]).is_empty()
        })
        .map(|b| {
            (
                b["namespace"].as_str().unwrap_or(&own).to_string(),
                s(&b["name"]).to_string(),
            )
        })
        .collect()
}

fn ingress_services(o: &DynamicObject) -> HashSet<(String, String)> {
    let spec = &o.data["spec"];
    let paths = arr(&spec["rules"])
        .flat_map(|r| arr(&r["http"]["paths"]))
        .map(|p| &p["backend"]);
    let default = std::iter::once(&spec["defaultBackend"]);
    paths
        .chain(default)
        .map(|b| s(&b["service"]["name"]))
        .filter(|n| !n.is_empty())
        .map(|n| (o.namespace().unwrap_or_default(), n.to_string()))
        .collect()
}

/// Does this parentRef (of an object living in `own_ns`) point at `name` in `ns`, of one of `kinds`?
fn points_at(p: &Value, own_ns: &str, kinds: &[&str], ns: &str, name: &str) -> bool {
    s(&p["name"]) == name
        && kinds.contains(&p["kind"].as_str().unwrap_or("Gateway"))
        && is_gateway(p["group"].as_str().unwrap_or(GATEWAY_GROUP))
        && p["namespace"].as_str().unwrap_or(own_ns) == ns
}

// ---------- hops ----------

pub type Keep = Arc<dyn Fn(&DynamicObject) -> bool + Send + Sync>;

/// One resource type to open, with the test that keeps only the children of the object we came from.
pub struct Target {
    pub group: &'static str,
    pub plural: &'static str,
    pub keep: Keep,
}

pub enum Hop {
    /// The first of these types the cluster serves.
    Open(Vec<Target>),
    /// The pods in the object's namespace matching this selector (None = all of them).
    Pods(Option<String>),
    /// The resource tree of an Argo CD Application or Helm release.
    Tree(crate::tree_view::Source),
}

pub(crate) fn target(
    group: &'static str,
    plural: &'static str,
    keep: impl Fn(&DynamicObject) -> bool + Send + Sync + 'static,
) -> Target {
    Target {
        group,
        plural,
        keep: Arc::new(keep),
    }
}

fn routes_to(kinds: &'static [&'static str], ns: String, name: String) -> Target {
    target(GATEWAY_GROUP, "httproutes", move |r| {
        let own = r.namespace().unwrap_or_default();
        arr(&r.data["spec"]["parentRefs"]).any(|p| points_at(p, &own, kinds, &ns, &name))
    })
}

fn services(wanted: HashSet<(String, String)>) -> Target {
    target("", "services", move |svc| {
        wanted.contains(&(svc.namespace().unwrap_or_default(), svc.name_any()))
    })
}

pub fn has_hop(w: Pane) -> bool {
    matches!(
        w,
        Pane::IngressClass
            | Pane::Ingress
            | Pane::NetworkPolicy
            | Pane::GatewayClass
            | Pane::Gateway
            | Pane::ListenerSet
            | Pane::Route
            | Pane::CronJob
    ) || crate::argo_panes::has_hop(w)
}

/// What Enter opens for this object: the next level down the hierarchy.
pub fn enter(w: Pane, o: &DynamicObject) -> Option<Hop> {
    if crate::argo_panes::has_hop(w) {
        return crate::argo_panes::enter(w, o);
    }
    let (ns, name) = (o.namespace().unwrap_or_default(), o.name_any());
    Some(Hop::Open(match w {
        Pane::GatewayClass => vec![target(GATEWAY_GROUP, "gateways", move |g| {
            s(&g.data["spec"]["gatewayClassName"]) == name
        })],
        Pane::Gateway => {
            // ListenerSets that name this gateway as parent; clusters without that type go straight to routes
            let (n1, name1) = (ns.clone(), name.clone());
            let listener_sets = move |l: &DynamicObject| {
                points_at(
                    &l.data["spec"]["parentRef"],
                    &l.namespace().unwrap_or_default(),
                    &["Gateway"],
                    &n1,
                    &name1,
                )
            };
            vec![
                target(GATEWAY_GROUP, "listenersets", listener_sets.clone()),
                target(GATEWAY_X_GROUP, "xlistenersets", listener_sets),
                routes_to(&["Gateway"], ns, name),
            ]
        }
        Pane::ListenerSet => vec![routes_to(&["ListenerSet", "XListenerSet"], ns, name)],
        Pane::Route => vec![services(route_services(o))],
        Pane::Ingress => vec![services(ingress_services(o))],
        Pane::CronJob => {
            // the Jobs it created: owned by its uid
            let uid = o.metadata.uid.clone().unwrap_or_default();
            vec![target("batch", "jobs", move |j| {
                j.owner_references().iter().any(|r| r.uid == uid)
            })]
        }
        Pane::IngressClass => vec![target("networking.k8s.io", "ingresses", move |i| {
            s(&i.data["spec"]["ingressClassName"]) == name
                || i.annotations()
                    .get("kubernetes.io/ingress.class")
                    .is_some_and(|c| *c == name) // the pre-1.18 way
        })],
        Pane::NetworkPolicy => {
            return Some(Hop::Pods(
                Some(selector_string(&o.data["spec"]["podSelector"])).filter(|l| !l.is_empty()),
            ));
        }
        _ => return None,
    }))
}

/// `r` on a Gateway or ListenerSet: the routes attached to it directly (Enter may go to ListenerSets first).
pub fn routes(w: Pane, o: &DynamicObject) -> Option<Hop> {
    if w == Pane::AppProject {
        return crate::argo_panes::related(w, o);
    }
    let (ns, name) = (o.namespace().unwrap_or_default(), o.name_any());
    match w {
        Pane::Gateway => Some(Hop::Open(vec![routes_to(&["Gateway"], ns, name)])),
        Pane::ListenerSet => Some(Hop::Open(vec![routes_to(
            &["ListenerSet", "XListenerSet"],
            ns,
            name,
        )])),
        _ => None,
    }
}

pub fn has_routes(w: Pane) -> bool {
    matches!(w, Pane::Gateway | Pane::ListenerSet | Pane::AppProject)
}

// ---------- list ----------

pub fn columns(w: Pane) -> Vec<(&'static str, Constraint)> {
    let n = |t, w| (t, Constraint::Length(w));
    let fill = |t| (t, Constraint::Fill(1));
    match w {
        Pane::IngressClass => vec![fill("CONTROLLER"), n("DEFAULT", 7)],
        Pane::Ingress => vec![
            n("CLASS", 14),
            fill("HOSTS"),
            n("ADDRESS", 18),
            n("PORTS", 8),
        ],
        Pane::NetworkPolicy => vec![
            fill("POD-SELECTOR"),
            n("TYPES", 16),
            n("INGRESS", 7),
            n("EGRESS", 6),
        ],
        Pane::GatewayClass => vec![fill("CONTROLLER")],
        Pane::Gateway => vec![
            n("CLASS", 18),
            n("ADDRESS", 20),
            fill("LISTENERS"),
            n("ATTACHED", 8),
        ],
        Pane::ListenerSet => vec![fill("PARENT"), fill("LISTENERS")],
        Pane::Route => vec![fill("HOSTNAMES"), fill("PARENTS"), fill("BACKENDS")],
        Pane::ReferenceGrant => vec![fill("FROM"), fill("TO")],
        Pane::BackendTlsPolicy => vec![fill("TARGETS"), fill("HOSTNAME")],
        _ => vec![],
    }
}

/// Worst answer among the controllers that looked at a route (or a policy): one refusal is what the user must see.
fn worst(parents: &Value) -> String {
    let (mut any, mut unresolved) = (false, false);
    for p in arr(parents) {
        any = true;
        if flag(&p["conditions"], "Accepted") == Some(false) {
            return "NotAccepted".into();
        }
        unresolved |= flag(&p["conditions"], "ResolvedRefs") == Some(false);
    }
    if !any {
        "Pending".into()
    } else if unresolved {
        "UnresolvedRefs".into()
    } else {
        "Accepted".into()
    }
}

pub fn status(w: Pane, o: &DynamicObject) -> String {
    let st = &o.data["status"];
    match w {
        Pane::GatewayClass | Pane::Gateway | Pane::ListenerSet => match (
            flag(&st["conditions"], "Accepted"),
            flag(&st["conditions"], "Programmed"),
        ) {
            (Some(false), _) => "NotAccepted".into(),
            (_, Some(false)) => "NotProgrammed".into(),
            (_, Some(true)) => "Programmed".into(),
            (Some(true), None) => "Accepted".into(),
            _ => "Pending".into(),
        },
        Pane::Route => worst(&st["parents"]),
        Pane::BackendTlsPolicy => worst(&st["ancestors"]),
        _ => String::new(),
    }
}

fn address(o: &DynamicObject) -> String {
    let lb = &o.data["status"]["loadBalancer"]["ingress"][0];
    [&lb["ip"], &lb["hostname"]]
        .into_iter()
        .map(|v| s(v))
        .find(|a| !a.is_empty())
        .unwrap_or_default()
        .to_string()
}

fn listeners(o: &DynamicObject) -> String {
    join(
        arr(&o.data["spec"]["listeners"]).map(|l| {
            format!(
                "{}:{}/{}",
                s(&l["name"]),
                text(&l["port"]),
                s(&l["protocol"])
            )
        }),
        ",",
    )
}

fn hostnames(o: &DynamicObject) -> String {
    let h = join(
        arr(&o.data["spec"]["hostnames"]).map(|h| s(h).to_string()),
        ",",
    );
    if h.is_empty() { "*".into() } else { h }
}

fn ref_parent(p: &Value) -> String {
    let ns = p["namespace"]
        .as_str()
        .map(|n| format!("{n}/"))
        .unwrap_or_default();
    format!("{ns}{}", s(&p["name"]))
}

fn target_list(o: &DynamicObject) -> String {
    join(
        arr(&o.data["spec"]["targetRefs"])
            .map(|t| format!("{}/{}", s(&t["kind"]).to_lowercase(), s(&t["name"]))),
        ",",
    )
}

fn grant_side(v: &Value) -> String {
    join(
        arr(v).map(|x| {
            format!(
                "{}{}",
                s(&x["kind"]),
                x["namespace"]
                    .as_str()
                    .map(|n| format!("@{n}"))
                    .unwrap_or_default()
            )
        }),
        ",",
    )
}

fn rule_count(o: &DynamicObject, key: &str) -> u64 {
    o.data["spec"][key].as_array().map_or(0, |a| a.len() as u64)
}

fn policy_types(o: &DynamicObject) -> Vec<String> {
    let t: Vec<String> = arr(&o.data["spec"]["policyTypes"])
        .map(|t| s(t).to_string())
        .collect();
    if t.is_empty() {
        vec!["Ingress".into()]
    } else {
        t
    } // the API default
}

fn pod_selector(o: &DynamicObject) -> String {
    let sel = selector_string(&o.data["spec"]["podSelector"]);
    if sel.is_empty() {
        "<all pods>".into()
    } else {
        sel
    }
}

pub fn cells(w: Pane, o: &DynamicObject) -> Vec<(String, Color)> {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let plain = |t: String| (t, Color::Reset);
    match w {
        Pane::IngressClass => {
            let default = o
                .annotations()
                .get("ingressclass.kubernetes.io/is-default-class")
                .is_some_and(|v| v == "true");
            vec![
                plain(text(&spec["controller"])),
                (if default { "yes" } else { "" }.into(), Color::Green),
            ]
        }
        Pane::Ingress => {
            let class = spec["ingressClassName"]
                .as_str()
                .map(String::from)
                .or_else(|| o.annotations().get("kubernetes.io/ingress.class").cloned());
            let mut hosts: Vec<String> = arr(&spec["rules"])
                .map(|r| r["host"].as_str().unwrap_or("*").to_string())
                .collect();
            hosts.dedup();
            let ports = if spec["tls"].as_array().is_some_and(|t| !t.is_empty()) {
                "80, 443"
            } else {
                "80"
            };
            vec![
                plain(class.unwrap_or("<none>".into())),
                plain(join(hosts, ",")),
                plain(address(o)),
                plain(ports.into()),
            ]
        }
        Pane::NetworkPolicy => vec![
            plain(pod_selector(o)),
            plain(join(policy_types(o), ",")),
            plain(rule_count(o, "ingress").to_string()),
            plain(rule_count(o, "egress").to_string()),
        ],
        Pane::GatewayClass => vec![plain(text(&spec["controllerName"]))],
        Pane::Gateway => {
            let attached: u64 = arr(&st["listeners"])
                .map(|l| num(&l["attachedRoutes"]))
                .sum();
            vec![
                plain(text(&spec["gatewayClassName"])),
                plain(s(&st["addresses"][0]["value"]).to_string()),
                plain(listeners(o)),
                plain(attached.to_string()),
            ]
        }
        Pane::ListenerSet => vec![plain(ref_parent(&spec["parentRef"])), plain(listeners(o))],
        Pane::Route => vec![
            plain(hostnames(o)),
            plain(join(arr(&spec["parentRefs"]).map(parent_label), ",")),
            plain(join(
                backends(o)
                    .into_iter()
                    .map(backend_label)
                    .collect::<std::collections::BTreeSet<_>>(),
                ",",
            )),
        ],
        Pane::ReferenceGrant => vec![
            plain(grant_side(&spec["from"])),
            plain(grant_side(&spec["to"])),
        ],
        Pane::BackendTlsPolicy => vec![
            plain(target_list(o)),
            plain(text(&spec["validation"]["hostname"])),
        ],
        _ => vec![],
    }
}

// ---------- describe ----------

fn dim() -> Style {
    Style::new().fg(Color::DarkGray)
}

fn condition_lines(conds: &Value) -> Vec<Vec<Span<'static>>> {
    arr(conds)
        .map(|c| {
            vec![
                mark(s(&c["status"]) == "True"),
                Span::raw(format!(" {}  ", s(&c["type"]))),
                Span::styled(s(&c["reason"]).to_string(), dim()),
            ]
        })
        .collect()
}

fn selector_text(sel: &Value) -> String {
    let t = selector_string(sel);
    if t.is_empty() { "*".into() } else { t }
}

fn peer(p: &Value) -> String {
    if let Some(block) = p.get("ipBlock") {
        let except = join(arr(&block["except"]).map(|e| s(e).to_string()), ",");
        let except = if except.is_empty() {
            String::new()
        } else {
            format!(" except {except}")
        };
        return format!("cidr {}{except}", s(&block["cidr"]));
    }
    let mut parts = Vec::new();
    if p.get("namespaceSelector").is_some() {
        parts.push(format!(
            "namespaces {{{}}}",
            selector_text(&p["namespaceSelector"])
        ));
    }
    if p.get("podSelector").is_some() {
        parts.push(format!("pods {{{}}}", selector_text(&p["podSelector"])));
    }
    parts.join(" in ")
}

fn ports(v: &Value) -> String {
    join(
        arr(v).map(|p| {
            let end = p["endPort"]
                .as_u64()
                .map(|e| format!("-{e}"))
                .unwrap_or_default();
            format!(
                "{}/{}{end}",
                p["protocol"].as_str().unwrap_or("TCP"),
                text(&p["port"])
            )
        }),
        ",",
    )
}

/// One line per rule: who, and which ports.
fn policy_rules(rules: &Value, peers: &str) -> Vec<Vec<Span<'static>>> {
    let all = rules.as_array().map(|a| a.is_empty()).unwrap_or(true);
    if all {
        return vec![vec![Span::styled("<none>", dim())]];
    }
    arr(rules)
        .map(|r| {
            let who = join(arr(&r[peers]).map(peer), "; ");
            let who = if who.is_empty() {
                "any peer".into()
            } else {
                who
            };
            let port = ports(&r["ports"]);
            vec![
                Span::raw(format!("{who}  ")),
                Span::styled(
                    if port.is_empty() {
                        "any port".into()
                    } else {
                        port
                    },
                    dim(),
                ),
            ]
        })
        .collect()
}

fn listener_lines(o: &DynamicObject) -> Vec<Vec<Span<'static>>> {
    let status: Vec<&Value> = arr(&o.data["status"]["listeners"]).collect();
    arr(&o.data["spec"]["listeners"])
        .map(|l| {
            let name = s(&l["name"]);
            let attached = status
                .iter()
                .find(|x| s(&x["name"]) == name)
                .map_or(0, |x| num(&x["attachedRoutes"]));
            let host = l["hostname"]
                .as_str()
                .map(|h| format!(" {h}"))
                .unwrap_or_default();
            let tls = l["tls"]["mode"]
                .as_str()
                .map(|m| format!(" tls:{m}"))
                .unwrap_or_default();
            let from = l["allowedRoutes"]["namespaces"]["from"]
                .as_str()
                .map(|f| format!(" from:{f}"))
                .unwrap_or_default();
            vec![
                Span::styled(format!("{name}  "), dim()),
                Span::raw(format!(
                    "{}/{}{host}{tls}{from}  ",
                    text(&l["port"]),
                    s(&l["protocol"])
                )),
                Span::styled(format!("{attached} routes"), dim()),
            ]
        })
        .collect()
}

fn param_text(p: &Value) -> String {
    if p.is_null() {
        String::new()
    } else {
        format!(
            "{}/{} {}",
            s(&p["kind"]),
            s(&p["name"]),
            p["namespace"]
                .as_str()
                .map(|n| format!("(ns {n})"))
                .unwrap_or_default()
        )
        .trim()
        .into()
    }
}

/// What the route does with a request, one line per rule: matches -> backends.
fn rule_lines(o: &DynamicObject) -> Vec<Vec<Span<'static>>> {
    arr(&o.data["spec"]["rules"])
        .map(|r| {
            let matches = join(
                arr(&r["matches"]).map(|m| {
                    let method = m["method"]
                        .as_str()
                        .map(|x| format!("{x} "))
                        .unwrap_or_default();
                    let path = &m["path"];
                    let p = if path.is_null() {
                        String::new()
                    } else {
                        format!(
                            "{}{}",
                            s(&path["value"]),
                            if s(&path["type"]) == "PathPrefix" {
                                "*"
                            } else {
                                ""
                            }
                        )
                    };
                    format!("{method}{p}")
                }),
                ", ",
            );
            let matches = if matches.is_empty() {
                "all requests".into()
            } else {
                matches
            };
            let to = join(arr(&r["backendRefs"]).map(|b| backend_label(b)), ", ");
            vec![
                Span::raw(format!("{matches}  ")),
                Span::styled("→ ", dim()),
                Span::raw(if to.is_empty() {
                    "<no backend>".into()
                } else {
                    to
                }),
            ]
        })
        .collect()
}

/// Each parentRef with what its controller said about it.
fn parent_lines(o: &DynamicObject) -> Vec<Vec<Span<'static>>> {
    let answers: Vec<&Value> = arr(&o.data["status"]["parents"]).collect();
    arr(&o.data["spec"]["parentRefs"])
        .map(|p| {
            let a = answers.iter().find(|a| {
                s(&a["parentRef"]["name"]) == s(&p["name"])
                    && (p["sectionName"].is_null()
                        || a["parentRef"]["sectionName"] == p["sectionName"])
            });
            let mut v = vec![Span::raw(format!("{}  ", parent_label(p)))];
            match a {
                Some(a) => {
                    for kind in ["Accepted", "ResolvedRefs"] {
                        if let Some(ok) = flag(&a["conditions"], kind) {
                            v.push(mark(ok));
                            v.push(Span::styled(format!("{kind} "), dim()));
                        }
                    }
                }
                None => v.push(Span::styled("no status yet", dim())),
            }
            v
        })
        .collect()
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
        Pane::IngressClass => {
            rows.push(field("Controller", Span::raw(text(&spec["controller"]))));
            rows.push(field(
                "Parameters",
                Span::raw(param_text(&spec["parameters"])),
            ));
        }
        Pane::Ingress => {
            let class = text(&spec["ingressClassName"]);
            rows.push(field(
                "Class",
                Span::raw(if class.is_empty() {
                    "<none>".into()
                } else {
                    class
                }),
            ));
            rows.push(field("Address", Span::raw(address(o))));
            let paths: Vec<Vec<Span>> = arr(&spec["rules"])
                .flat_map(|r| {
                    let host = r["host"].as_str().unwrap_or("*").to_string();
                    arr(&r["http"]["paths"]).map(move |p| {
                        let svc = &p["backend"]["service"];
                        let port = if svc["port"]["name"].is_null() {
                            text(&svc["port"]["number"])
                        } else {
                            text(&svc["port"]["name"])
                        };
                        vec![
                            Span::raw(format!("{host}{}  ", s(&p["path"]))),
                            Span::styled("→ ", dim()),
                            Span::raw(format!("{}:{port}", s(&svc["name"]))),
                        ]
                    })
                })
                .collect();
            rows.extend(lines("Rules", nonempty(paths)));
            if let Some(d) = spec["defaultBackend"]["service"]["name"].as_str() {
                rows.push(field("Default", Span::raw(d.to_string())));
            }
            let tls: Vec<Vec<Span>> = arr(&spec["tls"])
                .map(|t| {
                    vec![
                        Span::raw(join(arr(&t["hosts"]).map(|h| s(h).to_string()), ",")),
                        Span::styled(format!("  secret {}", s(&t["secretName"])), dim()),
                    ]
                })
                .collect();
            rows.extend(lines("TLS", nonempty(tls)));
        }
        Pane::NetworkPolicy => {
            rows.push(field("Pods", Span::raw(pod_selector(o))));
            rows.push(field("Types", Span::raw(join(policy_types(o), ", "))));
            rows.extend(lines("Ingress", policy_rules(&spec["ingress"], "from")));
            rows.extend(lines("Egress", policy_rules(&spec["egress"], "to")));
        }
        Pane::GatewayClass => {
            rows.push(field(
                "Controller",
                Span::raw(text(&spec["controllerName"])),
            ));
            rows.push(field("Description", Span::raw(text(&spec["description"]))));
            rows.push(field(
                "Parameters",
                Span::raw(param_text(&spec["parametersRef"])),
            ));
            rows.extend(lines("Conditions", condition_lines(&st["conditions"])));
        }
        Pane::Gateway => {
            rows.push(field("Class", Span::raw(text(&spec["gatewayClassName"]))));
            rows.push(field(
                "Addresses",
                Span::raw(join(
                    arr(&st["addresses"]).map(|a| s(&a["value"]).to_string()),
                    ", ",
                )),
            ));
            rows.extend(lines("Listeners", nonempty(listener_lines(o))));
            rows.extend(lines("Conditions", condition_lines(&st["conditions"])));
        }
        Pane::ListenerSet => {
            rows.push(field("Parent", Span::raw(ref_parent(&spec["parentRef"]))));
            rows.extend(lines("Listeners", nonempty(listener_lines(o))));
            rows.extend(lines("Conditions", condition_lines(&st["conditions"])));
        }
        Pane::Route => {
            rows.push(field("Hostnames", Span::raw(hostnames(o))));
            rows.extend(lines("Parents", nonempty(parent_lines(o))));
            rows.extend(lines("Rules", nonempty(rule_lines(o))));
        }
        Pane::ReferenceGrant => {
            let side = |v: &Value| -> Vec<Vec<Span<'static>>> {
                arr(v)
                    .map(|x| {
                        let group = if s(&x["group"]).is_empty() {
                            "core".to_string()
                        } else {
                            s(&x["group"]).to_string()
                        };
                        let ns = x["namespace"]
                            .as_str()
                            .map(|n| format!(" in {n}"))
                            .unwrap_or_default();
                        let name = x["name"]
                            .as_str()
                            .map(|n| format!(" {n}"))
                            .unwrap_or_default();
                        vec![Span::raw(format!("{} ({group}){name}{ns}", s(&x["kind"])))]
                    })
                    .collect()
            };
            rows.extend(lines("From", nonempty(side(&spec["from"]))));
            rows.extend(lines("To", nonempty(side(&spec["to"]))));
        }
        Pane::BackendTlsPolicy => {
            rows.push(field("Targets", Span::raw(target_list(o))));
            rows.push(field(
                "Hostname",
                Span::raw(text(&spec["validation"]["hostname"])),
            ));
            let cas = join(
                arr(&spec["validation"]["caCertificateRefs"])
                    .map(|c| format!("{}/{}", s(&c["kind"]).to_lowercase(), s(&c["name"]))),
                ", ",
            );
            let ca = if cas.is_empty() {
                text(&spec["validation"]["wellKnownCACertificates"])
            } else {
                cas
            };
            rows.push(field("CA", Span::raw(ca)));
            let conds: Vec<Vec<Span>> = arr(&st["ancestors"])
                .flat_map(|a| condition_lines(&a["conditions"]))
                .collect();
            rows.extend(lines("Conditions", conds));
        }
        _ => {}
    }
    rows.push(field("Created", Span::raw(created_text(&o.metadata))));
    rows.push(field(
        "UID",
        Span::raw(o.metadata.uid.clone().unwrap_or_default()),
    ));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(json: &str) -> DynamicObject {
        serde_yaml_ng::from_str(json).unwrap()
    }

    fn keeps(t: &Target, json: &str) -> bool {
        (t.keep)(&obj(json))
    }

    fn open(h: Option<Hop>) -> Vec<Target> {
        match h {
            Some(Hop::Open(t)) => t,
            _ => panic!("expected Open"),
        }
    }

    #[test]
    fn every_kind_has_one_cell_per_column() {
        let o = obj(r#"{"apiVersion": "x/v1", "kind": "X", "metadata": {"name": "x"}}"#);
        for w in [
            Pane::IngressClass,
            Pane::Ingress,
            Pane::NetworkPolicy,
            Pane::GatewayClass,
            Pane::Gateway,
            Pane::ListenerSet,
            Pane::Route,
            Pane::ReferenceGrant,
            Pane::BackendTlsPolicy,
        ] {
            assert_eq!(columns(w).len(), cells(w, &o).len(), "{w:?}");
            assert!(!info_rows(w, &o).is_empty(), "{w:?}");
            // the same answer through the dispatcher in panes.rs
            assert_eq!(
                crate::panes::columns(w).len(),
                crate::panes::cells(w, &o).len(),
                "{w:?}"
            );
        }
    }

    #[test]
    fn kinds_are_recognised_by_group_and_plural() {
        let info = |group: &str, name: &str| ResourceInfo {
            group: group.into(),
            name: name.into(),
            ..Default::default()
        };
        assert_eq!(
            kind_of(&info("gateway.networking.k8s.io", "httproutes")),
            Some(Pane::Route)
        );
        assert_eq!(
            kind_of(&info("gateway.networking.x-k8s.io", "xlistenersets")),
            Some(Pane::ListenerSet)
        );
        assert_eq!(
            kind_of(&info("networking.k8s.io", "ingresses")),
            Some(Pane::Ingress)
        );
        assert_eq!(kind_of(&info("other.io", "gateways")), None);
    }

    #[test]
    fn gateway_class_lists_gateways_of_that_class_in_any_namespace() {
        let class = obj(
            r#"{"apiVersion": "gateway.networking.k8s.io/v1", "kind": "GatewayClass", "metadata": {"name": "nginx"}}"#,
        );
        let t = open(enter(Pane::GatewayClass, &class));
        assert_eq!((t[0].group, t[0].plural), (GATEWAY_GROUP, "gateways"));
        assert!(keeps(
            &t[0],
            r#"{"apiVersion": "x/v1", "kind": "G", "metadata": {"name": "a", "namespace": "n1"}, "spec": {"gatewayClassName": "nginx"}}"#
        ));
        assert!(!keeps(
            &t[0],
            r#"{"apiVersion": "x/v1", "kind": "G", "metadata": {"name": "b"}, "spec": {"gatewayClassName": "other"}}"#
        ));
    }

    #[test]
    fn parent_refs_default_to_gateway_kind_and_the_routes_own_namespace() {
        let gw = obj(
            r#"{"apiVersion": "gateway.networking.k8s.io/v1", "kind": "Gateway", "metadata": {"name": "gw", "namespace": "infra"}}"#,
        );
        let routes = open(routes(Pane::Gateway, &gw));
        let route = |ns: &str, parent: &str| {
            format!(
                r#"{{"apiVersion": "x/v1", "kind": "R", "metadata": {{"name": "r", "namespace": "{ns}"}}, "spec": {{"parentRefs": [{parent}]}}}}"#
            )
        };
        assert!(keeps(
            &routes[0],
            &route("apps", r#"{"name": "gw", "namespace": "infra"}"#)
        ));
        assert!(keeps(&routes[0], &route("infra", r#"{"name": "gw"}"#))); // no namespace: the route's own
        assert!(!keeps(&routes[0], &route("apps", r#"{"name": "gw"}"#))); // that is apps/gw, a different gateway
        assert!(!keeps(
            &routes[0],
            &route("infra", r#"{"name": "gw", "kind": "ListenerSet"}"#)
        ));
        assert!(!keeps(
            &routes[0],
            &route("infra", r#"{"name": "gw", "group": "other.io"}"#)
        ));

        // Enter on a gateway tries ListenerSets first (both groups), then routes
        let chain = open(enter(Pane::Gateway, &gw));
        assert_eq!(
            chain.iter().map(|t| t.plural).collect::<Vec<_>>(),
            ["listenersets", "xlistenersets", "httproutes"]
        );
        let ls = |parent: &str| {
            format!(
                r#"{{"apiVersion": "x/v1", "kind": "L", "metadata": {{"name": "l", "namespace": "infra"}}, "spec": {{"parentRef": {parent}}}}}"#
            )
        };
        assert!(keeps(&chain[0], &ls(r#"{"name": "gw"}"#)));
        assert!(!keeps(&chain[0], &ls(r#"{"name": "other"}"#)));

        // a ListenerSet is the parent of routes through kind: ListenerSet
        let set = obj(
            r#"{"apiVersion": "x/v1", "kind": "L", "metadata": {"name": "l", "namespace": "infra"}}"#,
        );
        let via = open(enter(Pane::ListenerSet, &set));
        assert!(keeps(
            &via[0],
            &route("infra", r#"{"name": "l", "kind": "ListenerSet"}"#)
        ));
        assert!(!keeps(&via[0], &route("infra", r#"{"name": "l"}"#))); // that names a Gateway
    }

    #[test]
    fn route_backends_default_to_services_in_the_routes_namespace() {
        let r = obj(
            r#"{"apiVersion": "x/v1", "kind": "HTTPRoute", "metadata": {"name": "r", "namespace": "apps"},
            "spec": {"rules": [{"backendRefs": [{"name": "web", "port": 80}, {"name": "api", "namespace": "backend"},
                                                 {"name": "bucket", "kind": "Bucket", "group": "x.io"}],
                                "filters": [{"type": "RequestMirror", "requestMirror": {"backendRef": {"name": "shadow"}}}]}]}}"#,
        );
        let t = open(enter(Pane::Route, &r));
        let svc = |ns: &str, n: &str| {
            format!(
                r#"{{"apiVersion": "v1", "kind": "Service", "metadata": {{"name": "{n}", "namespace": "{ns}"}}}}"#
            )
        };
        assert!(keeps(&t[0], &svc("apps", "web")));
        assert!(keeps(&t[0], &svc("backend", "api")));
        assert!(keeps(&t[0], &svc("apps", "shadow")));
        assert!(!keeps(&t[0], &svc("apps", "api"))); // api lives in backend
        assert!(!keeps(&t[0], &svc("apps", "bucket"))); // not a Service
        assert_eq!(
            cells(Pane::Route, &r)[2].0,
            "backend/api,bucket/bucket,shadow,web:80"
        );
    }

    #[test]
    fn ingress_hops_and_status() {
        let ing = obj(
            r#"{"apiVersion": "networking.k8s.io/v1", "kind": "Ingress", "metadata": {"name": "i", "namespace": "apps"},
            "spec": {"ingressClassName": "nginx", "defaultBackend": {"service": {"name": "fallback", "port": {"number": 80}}},
                     "rules": [{"host": "a.io", "http": {"paths": [{"path": "/", "pathType": "Prefix", "backend": {"service": {"name": "web", "port": {"name": "http"}}}}]}}],
                     "tls": [{"hosts": ["a.io"], "secretName": "cert"}]},
            "status": {"loadBalancer": {"ingress": [{"ip": "1.2.3.4"}]}}}"#,
        );
        let t = open(enter(Pane::Ingress, &ing));
        let svc = |n: &str| {
            format!(
                r#"{{"apiVersion": "v1", "kind": "Service", "metadata": {{"name": "{n}", "namespace": "apps"}}}}"#
            )
        };
        assert!(
            keeps(&t[0], &svc("web"))
                && keeps(&t[0], &svc("fallback"))
                && !keeps(&t[0], &svc("db"))
        );
        let c: Vec<String> = cells(Pane::Ingress, &ing)
            .into_iter()
            .map(|(t, _)| t)
            .collect();
        assert_eq!(c, ["nginx", "a.io", "1.2.3.4", "80, 443"]);

        let class = obj(
            r#"{"apiVersion": "networking.k8s.io/v1", "kind": "IngressClass", "metadata": {"name": "nginx"}}"#,
        );
        let t = open(enter(Pane::IngressClass, &class));
        assert!(keeps(
            &t[0],
            r#"{"apiVersion": "x/v1", "kind": "I", "metadata": {"name": "a"}, "spec": {"ingressClassName": "nginx"}}"#
        ));
        assert!(keeps(
            &t[0],
            r#"{"apiVersion": "x/v1", "kind": "I", "metadata": {"name": "a", "annotations": {"kubernetes.io/ingress.class": "nginx"}}}"#
        ));
        assert!(!keeps(
            &t[0],
            r#"{"apiVersion": "x/v1", "kind": "I", "metadata": {"name": "a"}}"#
        ));
    }

    #[test]
    fn network_policy_opens_its_pods_and_summarises_rules() {
        let np = obj(
            r#"{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "p", "namespace": "apps"},
            "spec": {"podSelector": {"matchLabels": {"app": "web"}}, "ingress": [{"from": [{"podSelector": {"matchLabels": {"role": "fe"}}}], "ports": [{"port": 80}]}]}}"#,
        );
        match enter(Pane::NetworkPolicy, &np) {
            Some(Hop::Pods(Some(l))) => assert_eq!(l, "app=web"),
            _ => panic!("expected pods"),
        }
        let text: String = info_rows(Pane::NetworkPolicy, &np)
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("pods {role=fe}") && text.contains("TCP/80"),
            "{text}"
        );
        let all = obj(
            r#"{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "p"}, "spec": {"podSelector": {}}}"#,
        );
        assert!(matches!(
            enter(Pane::NetworkPolicy, &all),
            Some(Hop::Pods(None))
        )); // empty selector = every pod
    }

    #[test]
    fn gateway_and_route_status_words() {
        let gw = obj(
            r#"{"apiVersion": "x/v1", "kind": "Gateway", "metadata": {"name": "g"}, "spec": {"gatewayClassName": "c", "listeners": [{"name": "http", "port": 80, "protocol": "HTTP"}]},
            "status": {"addresses": [{"value": "10.0.0.1"}], "conditions": [{"type": "Accepted", "status": "True"}, {"type": "Programmed", "status": "True"}],
                       "listeners": [{"name": "http", "attachedRoutes": 3}, {"name": "x", "attachedRoutes": 2}]}}"#,
        );
        assert_eq!(status(Pane::Gateway, &gw), "Programmed");
        let c: Vec<String> = cells(Pane::Gateway, &gw)
            .into_iter()
            .map(|(t, _)| t)
            .collect();
        assert_eq!(c, ["c", "10.0.0.1", "http:80/HTTP", "5"]);
        let bad = obj(
            r#"{"apiVersion": "x/v1", "kind": "HTTPRoute", "metadata": {"name": "r"},
            "status": {"parents": [{"conditions": [{"type": "Accepted", "status": "True"}, {"type": "ResolvedRefs", "status": "False"}]}]}}"#,
        );
        assert_eq!(status(Pane::Route, &bad), "UnresolvedRefs");
        let none = obj(r#"{"apiVersion": "x/v1", "kind": "HTTPRoute", "metadata": {"name": "r"}}"#);
        assert_eq!(status(Pane::Route, &none), "Pending");
    }
}
