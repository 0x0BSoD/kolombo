//! core/v1 types read as untyped objects: list columns, a status word and describe rows for
//! Services, Endpoints, ConfigMaps, Secrets, volumes, ServiceAccounts, Events, quotas and limits.
//! Secret values are never read into a row, only key names and sizes.

use crate::panes::{Pane, desired, images, label_selector, lines, mark, num, owner, ratio, text};
use crate::{created_text, field, format_age, parse_cpu, parse_mem, status_span};
use k8s_openapi::{jiff::Timestamp, serde_json::Value};
use kube::{ResourceExt, core::DynamicObject};
use ratatui::{
    layout::Constraint,
    style::{Color, Style},
    text::{Line, Span},
};
use std::collections::BTreeSet;

const DIM: Style = Style::new().fg(Color::DarkGray);
const MAX_LINES: usize = 20; // shortcut: long lists are cut here, `y` (YAML) shows everything

fn strings(v: &Value) -> Vec<String> {
    v.as_array().into_iter().flatten().map(text).collect()
}

fn keys(v: &Value) -> Vec<String> {
    v.as_object()
        .into_iter()
        .flatten()
        .map(|(k, _)| k.clone())
        .collect()
}

/// `a, b, c +2`
fn joined(items: &[String], shown: usize) -> String {
    let head = items
        .iter()
        .take(shown)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if items.len() > shown {
        format!("{head} +{}", items.len() - shown)
    } else {
        head
    }
}

fn or_none(s: String) -> String {
    if s.is_empty() { "<none>".into() } else { s }
}

fn seconds(v: &Value) -> Option<i64> {
    v.as_str()?.parse::<Timestamp>().ok().map(|t| t.as_second())
}

fn access(v: &Value) -> String {
    strings(v)
        .iter()
        .map(|m| match m.as_str() {
            "ReadWriteOnce" => "RWO",
            "ReadOnlyMany" => "ROX",
            "ReadWriteMany" => "RWX",
            "ReadWriteOncePod" => "RWOP",
            other => other,
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Rough decoded size of a base64 value.
fn b64_bytes(s: &str) -> usize {
    s.trim_end_matches('=').len() * 3 / 4
}

fn size(bytes: usize) -> String {
    if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

// ---- Service ------------------------------------------------------------------------------

fn svc_type(o: &DynamicObject) -> String {
    Some(text(&o.data["spec"]["type"]))
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| "ClusterIP".into())
}

fn lb_ingress(o: &DynamicObject) -> Vec<String> {
    o.data["status"]["loadBalancer"]["ingress"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|i| {
            Some(text(&i["ip"]))
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| text(&i["hostname"]))
        })
        .collect()
}

fn external(o: &DynamicObject) -> String {
    let spec = &o.data["spec"];
    if svc_type(o) == "ExternalName" {
        return text(&spec["externalName"]);
    }
    let mut all = lb_ingress(o);
    all.extend(strings(&spec["externalIPs"]));
    if all.is_empty() {
        return if svc_type(o) == "LoadBalancer" {
            "<pending>".into()
        } else {
            "<none>".into()
        };
    }
    all.join(",")
}

/// `80:30080/TCP` like kubectl: the node port after a colon when there is one.
fn svc_ports(o: &DynamicObject) -> Vec<String> {
    o.data["spec"]["ports"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|p| {
            let proto = Some(text(&p["protocol"]))
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "TCP".into());
            match num(&p["nodePort"]) {
                0 => format!("{}/{proto}", num(&p["port"])),
                node => format!("{}:{node}/{proto}", num(&p["port"])),
            }
        })
        .collect()
}

// ---- Endpoints ----------------------------------------------------------------------------

struct Address {
    endpoint: String, // ip or ip:port
    ready: bool,
    target: String, // the pod behind it
}

fn addresses(o: &DynamicObject) -> Vec<Address> {
    let mut out = Vec::new();
    for subset in o.data["subsets"].as_array().into_iter().flatten() {
        let ports: Vec<u64> = subset["ports"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| num(&p["port"]))
            .collect();
        for (key, ready) in [("addresses", true), ("notReadyAddresses", false)] {
            for a in subset[key].as_array().into_iter().flatten() {
                let ip = text(&a["ip"]);
                let target = text(&a["targetRef"]["name"]);
                if ports.is_empty() {
                    out.push(Address {
                        endpoint: ip,
                        ready,
                        target,
                    });
                } else {
                    out.extend(ports.iter().map(|p| Address {
                        endpoint: format!("{ip}:{p}"),
                        ready,
                        target: target.clone(),
                    }));
                }
            }
        }
    }
    out
}

// ---- ResourceQuota / LimitRange -----------------------------------------------------------

fn quantity(key: &str, v: &str) -> Option<u64> {
    if key.ends_with("cpu") {
        parse_cpu(v)
    } else {
        parse_mem(v)
    }
}

/// (resource, used, hard), for every resource the quota limits.
fn quota(o: &DynamicObject) -> Vec<(String, String, String)> {
    let hard = if o.data["status"]["hard"].is_object() {
        &o.data["status"]["hard"]
    } else {
        &o.data["spec"]["hard"]
    };
    keys(hard)
        .into_iter()
        .map(|k| (text(&o.data["status"]["used"][&k]), text(&hard[&k]), k))
        .map(|(u, h, k)| (k, u, h))
        .collect()
}

fn at_limit(used: &str, hard: &str, key: &str) -> bool {
    matches!((quantity(key, used), quantity(key, hard)), (Some(u), Some(h)) if h > 0 && u >= h)
}

fn limit_parts(l: &Value) -> String {
    [
        "min",
        "max",
        "default",
        "defaultRequest",
        "maxLimitRequestRatio",
    ]
    .iter()
    .filter_map(|k| {
        let m = l[*k].as_object().filter(|m| !m.is_empty())?;
        let pairs = m
            .iter()
            .map(|(r, v)| format!("{r}={}", text(v)))
            .collect::<Vec<_>>()
            .join(",");
        Some(format!("{k} {pairs}"))
    })
    .collect::<Vec<_>>()
    .join(" · ")
}

// ---- PersistentVolume ---------------------------------------------------------------------

/// The volume source is whichever spec key is not one of the common fields (`csi`, `nfs`, `hostPath`…).
fn pv_source(o: &DynamicObject) -> String {
    const COMMON: [&str; 10] = [
        "accessModes",
        "capacity",
        "claimRef",
        "persistentVolumeReclaimPolicy",
        "storageClassName",
        "volumeMode",
        "nodeAffinity",
        "mountOptions",
        "volumeAttributesClassName",
        "claimPolicy",
    ];
    let spec = &o.data["spec"];
    let Some(kind) = keys(spec)
        .into_iter()
        .find(|k| !COMMON.contains(&k.as_str()))
    else {
        return String::new();
    };
    let detail = match kind.as_str() {
        "csi" => format!(
            "{} {}",
            text(&spec["csi"]["driver"]),
            text(&spec["csi"]["volumeHandle"])
        ),
        "nfs" => format!(
            "{}:{}",
            text(&spec["nfs"]["server"]),
            text(&spec["nfs"]["path"])
        ),
        "hostPath" | "local" => text(&spec[&kind]["path"]),
        _ => String::new(),
    };
    format!("{kind} {detail}").trim().to_string()
}

fn claim(o: &DynamicObject) -> String {
    let c = &o.data["spec"]["claimRef"];
    if c.is_null() {
        String::new()
    } else {
        format!("{}/{}", text(&c["namespace"]), text(&c["name"]))
    }
}

// ---- the four functions panes.rs dispatches to --------------------------------------------

pub fn columns(w: Pane) -> Vec<(&'static str, Constraint)> {
    let n = |t, w| (t, Constraint::Length(w));
    let fill = |t, w| (t, Constraint::Fill(w));
    match w {
        Pane::Service => vec![
            n("TYPE", 12),
            n("CLUSTER-IP", 16),
            fill("EXTERNAL-IP", 1),
            fill("PORTS", 2),
        ],
        Pane::Endpoints => vec![n("READY", 7), fill("ENDPOINTS", 3)],
        Pane::ConfigMap => vec![n("DATA", 6), fill("KEYS", 3)],
        Pane::Secret => vec![fill("TYPE", 1), n("DATA", 6), fill("KEYS", 2)],
        Pane::Pvc => vec![
            fill("VOLUME", 2),
            n("CAPACITY", 9),
            n("ACCESS", 8),
            fill("CLASS", 1),
        ],
        Pane::Pv => vec![
            n("CAPACITY", 9),
            n("ACCESS", 8),
            n("RECLAIM", 9),
            fill("CLASS", 1),
            fill("CLAIM", 2),
        ],
        Pane::ServiceAccount => vec![n("SECRETS", 8), fill("PULL SECRETS", 1), n("AUTOMOUNT", 9)],
        Pane::Event => vec![
            n("LAST SEEN", 9),
            n("REASON", 22),
            fill("OBJECT", 1),
            n("COUNT", 6),
            fill("MESSAGE", 3),
        ],
        Pane::ResourceQuota => vec![fill("USAGE", 1)],
        Pane::LimitRange => vec![fill("TYPES", 1)],
        Pane::ReplicationController => vec![n("READY", 7), n("DESIRED", 7), fill("IMAGES", 2)],
        _ => vec![],
    }
}

pub fn status(w: Pane, o: &DynamicObject) -> String {
    let st = &o.data["status"];
    match w {
        Pane::Service => {
            if svc_type(o) == "LoadBalancer" && lb_ingress(o).is_empty() {
                "Pending".into()
            } else {
                String::new()
            }
        }
        Pane::Endpoints => {
            let all = addresses(o);
            match (all.iter().filter(|a| a.ready).count(), all.len()) {
                (_, 0) => "NoEndpoints".into(),
                (0, _) => "NotReady".into(),
                (r, t) if r == t => "Ready".into(),
                _ => "Degraded".into(),
            }
        }
        Pane::Pvc | Pane::Pv => text(&st["phase"]),
        Pane::Event => text(&o.data["type"]),
        Pane::ResourceQuota => {
            let rows = quota(o);
            if rows.is_empty() {
                String::new()
            } else if rows.iter().any(|(k, u, h)| at_limit(u, h, k)) {
                "AtLimit".into()
            } else {
                "OK".into()
            }
        }
        Pane::ReplicationController => {
            let (want, ready) = (desired(o), num(&st["readyReplicas"]));
            if want == 0 {
                "ScaledToZero".into()
            } else if ready >= want {
                "Ready".into()
            } else {
                "NotReady".into()
            }
        }
        _ => String::new(),
    }
}

pub fn cells(w: Pane, o: &DynamicObject) -> Vec<(String, Color)> {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let plain = |s: String| (s, Color::Reset);
    match w {
        Pane::Service => vec![
            plain(svc_type(o)),
            plain(text(&spec["clusterIP"])),
            plain(external(o)),
            plain(or_none(svc_ports(o).join(","))),
        ],
        Pane::Endpoints => {
            let all = addresses(o);
            let ready = all.iter().filter(|a| a.ready).count() as u64;
            let shown: Vec<String> = all.iter().map(|a| a.endpoint.clone()).collect();
            vec![
                (
                    format!("{ready}/{}", all.len()),
                    ratio(ready, all.len() as u64),
                ),
                plain(or_none(joined(&shown, 3))),
            ]
        }
        Pane::ConfigMap | Pane::Secret => {
            let mut names: BTreeSet<String> = keys(&o.data["data"]).into_iter().collect();
            names.extend(keys(&o.data["binaryData"]));
            let names: Vec<String> = names.into_iter().collect();
            let count = (names.len().to_string(), Color::Reset);
            if w == Pane::Secret {
                vec![
                    plain(text(&o.data["type"])),
                    count,
                    plain(joined(&names, 6)),
                ]
            } else {
                vec![count, plain(joined(&names, 8))]
            }
        }
        Pane::Pvc => vec![
            plain(text(&spec["volumeName"])),
            plain(text(&st["capacity"]["storage"])),
            plain(access(&spec["accessModes"])),
            plain(text(&spec["storageClassName"])),
        ],
        Pane::Pv => vec![
            plain(text(&spec["capacity"]["storage"])),
            plain(access(&spec["accessModes"])),
            plain(text(&spec["persistentVolumeReclaimPolicy"])),
            plain(text(&spec["storageClassName"])),
            plain(claim(o)),
        ],
        Pane::ServiceAccount => {
            let automount = match o.data["automountServiceAccountToken"].as_bool() {
                Some(b) => b.to_string(),
                None => "default".into(),
            };
            let pull: Vec<String> = o.data["imagePullSecrets"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|s| text(&s["name"]))
                .collect();
            vec![
                (
                    o.data["secrets"].as_array().map_or(0, Vec::len).to_string(),
                    Color::Reset,
                ),
                plain(joined(&pull, 3)),
                plain(automount),
            ]
        }
        Pane::Event => {
            let at = [
                &o.data["lastTimestamp"],
                &o.data["series"]["lastObservedTime"],
                &o.data["eventTime"],
            ]
            .into_iter()
            .find_map(seconds)
            .unwrap_or_else(|| crate::created_secs(&o.metadata));
            let count = num(&o.data["series"]["count"])
                .max(num(&o.data["count"]))
                .max(1);
            let object = format!(
                "{}/{}",
                text(&o.data["involvedObject"]["kind"]).to_lowercase(),
                text(&o.data["involvedObject"]["name"])
            );
            vec![
                plain(format_age(Timestamp::now().as_second() - at)),
                plain(text(&o.data["reason"])),
                plain(object),
                plain(count.to_string()),
                plain(text(&o.data["message"]).replace('\n', " ")),
            ]
        }
        Pane::ResourceQuota => {
            let rows = quota(o);
            let text: Vec<String> = rows
                .iter()
                .map(|(k, u, h)| format!("{k} {}/{h}", if u.is_empty() { "0" } else { u }))
                .collect();
            let hot = rows.iter().any(|(k, u, h)| at_limit(u, h, k));
            vec![(
                or_none(text.join(", ")),
                if hot { Color::Yellow } else { Color::Reset },
            )]
        }
        Pane::LimitRange => {
            let types: Vec<String> = spec["limits"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|l| text(&l["type"]))
                .collect();
            vec![plain(or_none(types.join(", ")))]
        }
        Pane::ReplicationController => {
            let (want, ready) = (desired(o), num(&st["readyReplicas"]));
            vec![
                (format!("{ready}/{want}"), ratio(ready, want)),
                (want.to_string(), Color::Reset),
                plain(images(o)),
            ]
        }
        _ => vec![],
    }
}

pub fn info_rows(w: Pane, o: &DynamicObject) -> Vec<Line<'static>> {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let mut rows: Vec<Line<'static>> = Vec::new();
    let status = status(w, o);
    if !status.is_empty() {
        rows.push(field("Status", status_span(&status)));
    }
    let cut = |mut v: Vec<Vec<Span<'static>>>| {
        if v.len() > MAX_LINES {
            let more = v.len() - MAX_LINES;
            v.truncate(MAX_LINES);
            v.push(vec![Span::styled(
                format!("… +{more} more (y shows all)"),
                DIM,
            )]);
        }
        v
    };
    let none = || vec![vec![Span::styled("<none>", DIM)]];
    match w {
        Pane::Service => {
            rows.push(field("Type", Span::raw(svc_type(o))));
            let ips = strings(&spec["clusterIPs"]);
            rows.push(field(
                "Cluster IP",
                Span::raw(if ips.is_empty() {
                    text(&spec["clusterIP"])
                } else {
                    ips.join(", ")
                }),
            ));
            rows.push(field("External", Span::raw(external(o))));
            let ports: Vec<Vec<Span>> = spec["ports"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|p| {
                    let proto = Some(text(&p["protocol"]))
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "TCP".into());
                    let target = Some(text(&p["targetPort"]))
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| num(&p["port"]).to_string());
                    let node = if num(&p["nodePort"]) > 0 {
                        format!("  nodePort {}", num(&p["nodePort"]))
                    } else {
                        String::new()
                    };
                    vec![
                        Span::styled(format!("{:<8}", text(&p["name"])), DIM),
                        Span::raw(format!("{} → {target}/{proto}{node}", num(&p["port"]))),
                    ]
                })
                .collect();
            rows.extend(lines(
                "Ports",
                if ports.is_empty() { none() } else { cut(ports) },
            ));
            let selector = label_selector(o);
            rows.push(field(
                "Selector",
                if selector.is_empty() {
                    Span::styled("<none>", DIM)
                } else {
                    Span::raw(selector)
                },
            ));
            rows.push(field("Affinity", Span::raw(text(&spec["sessionAffinity"]))));
            let policy = |k: &str| Some(text(&spec[k])).filter(|s| !s.is_empty());
            let traffic: Vec<String> = [
                ("external", "externalTrafficPolicy"),
                ("internal", "internalTrafficPolicy"),
            ]
            .iter()
            .filter_map(|(label, k)| policy(k).map(|p| format!("{label} {p}")))
            .collect();
            if !traffic.is_empty() {
                rows.push(field("Traffic", Span::raw(traffic.join(" · "))));
            }
            let families = strings(&spec["ipFamilies"]);
            if !families.is_empty() {
                rows.push(field(
                    "IP family",
                    Span::raw(format!(
                        "{} ({})",
                        families.join(","),
                        text(&spec["ipFamilyPolicy"])
                    )),
                ));
            }
        }
        Pane::Endpoints => {
            let all = addresses(o);
            let ready = all.iter().filter(|a| a.ready).count();
            rows.push(field(
                "Ready",
                Span::raw(format!("{ready} of {}", all.len())),
            ));
            let list: Vec<Vec<Span>> = all
                .iter()
                .map(|a| {
                    vec![
                        mark(a.ready),
                        Span::raw(format!(" {}  ", a.endpoint)),
                        Span::styled(a.target.clone(), DIM),
                    ]
                })
                .collect();
            rows.extend(lines(
                "Addresses",
                if list.is_empty() { none() } else { cut(list) },
            ));
        }
        Pane::ConfigMap | Pane::Secret => {
            if w == Pane::Secret {
                rows.push(field("Type", Span::raw(text(&o.data["type"]))));
            }
            if o.data["immutable"] == true {
                rows.push(field("Immutable", Span::raw("yes")));
            }
            let mut entries: Vec<(String, usize)> = Vec::new();
            for (field_name, base64) in [("data", w == Pane::Secret), ("binaryData", true)] {
                for (k, v) in o.data[field_name].as_object().into_iter().flatten() {
                    let v = v.as_str().unwrap_or_default();
                    entries.push((k.clone(), if base64 { b64_bytes(v) } else { v.len() }));
                }
            }
            entries.sort();
            let list: Vec<Vec<Span>> = entries
                .iter()
                .map(|(k, n)| vec![Span::raw(format!("{k}  ")), Span::styled(size(*n), DIM)])
                .collect();
            rows.extend(lines(
                if w == Pane::Secret { "Keys" } else { "Data" },
                if list.is_empty() { none() } else { cut(list) },
            ));
        }
        Pane::Pvc => {
            rows.push(field(
                "Volume",
                Span::raw(or_none(text(&spec["volumeName"]))),
            ));
            let (asked, got) = (
                text(&spec["resources"]["requests"]["storage"]),
                text(&st["capacity"]["storage"]),
            );
            rows.push(field(
                "Capacity",
                Span::raw(if got.is_empty() {
                    format!("requested {asked}")
                } else {
                    format!("{got} (requested {asked})")
                }),
            ));
            rows.push(field("Access", Span::raw(access(&spec["accessModes"]))));
            rows.push(field(
                "Class",
                Span::raw(or_none(text(&spec["storageClassName"]))),
            ));
            rows.push(field(
                "Mode",
                Span::raw(
                    Some(text(&spec["volumeMode"]))
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "Filesystem".into()),
                ),
            ));
            let conditions: Vec<Vec<Span>> = st["conditions"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|c| {
                    vec![
                        mark(c["status"] == "True"),
                        Span::raw(format!(" {}  ", text(&c["type"]))),
                        Span::styled(text(&c["message"]), DIM),
                    ]
                })
                .collect();
            rows.extend(lines("Conditions", conditions));
        }
        Pane::Pv => {
            rows.push(field(
                "Capacity",
                Span::raw(text(&spec["capacity"]["storage"])),
            ));
            rows.push(field("Access", Span::raw(access(&spec["accessModes"]))));
            rows.push(field(
                "Reclaim",
                Span::raw(text(&spec["persistentVolumeReclaimPolicy"])),
            ));
            rows.push(field(
                "Class",
                Span::raw(or_none(text(&spec["storageClassName"]))),
            ));
            rows.push(field("Claim", Span::raw(or_none(claim(o)))));
            rows.push(field(
                "Mode",
                Span::raw(
                    Some(text(&spec["volumeMode"]))
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| "Filesystem".into()),
                ),
            ));
            rows.push(field("Source", Span::raw(pv_source(o))));
            if !text(&st["message"]).is_empty() {
                rows.push(field(
                    "Message",
                    Span::styled(text(&st["message"]), Style::new().fg(Color::Red)),
                ));
            }
        }
        Pane::ServiceAccount => {
            let secrets: Vec<Vec<Span>> = o.data["secrets"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|s| vec![Span::raw(text(&s["name"]))])
                .collect();
            rows.extend(lines(
                "Secrets",
                if secrets.is_empty() {
                    none()
                } else {
                    cut(secrets)
                },
            ));
            let pull: Vec<Vec<Span>> = o.data["imagePullSecrets"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|s| vec![Span::raw(text(&s["name"]))])
                .collect();
            rows.extend(lines(
                "Pull",
                if pull.is_empty() { none() } else { cut(pull) },
            ));
            rows.push(field("Automount", Span::raw(cells(w, o)[2].0.clone())));
        }
        Pane::Event => {
            let obj = &o.data["involvedObject"];
            rows.push(field("Reason", Span::raw(text(&o.data["reason"]))));
            rows.push(field(
                "Object",
                Span::raw(format!(
                    "{} {}/{}",
                    text(&obj["kind"]),
                    text(&obj["namespace"]),
                    text(&obj["name"])
                )),
            ));
            // the info box does not wrap, so a long message is cut into lines here
            let message = text(&o.data["message"]).replace('\n', " ");
            let chars: Vec<char> = message.chars().collect();
            let parts: Vec<Vec<Span>> = chars
                .chunks(100)
                .map(|c| vec![Span::raw(c.iter().collect::<String>())])
                .collect();
            rows.extend(lines(
                "Message",
                if parts.is_empty() { none() } else { parts },
            ));
            rows.push(field("Count", Span::raw(cells(w, o)[3].0.clone())));
            let when = |v: &Value| {
                seconds(v).map_or(String::new(), |t| {
                    format!("{} ago", format_age(Timestamp::now().as_second() - t))
                })
            };
            rows.push(field("First", Span::raw(when(&o.data["firstTimestamp"]))));
            rows.push(field("Last", Span::raw(when(&o.data["lastTimestamp"]))));
            let source = Some(text(&o.data["source"]["component"]))
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| text(&o.data["reportingComponent"]));
            rows.push(field("Source", Span::raw(source)));
        }
        Pane::ResourceQuota => {
            let list: Vec<Vec<Span>> = quota(o)
                .iter()
                .map(|(k, u, h)| {
                    let used = if u.is_empty() { "0" } else { u };
                    let pct = match (quantity(k, used), quantity(k, h)) {
                        (Some(u), Some(h)) if h > 0 => format!(" ({}%)", u * 100 / h),
                        _ => String::new(),
                    };
                    let color = if at_limit(used, h, k) {
                        Color::Yellow
                    } else {
                        Color::Reset
                    };
                    vec![
                        Span::styled(format!("{k:<24}"), DIM),
                        Span::styled(format!("{used} / {h}{pct}"), Style::new().fg(color)),
                    ]
                })
                .collect();
            rows.extend(lines("Usage", if list.is_empty() { none() } else { list }));
        }
        Pane::LimitRange => {
            let list: Vec<Vec<Span>> = spec["limits"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|l| {
                    vec![
                        Span::styled(format!("{:<12}", text(&l["type"])), DIM),
                        Span::raw(limit_parts(l)),
                    ]
                })
                .collect();
            rows.extend(lines("Limits", if list.is_empty() { none() } else { list }));
        }
        Pane::ReplicationController => {
            let counts = [
                ("desired", desired(o)),
                ("current", num(&st["replicas"])),
                ("ready", num(&st["readyReplicas"])),
                ("available", num(&st["availableReplicas"])),
            ];
            rows.push(field(
                "Replicas",
                Span::raw(
                    counts
                        .iter()
                        .map(|(k, v)| format!("{v} {k}"))
                        .collect::<Vec<_>>()
                        .join(" · "),
                ),
            ));
            rows.push(field("Owner", Span::raw(or_none(owner(o)))));
            rows.push(field("Selector", Span::raw(label_selector(o))));
            let imgs: Vec<Vec<Span>> = crate::panes::pod_containers(o)
                .into_iter()
                .map(|(n, i)| vec![Span::styled(format!("{n}  "), DIM), Span::raw(i)])
                .collect();
            rows.extend(lines("Images", imgs));
        }
        _ => {}
    }
    rows.push(field("Created", Span::raw(created_text(&o.metadata))));
    rows.push(field(
        "UID",
        Span::raw(o.metadata.uid.clone().unwrap_or_default()),
    ));
    let _ = o.name_any(); // (ResourceExt is used by callers' `namespace()`)
    rows
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

    fn all_text(w: Pane, o: &DynamicObject) -> String {
        info_rows(w, o)
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn service_shows_ports_and_pending_load_balancers() {
        let svc = obj(
            r#"{"apiVersion": "v1", "kind": "Service", "metadata": {"name": "web", "namespace": "n"},
            "spec": {"type": "LoadBalancer", "clusterIP": "10.0.0.1", "selector": {"app": "web"},
                     "ports": [{"name": "http", "port": 80, "targetPort": 8080, "protocol": "TCP", "nodePort": 30080}, {"port": 53, "protocol": "UDP"}]}}"#,
        );
        assert_eq!(
            cell_text(Pane::Service, &svc),
            [
                "LoadBalancer",
                "10.0.0.1",
                "<pending>",
                "80:30080/TCP,53/UDP"
            ]
        );
        assert_eq!(status(Pane::Service, &svc), "Pending");
        let text = all_text(Pane::Service, &svc);
        assert!(text.contains("80 → 8080/TCP  nodePort 30080") && text.contains("app=web"));
        let up = obj(
            r#"{"apiVersion": "v1", "kind": "Service", "metadata": {"name": "w"}, "spec": {"type": "LoadBalancer"},
            "status": {"loadBalancer": {"ingress": [{"ip": "1.2.3.4"}]}}}"#,
        );
        assert_eq!(
            (
                status(Pane::Service, &up).as_str(),
                cell_text(Pane::Service, &up)[2].as_str()
            ),
            ("", "1.2.3.4")
        );
    }

    #[test]
    fn endpoints_count_ready_addresses_per_port() {
        let ep = obj(
            r#"{"apiVersion": "v1", "kind": "Endpoints", "metadata": {"name": "web"},
            "subsets": [{"addresses": [{"ip": "10.1.1.1", "targetRef": {"name": "web-1"}}], "notReadyAddresses": [{"ip": "10.1.1.2"}],
                         "ports": [{"port": 80}]}]}"#,
        );
        assert_eq!(
            cell_text(Pane::Endpoints, &ep),
            ["1/2", "10.1.1.1:80, 10.1.1.2:80"]
        );
        assert_eq!(status(Pane::Endpoints, &ep), "Degraded");
        assert_eq!(
            status(
                Pane::Endpoints,
                &obj(r#"{"apiVersion": "v1", "kind": "Endpoints", "metadata": {"name": "x"}}"#)
            ),
            "NoEndpoints"
        );
    }

    #[test]
    fn secrets_list_key_names_and_sizes_never_values() {
        let s = obj(
            r#"{"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "s"}, "type": "kubernetes.io/tls",
            "data": {"tls.crt": "c2VjcmV0LXZhbHVl", "tls.key": "YQ=="}}"#,
        );
        assert_eq!(
            cell_text(Pane::Secret, &s),
            ["kubernetes.io/tls", "2", "tls.crt, tls.key"]
        );
        let text = all_text(Pane::Secret, &s);
        assert!(
            text.contains("tls.crt  12 B")
                && !text.contains("c2VjcmV0")
                && !text.contains("secret-value")
        );
    }

    #[test]
    fn volumes_quotas_events_and_limits() {
        let pvc = obj(
            r#"{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "d"},
            "spec": {"volumeName": "pv-1", "accessModes": ["ReadWriteOnce"], "storageClassName": "fast", "resources": {"requests": {"storage": "10Gi"}}},
            "status": {"phase": "Bound", "capacity": {"storage": "10Gi"}}}"#,
        );
        assert_eq!(cell_text(Pane::Pvc, &pvc), ["pv-1", "10Gi", "RWO", "fast"]);
        assert_eq!(status(Pane::Pvc, &pvc), "Bound");

        let pv = obj(
            r#"{"apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": "pv-1"},
            "spec": {"capacity": {"storage": "10Gi"}, "accessModes": ["ReadWriteMany"], "persistentVolumeReclaimPolicy": "Retain",
                     "claimRef": {"namespace": "n", "name": "d"}, "nfs": {"server": "10.0.0.9", "path": "/exports"}}}"#,
        );
        assert_eq!(cell_text(Pane::Pv, &pv)[4], "n/d");
        assert!(all_text(Pane::Pv, &pv).contains("nfs 10.0.0.9:/exports"));

        let quota = obj(
            r#"{"apiVersion": "v1", "kind": "ResourceQuota", "metadata": {"name": "q"},
            "status": {"hard": {"pods": "10", "requests.cpu": "2"}, "used": {"pods": "10", "requests.cpu": "500m"}}}"#,
        );
        assert_eq!(status(Pane::ResourceQuota, &quota), "AtLimit");
        assert!(
            all_text(Pane::ResourceQuota, &quota).contains("requests.cpu")
                && all_text(Pane::ResourceQuota, &quota).contains("(25%)")
        );

        let event = obj(
            r#"{"apiVersion": "v1", "kind": "Event", "metadata": {"name": "e", "creationTimestamp": "2026-01-01T00:00:00Z"},
            "type": "Warning", "reason": "BackOff", "message": "Back-off restarting failed container", "count": 3,
            "involvedObject": {"kind": "Pod", "name": "web-1", "namespace": "n"}, "lastTimestamp": "2026-01-01T00:00:00Z"}"#,
        );
        let cols = cell_text(Pane::Event, &event);
        assert_eq!(
            (cols[1].as_str(), cols[2].as_str(), cols[3].as_str()),
            ("BackOff", "pod/web-1", "3")
        );
        assert_eq!(status(Pane::Event, &event), "Warning");

        let limits = obj(
            r#"{"apiVersion": "v1", "kind": "LimitRange", "metadata": {"name": "l"},
            "spec": {"limits": [{"type": "Container", "max": {"cpu": "2"}, "defaultRequest": {"cpu": "100m"}}]}}"#,
        );
        assert_eq!(cell_text(Pane::LimitRange, &limits), ["Container"]);
        assert!(
            all_text(Pane::LimitRange, &limits).contains("max cpu=2 · defaultRequest cpu=100m")
        );
    }

    #[test]
    fn every_core_kind_has_one_cell_per_column() {
        let o = obj(r#"{"apiVersion": "v1", "kind": "X", "metadata": {"name": "x"}}"#);
        for w in [
            Pane::Service,
            Pane::Endpoints,
            Pane::ConfigMap,
            Pane::Secret,
            Pane::Pvc,
            Pane::Pv,
            Pane::ServiceAccount,
            Pane::Event,
            Pane::ResourceQuota,
            Pane::LimitRange,
            Pane::ReplicationController,
        ] {
            assert_eq!(columns(w).len(), cells(w, &o).len(), "{w:?}");
            assert!(!info_rows(w, &o).is_empty(), "{w:?}");
        }
    }
}
