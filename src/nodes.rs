//! Node-specific reading of untyped objects: roles, status, addresses, allocatable resources.

use crate::{Usage, cpu_text, field, mem_text, parse_cpu, parse_mem, resources::ResourceInfo};
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::reflector::Store;
use kube::{
    Api, Client, ResourceExt,
    api::ListParams,
    core::{ApiResource, DynamicObject, GroupVersionKind},
};
use ratatui::{
    style::{Color, Style},
    text::{Line, Span},
};
use std::collections::HashMap;

/// What the node list shows beyond name, status and age.
pub struct NodeCols {
    pub roles: String,
    pub version: String,
    pub ip: String,
    pub os: String,
    pub cpu_alloc: Option<u64>,  // millicores
    pub mem_alloc: Option<u64>,  // bytes
    pub pods: usize,             // pods running or pending on the node
    pub pods_alloc: Option<u64>, // how many it may hold
}

pub fn is_nodes(info: &ResourceInfo) -> bool {
    info.group.is_empty() && info.name == "nodes"
}

/// `node-role.kubernetes.io/control-plane` -> `control-plane`, comma separated like kubectl.
pub fn roles(o: &DynamicObject) -> String {
    let labels = o.labels();
    let mut roles: Vec<&str> = labels
        .keys()
        .filter_map(|k| k.strip_prefix("node-role.kubernetes.io/"))
        .collect();
    if let Some(r) = labels.get("kubernetes.io/role") {
        roles.push(r);
    }
    roles.sort();
    roles.dedup();
    if roles.is_empty() {
        "<none>".into()
    } else {
        roles.join(",")
    }
}

/// `Ready` / `NotReady`, plus `,SchedulingDisabled` for a cordoned node (kubectl's wording).
pub fn status(o: &DynamicObject) -> String {
    let ready = o.data["status"]["conditions"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|c| c["type"].as_str() == Some("Ready"))
        .is_some_and(|c| c["status"].as_str() == Some("True"));
    let mut s = if ready {
        "Ready".to_string()
    } else {
        "NotReady".to_string()
    };
    if o.data["spec"]["unschedulable"].as_bool() == Some(true) {
        s.push_str(",SchedulingDisabled");
    }
    s
}

fn address(o: &DynamicObject, kind: &str) -> String {
    o.data["status"]["addresses"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|a| a["type"].as_str() == Some(kind))
        .and_then(|a| a["address"].as_str())
        .unwrap_or_default()
        .to_string()
}

fn text(o: &DynamicObject, path: &[&str]) -> String {
    let mut v = &o.data["status"];
    for p in path {
        v = &v[*p];
    }
    v.as_str().unwrap_or_default().to_string()
}

pub fn cols(o: &DynamicObject, pods: usize) -> NodeCols {
    let alloc = &o.data["status"]["allocatable"];
    NodeCols {
        roles: roles(o),
        version: text(o, &["nodeInfo", "kubeletVersion"]),
        ip: address(o, "InternalIP"),
        os: text(o, &["nodeInfo", "osImage"]),
        cpu_alloc: alloc["cpu"].as_str().and_then(parse_cpu),
        mem_alloc: alloc["memory"].as_str().and_then(parse_mem),
        pods,
        pods_alloc: alloc["pods"].as_str().and_then(|p| p.parse().ok()),
    }
}

/// Pods per node name. Finished pods (Succeeded/Failed) no longer use a slot, so they are not counted.
pub fn pod_counts(pods: &Store<Pod>) -> HashMap<String, usize> {
    let mut counts = HashMap::new();
    for p in pods.state() {
        let done = matches!(
            p.status.as_ref().and_then(|s| s.phase.as_deref()),
            Some("Succeeded" | "Failed")
        );
        if let (Some(node), false) = (p.spec.as_ref().and_then(|s| s.node_name.as_ref()), done) {
            *counts.entry(node.clone()).or_default() += 1;
        }
    }
    counts
}

/// NodeMetrics from metrics-server: node name -> current usage. Errors mean "not available".
pub async fn fetch_usage(client: &Client) -> Result<HashMap<String, Usage>, kube::Error> {
    let gvk = GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "NodeMetrics");
    let api: Api<DynamicObject> = Api::all_with(
        client.clone(),
        &ApiResource::from_gvk_with_plural(&gvk, "nodes"),
    );
    Ok(api
        .list(&ListParams::default())
        .await?
        .items
        .iter()
        .map(|o| {
            let cpu = o.data["usage"]["cpu"]
                .as_str()
                .and_then(parse_cpu)
                .unwrap_or(0);
            let mem = o.data["usage"]["memory"]
                .as_str()
                .and_then(parse_mem)
                .unwrap_or(0);
            (o.name_any(), Usage { cpu, mem })
        })
        .collect())
}

/// One resource bar of the node details: used of allocatable.
pub struct Bar {
    pub label: &'static str,
    pub detail: String,
    pub ratio: f64,
}

/// What the node details show besides the info rows: resource bars and conditions.
pub struct NodeExtra {
    pub bars: Vec<Bar>,
    pub conditions: Vec<crate::CondRow>,
}

/// Bars (live usage included when metrics-server answered) and conditions for the details view.
pub fn extra(o: &DynamicObject, usage: Option<&Usage>, pods: usize) -> NodeExtra {
    let c = cols(o, pods);
    let ratio = |used: Option<u64>, alloc: Option<u64>| match (used, alloc) {
        (Some(u), Some(a)) if a > 0 => u as f64 / a as f64,
        _ => 0.0,
    };
    let bars = vec![
        Bar {
            label: "CPU",
            detail: format!(
                "{}/{}",
                cpu_text(usage.map(|u| u.cpu)),
                cpu_text(c.cpu_alloc)
            ),
            ratio: ratio(usage.map(|u| u.cpu), c.cpu_alloc),
        },
        Bar {
            label: "Memory",
            detail: format!(
                "{}/{}",
                mem_text(usage.map(|u| u.mem)),
                mem_text(c.mem_alloc)
            ),
            ratio: ratio(usage.map(|u| u.mem), c.mem_alloc),
        },
        Bar {
            label: "Pods",
            detail: format!(
                "{}/{}",
                c.pods,
                c.pods_alloc.map_or("-".into(), |a| a.to_string())
            ),
            ratio: ratio(Some(c.pods as u64), c.pods_alloc),
        },
    ];
    let conditions = o.data["status"]["conditions"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| {
            let kind = c["type"].as_str().unwrap_or_default();
            // Ready is good when True; the pressure conditions are good when False.
            let ok = (c["status"].as_str() == Some("True")) == (kind == "Ready");
            let reason = [c["reason"].as_str(), c["message"].as_str()]
                .into_iter()
                .flatten()
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(": ");
            let age = c["lastTransitionTime"]
                .as_str()
                .and_then(|t| t.parse::<k8s_openapi::jiff::Timestamp>().ok())
                .map_or(String::new(), |t| {
                    crate::format_age(
                        k8s_openapi::jiff::Timestamp::now().as_second() - t.as_second(),
                    )
                });
            crate::CondRow {
                kind: kind.to_string(),
                ok,
                age,
                reason,
            }
        })
        .collect();
    NodeExtra { bars, conditions }
}

/// Rows of the Info box.
pub fn info_rows(o: &DynamicObject) -> Vec<Line<'static>> {
    let dim = Style::new().fg(Color::DarkGray);
    let c = cols(o, 0);
    let taints: Vec<String> = o.data["spec"]["taints"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|t| {
            let value = t["value"]
                .as_str()
                .filter(|v| !v.is_empty())
                .map_or(String::new(), |v| format!("={v}"));
            format!(
                "{}{value}:{}",
                t["key"].as_str().unwrap_or_default(),
                t["effect"].as_str().unwrap_or_default()
            )
        })
        .collect();
    vec![
        field("Status", crate::status_span(&status(o))),
        field("Roles", Span::raw(c.roles)),
        field("Kubelet", Span::raw(c.version)),
        field("OS", Span::raw(c.os)),
        field("Kernel", Span::raw(text(o, &["nodeInfo", "kernelVersion"]))),
        field(
            "Runtime",
            Span::raw(text(o, &["nodeInfo", "containerRuntimeVersion"])),
        ),
        field(
            "IP",
            Span::raw(format!("{} ({})", c.ip, address(o, "Hostname"))),
        ),
        field(
            "Taints",
            if taints.is_empty() {
                Span::styled("<none>", dim)
            } else {
                Span::raw(taints.join(", "))
            },
        ),
        field("Created", Span::raw(crate::created_text(&o.metadata))),
        field("UID", Span::raw(o.metadata.uid.clone().unwrap_or_default())),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node() -> DynamicObject {
        serde_yaml_ng::from_str(
            r#"{
            "apiVersion": "v1", "kind": "Node",
            "metadata": {"name": "n1", "labels": {"node-role.kubernetes.io/control-plane": "", "node-role.kubernetes.io/etcd": "", "x": "y"}},
            "spec": {"unschedulable": true},
            "status": {
                "conditions": [{"type": "MemoryPressure", "status": "False"}, {"type": "Ready", "status": "True"}],
                "addresses": [{"type": "Hostname", "address": "n1.local"}, {"type": "InternalIP", "address": "10.0.0.5"}],
                "allocatable": {"cpu": "3920m", "memory": "7900Mi", "pods": "110"},
                "nodeInfo": {"kubeletVersion": "v1.30.2", "osImage": "Ubuntu 24.04 LTS"}
            }
        }"#,
        )
        .unwrap()
    }

    #[test]
    fn reads_roles_status_addresses_and_allocatable() {
        let n = node();
        let c = cols(&n, 12);
        assert_eq!(c.roles, "control-plane,etcd");
        assert_eq!(status(&n), "Ready,SchedulingDisabled");
        assert_eq!(
            (c.ip.as_str(), c.version.as_str(), c.os.as_str()),
            ("10.0.0.5", "v1.30.2", "Ubuntu 24.04 LTS")
        );
        assert_eq!((c.cpu_alloc, c.mem_alloc), (Some(3920), Some(7900 << 20)));
        let x = extra(
            &n,
            Some(&Usage {
                cpu: 392,
                mem: 790 << 20,
            }),
            12,
        );
        assert!((x.bars[0].ratio - 0.1).abs() < 0.001);
        assert_eq!(x.bars[2].detail, "12/110");
        assert_eq!(
            x.conditions.iter().map(|c| c.ok).collect::<Vec<_>>(),
            vec![true, true]
        ); // no memory pressure, Ready
        assert!(
            info_rows(&n)
                .iter()
                .any(|l| l.to_string().contains("10.0.0.5"))
        );
        assert_eq!((c.pods, c.pods_alloc), (12, Some(110)));
    }
}
