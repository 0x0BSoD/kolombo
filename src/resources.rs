//! The cluster's resource types (what `kubectl api-resources` lists) and name/alias lookup.

use fuzzy_matcher::{FuzzyMatcher, skim::SkimMatcherV2};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::APIResourceList;
use kube::Client;

#[derive(Clone, Debug, PartialEq)]
pub struct ResourceInfo {
    pub name: String, // plural, e.g. "pods"
    pub singular: String,
    pub short_names: Vec<String>, // e.g. ["po"]
    pub kind: String,
    pub group: String, // "" for the core group
    pub version: String,
}

/// What the app can open today.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Target {
    Namespaces,
    Pods,
}

impl ResourceInfo {
    pub fn target(&self) -> Option<Target> {
        match (self.group.as_str(), self.name.as_str()) {
            ("", "pods") => Some(Target::Pods),
            ("", "namespaces") => Some(Target::Namespaces),
            _ => None,
        }
    }

    pub fn aliases(&self) -> String {
        self.short_names.join(", ")
    }
}

/// Used until discovery has answered (or when it fails), so `:po` and `:ns` always work.
pub fn builtin() -> Vec<ResourceInfo> {
    let info = |name: &str, singular: &str, short: &str, kind: &str| ResourceInfo {
        name: name.into(),
        singular: singular.into(),
        short_names: vec![short.into()],
        kind: kind.into(),
        group: String::new(),
        version: "v1".into(),
    };
    vec![info("namespaces", "namespace", "ns", "Namespace"), info("pods", "pod", "po", "Pod")]
}

fn collect(list: APIResourceList, out: &mut Vec<ResourceInfo>) {
    let group = list.group_version.split_once('/').map_or("", |(g, _)| g).to_string();
    let version = list.group_version.rsplit('/').next().unwrap_or_default().to_string();
    for r in list.resources {
        // "pods/log" and friends are subresources; "watch" and "list" are what a list view needs.
        if r.name.contains('/') || !r.verbs.iter().any(|v| v == "list") {
            continue;
        }
        out.push(ResourceInfo {
            name: r.name,
            singular: r.singular_name,
            short_names: r.short_names.unwrap_or_default(),
            kind: r.kind,
            group: group.clone(),
            version: version.clone(),
        });
    }
}

/// Core group plus the preferred version of every API group, fetched concurrently.
pub async fn discover(client: &Client) -> Vec<ResourceInfo> {
    let mut out = Vec::new();
    if let Ok(core) = client.list_core_api_resources("v1").await {
        collect(core, &mut out);
    }
    if let Ok(groups) = client.list_api_groups().await {
        let versions: Vec<String> = groups
            .groups
            .into_iter()
            .filter_map(|g| g.preferred_version.or_else(|| g.versions.into_iter().next()))
            .map(|v| v.group_version)
            .collect();
        let lists = futures::future::join_all(versions.iter().map(|v| client.list_api_group_resources(v))).await;
        for list in lists.into_iter().flatten() {
            collect(list, &mut out);
        }
    }
    out
}

/// Match strength of `query` against one resource; None = not a match.
/// Exact name/alias/singular/kind beats a prefix, which beats a fuzzy match.
fn score(info: &ResourceInfo, query: &str, matcher: &SkimMatcherV2) -> Option<i64> {
    if query.is_empty() {
        return Some(0);
    }
    let kind = info.kind.to_lowercase();
    let names = || [info.name.as_str(), info.singular.as_str(), kind.as_str()].into_iter().chain(info.short_names.iter().map(String::as_str));
    if names().any(|n| n == query) {
        Some(3000)
    } else if names().any(|n| n.starts_with(query)) {
        Some(2000 - info.name.len() as i64)
    } else {
        matcher.fuzzy_match(&info.name, query).map(|s| s.min(1500))
    }
}

/// Best matches first; the core group wins ties (`pods` is also a metrics.k8s.io resource).
pub fn matches(all: &[ResourceInfo], query: &str, matcher: &SkimMatcherV2) -> Vec<ResourceInfo> {
    let query = query.trim().to_lowercase();
    let mut scored: Vec<(i64, &ResourceInfo)> = all.iter().filter_map(|r| Some((score(r, &query, matcher)?, r))).collect();
    scored.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| a.1.group.is_empty().cmp(&b.1.group.is_empty()).reverse())
            .then_with(|| a.1.name.cmp(&b.1.name))
            .then_with(|| a.1.group.cmp(&b.1.group))
    });
    scored.into_iter().map(|(_, r)| r.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn res(name: &str, singular: &str, short: &[&str], kind: &str, group: &str) -> ResourceInfo {
        ResourceInfo {
            name: name.into(),
            singular: singular.into(),
            short_names: short.iter().map(|s| s.to_string()).collect(),
            kind: kind.into(),
            group: group.into(),
            version: "v1".into(),
        }
    }

    #[test]
    fn resolves_names_aliases_singulars_and_kinds() {
        let all = vec![
            res("pods", "", &[], "PodMetrics", "metrics.k8s.io"),
            res("services", "", &["svc"], "Service", ""),
            res("pods", "pod", &["po"], "Pod", ""),
            res("deployments", "", &["deploy"], "Deployment", "apps"),
        ];
        let m = SkimMatcherV2::default();
        let first = |q: &str| matches(&all, q, &m).first().map(|r| (r.name.clone(), r.group.clone()));
        assert_eq!(first("po"), Some(("pods".into(), "".into()))); // alias, core group before metrics
        assert_eq!(first("pods"), Some(("pods".into(), "".into())));
        assert_eq!(first("svc"), Some(("services".into(), "".into())));
        assert_eq!(first("Service"), Some(("services".into(), "".into()))); // kind, any case
        assert_eq!(first("dep"), Some(("deployments".into(), "apps".into()))); // prefix
        assert_eq!(first("dpl"), Some(("deployments".into(), "apps".into()))); // fuzzy
        assert_eq!(first("zzz"), None);
        assert_eq!(matches(&all, "", &m).len(), 4);
    }

    #[test]
    fn only_core_pods_and_namespaces_can_be_opened() {
        let b = builtin();
        assert_eq!(b[0].target(), Some(Target::Namespaces));
        assert_eq!(b[1].target(), Some(Target::Pods));
        assert_eq!(res("pods", "", &[], "PodMetrics", "metrics.k8s.io").target(), None);
    }
}
