//! Cordon, uncordon and drain, with the same rules as `kubectl drain`.

use futures::future::join_all;
use k8s_openapi::{
    api::core::v1::{Node, Pod},
    serde_json::json,
};
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, EvictParams, ListParams, Patch, PatchParams},
};
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(300);

/// The drain checkboxes, named after the kubectl flags they stand for.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct DrainOpts {
    pub ignore_daemonsets: bool, // --ignore-daemonsets
    pub force: bool,             // --force: also pods that no controller would recreate
    pub disable_eviction: bool, // --disable-eviction: delete pods directly, ignoring PodDisruptionBudgets
    pub delete_emptydir: bool,  // --delete-emptydir-data
}

impl Default for DrainOpts {
    fn default() -> Self {
        Self {
            ignore_daemonsets: true,
            force: false,
            disable_eviction: false,
            delete_emptydir: false,
        }
    }
}

pub async fn set_cordon(client: &Client, node: &str, on: bool) -> Result<(), kube::Error> {
    let patch = Patch::Merge(json!({ "spec": { "unschedulable": on } }));
    Api::<Node>::all(client.clone())
        .patch(node, &PatchParams::default(), &patch)
        .await
        .map(|_| ())
}

pub async fn cordon_message(client: &Client, node: &str, on: bool) -> String {
    match set_cordon(client, node, on).await {
        Ok(()) => format!("{node} {}", if on { "cordoned" } else { "uncordoned" }),
        Err(e) => format!("{} failed: {e}", if on { "cordon" } else { "uncordon" }),
    }
}

fn finished(pod: &Pod) -> bool {
    matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some("Succeeded" | "Failed")
    )
}

/// Which pods to evict, or why the drain must not start. Mirrors kubectl: static (mirror) pods and
/// pods already terminating are left alone; DaemonSet pods, pods without a controller and pods with
/// emptyDir data are refused unless the matching box is ticked.
pub fn plan(pods: &[Pod], opts: DrainOpts) -> Result<Vec<Pod>, String> {
    let (mut victims, mut refused) = (Vec::new(), Vec::new());
    for pod in pods {
        if pod.metadata.deletion_timestamp.is_some()
            || pod
                .annotations()
                .contains_key("kubernetes.io/config.mirror")
        {
            continue;
        }
        let who = format!("{}/{}", pod.namespace().unwrap_or_default(), pod.name_any());
        let controller = pod
            .owner_references()
            .iter()
            .find(|o| o.controller == Some(true));
        if controller.is_some_and(|o| o.kind == "DaemonSet") {
            if !opts.ignore_daemonsets {
                refused.push(format!("{who} (DaemonSet, tick Ignore DaemonSets)"));
            }
            continue; // a DaemonSet pod is never evicted: it would be recreated on the same node
        }
        if !finished(pod) {
            if controller.is_none() && !opts.force {
                refused.push(format!("{who} (no controller, tick Force)"));
                continue;
            }
            let has_emptydir = pod
                .spec
                .as_ref()
                .is_some_and(|s| s.volumes.iter().flatten().any(|v| v.empty_dir.is_some()));
            if has_emptydir && !opts.delete_emptydir {
                refused.push(format!("{who} (emptyDir data, tick Delete emptyDir data)"));
                continue;
            }
        }
        victims.push(pod.clone());
    }
    if refused.is_empty() {
        Ok(victims)
    } else {
        let shown = refused
            .iter()
            .take(2)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let more = if refused.len() > 2 {
            format!(" +{} more", refused.len() - 2)
        } else {
            String::new()
        };
        Err(format!("{shown}{more}"))
    }
}

fn code(e: &kube::Error) -> Option<u16> {
    match e {
        kube::Error::Api(status) => Some(status.code),
        _ => None,
    }
}

/// Evict (or delete) one pod, retrying while a PodDisruptionBudget says 429, then wait until it is gone.
async fn evict_one(client: Client, pod: Pod, opts: DrainOpts) -> Result<(), String> {
    let (ns, name) = (pod.namespace().unwrap_or_default(), pod.name_any());
    let api: Api<Pod> = Api::namespaced(client, &ns);
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let result = if opts.disable_eviction {
            api.delete(&name, &DeleteParams::default())
                .await
                .map(|_| ())
        } else {
            api.evict(&name, &EvictParams::default()).await.map(|_| ())
        };
        match result {
            Ok(()) => break,
            Err(e) if code(&e) == Some(404) => return Ok(()), // already gone
            Err(e) if code(&e) == Some(429) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_secs(5)).await
            }
            Err(e) => return Err(format!("{ns}/{name}: {e}")),
        }
    }
    while Instant::now() < deadline {
        match api.get_opt(&name).await {
            Ok(None) => return Ok(()),
            Ok(Some(p)) if p.metadata.uid != pod.metadata.uid => return Ok(()), // a replacement took the name
            Ok(Some(_)) => tokio::time::sleep(Duration::from_secs(2)).await,
            Err(e) => return Err(format!("{ns}/{name}: {e}")),
        }
    }
    Err(format!(
        "{ns}/{name}: still terminating after {} min",
        TIMEOUT.as_secs() / 60
    ))
}

/// Cordon, then evict everything `plan` allows. The returned text is for the status line.
pub async fn drain(client: Client, node: String, opts: DrainOpts) -> String {
    if let Err(e) = set_cordon(&client, &node, true).await {
        return format!("cordon failed: {e}");
    }
    let params = ListParams::default().fields(&format!("spec.nodeName={node}"));
    let pods = match Api::<Pod>::all(client.clone()).list(&params).await {
        Ok(list) => list.items,
        Err(e) => return format!("{node} cordoned, listing pods failed: {e}"),
    };
    let victims = match plan(&pods, opts) {
        Ok(v) => v,
        Err(why) => return format!("{node} cordoned, drain refused: {why}"),
    };
    let total = victims.len();
    let results = join_all(
        victims
            .into_iter()
            .map(|p| evict_one(client.clone(), p, opts)),
    )
    .await;
    let failed: Vec<String> = results.into_iter().filter_map(Result::err).collect();
    match failed.first() {
        None => format!("{node} drained ({total} pods)"),
        Some(first) => format!(
            "{node}: {} of {total} pods not evicted: {first}",
            failed.len()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(name: &str, extra: &str) -> Pod {
        serde_yaml_ng::from_str(&format!(r#"{{"metadata": {{"name": "{name}", "namespace": "ns" {extra}}}, "spec": {{"containers": []}}}}"#)).unwrap()
    }

    fn owned(name: &str, kind: &str) -> Pod {
        pod(
            name,
            &format!(
                r#", "ownerReferences": [{{"apiVersion": "v1", "kind": "{kind}", "name": "o", "uid": "u", "controller": true}}]"#
            ),
        )
    }

    #[test]
    fn drain_rules_follow_kubectl() {
        let opts = DrainOpts::default();
        let mirror = pod(
            "static",
            r#", "annotations": {"kubernetes.io/config.mirror": "x"}"#,
        );
        let plain = pod("bare", "");
        let ds = owned("ds", "DaemonSet");
        let web = owned("web", "ReplicaSet");

        let names = |v: Vec<Pod>| v.iter().map(|p| p.name_any()).collect::<Vec<_>>();
        // DaemonSet and static pods are skipped; a bare pod blocks the drain until Force
        let err = plan(
            &[mirror.clone(), ds.clone(), web.clone(), plain.clone()],
            opts,
        )
        .unwrap_err();
        assert!(err.contains("ns/bare") && err.contains("Force"));
        assert_eq!(
            names(
                plan(
                    &[mirror, ds.clone(), web.clone(), plain.clone()],
                    DrainOpts {
                        force: true,
                        ..opts
                    }
                )
                .unwrap()
            ),
            ["web", "bare"]
        );
        assert!(
            plan(
                &[ds.clone()],
                DrainOpts {
                    ignore_daemonsets: false,
                    ..opts
                }
            )
            .unwrap_err()
            .contains("DaemonSet")
        );

        let mut scratch = owned("scratch", "ReplicaSet");
        scratch.spec.as_mut().unwrap().volumes = Some(vec![k8s_openapi::api::core::v1::Volume {
            name: "v".into(),
            empty_dir: Some(Default::default()),
            ..Default::default()
        }]);
        assert!(
            plan(&[scratch.clone()], opts)
                .unwrap_err()
                .contains("emptyDir")
        );
        assert_eq!(
            plan(
                &[scratch],
                DrainOpts {
                    delete_emptydir: true,
                    ..opts
                }
            )
            .unwrap()
            .len(),
            1
        );
    }
}
