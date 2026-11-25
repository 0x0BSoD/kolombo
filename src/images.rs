//! Image usage: how many pods run each image.

use k8s_openapi::api::core::v1::Pod;
use std::collections::{BTreeSet, HashMap};

/// (image, pods using it), most used first. Finished pods are skipped; init containers count.
pub fn counts<'a>(pods: impl IntoIterator<Item = &'a Pod>) -> Vec<(String, usize)> {
    let mut by_image: HashMap<String, usize> = HashMap::new();
    for p in pods {
        if matches!(
            p.status.as_ref().and_then(|s| s.phase.as_deref()),
            Some("Succeeded" | "Failed")
        ) {
            continue;
        }
        let Some(spec) = &p.spec else { continue };
        let images: BTreeSet<&str> = spec
            .containers
            .iter()
            .chain(spec.init_containers.iter().flatten())
            .filter_map(|c| c.image.as_deref())
            .collect();
        for i in images {
            *by_image.entry(i.to_string()).or_default() += 1;
        }
    }
    let mut rows: Vec<_> = by_image.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pod(phase: &str, images: &[&str]) -> Pod {
        let cs: Vec<_> = images
            .iter()
            .enumerate()
            .map(|(i, img)| k8s_openapi::serde_json::json!({"name": format!("c{i}"), "image": img}))
            .collect();
        k8s_openapi::serde_json::from_value(k8s_openapi::serde_json::json!({"spec": {"containers": cs}, "status": {"phase": phase}})).unwrap()
    }

    #[test]
    fn counts_pods_per_image_and_skips_finished() {
        let pods = [
            pod("Running", &["a", "a", "b"]),
            pod("Running", &["a"]),
            pod("Succeeded", &["b"]),
        ];
        assert_eq!(
            counts(&pods),
            vec![("a".to_string(), 2), ("b".to_string(), 1)]
        );
    }
}
