//! batch/v1: Jobs and CronJobs.

use crate::panes::{Pane, images, lines, mark, num, owner, pod_containers, ratio, text};
use crate::{created_text, field, format_age, status_span};
use k8s_openapi::{jiff, serde_json::Value};
use kube::core::DynamicObject;
use ratatui::{
    layout::Constraint,
    style::{Color, Style},
    text::{Line, Span},
};

fn arr(v: &Value) -> impl Iterator<Item = &Value> {
    v.as_array().into_iter().flatten()
}

fn secs(v: &Value) -> Option<i64> {
    v.as_str()?
        .parse::<jiff::Timestamp>()
        .ok()
        .map(|t| t.as_second())
}

fn now() -> i64 {
    jiff::Timestamp::now().as_second()
}

/// Seconds the job ran: start to completion, or start to now while it still runs.
fn duration(o: &DynamicObject) -> Option<i64> {
    let st = &o.data["status"];
    let start = secs(&st["startTime"])?;
    Some(
        secs(&st["completionTime"])
            .or_else(|| secs(&st["failureTime"]))
            .unwrap_or_else(now)
            - start,
    )
}

fn has_condition(o: &DynamicObject, kind: &str) -> bool {
    arr(&o.data["status"]["conditions"])
        .any(|c| c["type"].as_str() == Some(kind) && c["status"] == "True")
}

fn completions(o: &DynamicObject) -> Option<u64> {
    o.data["spec"]["completions"].as_u64()
}

fn cron_images(o: &DynamicObject) -> Vec<(String, String)> {
    arr(&o.data["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"])
        .map(|c| (text(&c["name"]), text(&c["image"])))
        .collect()
}

fn cron_image_text(o: &DynamicObject) -> String {
    let all = cron_images(o);
    match all.split_first() {
        None => String::new(),
        Some(((_, first), rest)) if rest.is_empty() => crate::short_image(first),
        Some(((_, first), rest)) => format!("{} +{}", crate::short_image(first), rest.len()),
    }
}

fn ago(v: &Value) -> String {
    secs(v).map_or("<none>".into(), |t| format_age(now() - t))
}

pub fn columns(w: Pane) -> Vec<(&'static str, Constraint)> {
    let n = |t, w| (t, Constraint::Length(w));
    let images = ("IMAGES", Constraint::Fill(2));
    match w {
        Pane::Job => vec![
            n("COMPLETIONS", 11),
            n("DURATION", 8),
            n("ACTIVE", 6),
            n("FAILED", 6),
            n("OWNER", 28),
            images,
        ],
        Pane::CronJob => vec![
            n("SCHEDULE", 16),
            n("SUSPEND", 7),
            n("ACTIVE", 6),
            n("LAST SCHEDULE", 13),
            images,
        ],
        _ => vec![],
    }
}

pub fn status(w: Pane, o: &DynamicObject) -> String {
    match w {
        Pane::Job => {
            if has_condition(o, "Failed") {
                "Failed".into()
            } else if has_condition(o, "Complete") || has_condition(o, "SuccessCriteriaMet") {
                "Complete".into()
            } else if o.data["spec"]["suspend"] == true {
                "Suspended".into()
            } else if num(&o.data["status"]["active"]) > 0 {
                "Running".into()
            } else {
                "Pending".into()
            }
        }
        Pane::CronJob if o.data["spec"]["suspend"] == true => "Suspended".into(),
        _ => String::new(),
    }
}

pub fn cells(w: Pane, o: &DynamicObject) -> Vec<(String, Color)> {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let plain = |t: String| (t, Color::Reset);
    match w {
        Pane::Job => {
            let done = num(&st["succeeded"]);
            let (text, color) = match completions(o) {
                Some(want) => (format!("{done}/{want}"), ratio(done, want)),
                None => (format!("{done}/-"), Color::Reset), // a work-queue job decides for itself
            };
            let failed = num(&st["failed"]);
            vec![
                (text, color),
                plain(duration(o).map_or(String::new(), format_age)),
                plain(num(&st["active"]).to_string()),
                (
                    failed.to_string(),
                    if failed > 0 { Color::Red } else { Color::Reset },
                ),
                plain(owner(o)),
                plain(images(o)),
            ]
        }
        Pane::CronJob => vec![
            plain(text(&spec["schedule"])),
            (
                if spec["suspend"] == true { "yes" } else { "no" }.into(),
                if spec["suspend"] == true {
                    Color::Yellow
                } else {
                    Color::Reset
                },
            ),
            plain(arr(&st["active"]).count().to_string()),
            plain(secs(&st["lastScheduleTime"]).map_or(String::new(), |t| format_age(now() - t))),
            plain(cron_image_text(o)),
        ],
        _ => vec![],
    }
}

pub fn info_rows(w: Pane, o: &DynamicObject) -> Vec<Line<'static>> {
    let (spec, st) = (&o.data["spec"], &o.data["status"]);
    let dim = Style::new().fg(Color::DarkGray);
    let mut rows = Vec::new();
    let status = status(w, o);
    if !status.is_empty() {
        rows.push(field("Status", status_span(&status)));
    }
    let counts = |pairs: &[(&str, u64)]| {
        pairs
            .iter()
            .map(|(k, v)| format!("{v} {k}"))
            .collect::<Vec<_>>()
            .join(" · ")
    };
    let image_lines = |list: Vec<(String, String)>| -> Vec<Line<'static>> {
        lines(
            "Images",
            list.into_iter()
                .map(|(n, i)| vec![Span::styled(format!("{n}  "), dim), Span::raw(i)])
                .collect(),
        )
    };
    match w {
        Pane::Job => {
            let want = completions(o).map_or("-".to_string(), |c| c.to_string());
            rows.push(field(
                "Completions",
                Span::raw(format!("{} of {want}", num(&st["succeeded"]))),
            ));
            rows.push(field(
                "Pods",
                Span::raw(counts(&[
                    ("active", num(&st["active"])),
                    ("succeeded", num(&st["succeeded"])),
                    ("failed", num(&st["failed"])),
                    ("ready", num(&st["ready"])),
                ])),
            ));
            rows.push(field(
                "Limits",
                Span::raw(format!(
                    "parallelism {} · backoff limit {}{}{}",
                    spec["parallelism"].as_u64().unwrap_or(1),
                    spec["backoffLimit"].as_u64().unwrap_or(6),
                    spec["activeDeadlineSeconds"]
                        .as_u64()
                        .map(|d| format!(" · deadline {d}s"))
                        .unwrap_or_default(),
                    spec["ttlSecondsAfterFinished"]
                        .as_u64()
                        .map(|d| format!(" · deleted {d}s after finishing"))
                        .unwrap_or_default(),
                )),
            ));
            rows.push(field("Started", Span::raw(ago(&st["startTime"]) + " ago")));
            rows.push(field(
                "Duration",
                Span::raw(duration(o).map_or("-".into(), format_age)),
            ));
            rows.push(field("Owner", Span::raw(owner(o))));
            rows.extend(image_lines(pod_containers(o)));
            let conds: Vec<Vec<Span>> = arr(&st["conditions"])
                .map(|c| {
                    vec![
                        mark(c["status"] == "True"),
                        Span::raw(format!(" {}  ", text(&c["type"]))),
                        Span::styled(text(&c["reason"]), dim),
                    ]
                })
                .collect();
            rows.extend(lines("Conditions", conds));
        }
        Pane::CronJob => {
            let zone = spec["timeZone"]
                .as_str()
                .map(|z| format!(" ({z})"))
                .unwrap_or_default();
            rows.push(field(
                "Schedule",
                Span::raw(format!("{}{zone}", text(&spec["schedule"]))),
            ));
            rows.push(field(
                "Suspend",
                Span::raw(if spec["suspend"] == true { "yes" } else { "no" }),
            ));
            rows.push(field(
                "Policy",
                Span::raw(format!(
                    "concurrency {} · keeps {} successful, {} failed jobs{}",
                    spec["concurrencyPolicy"].as_str().unwrap_or("Allow"),
                    spec["successfulJobsHistoryLimit"].as_u64().unwrap_or(3),
                    spec["failedJobsHistoryLimit"].as_u64().unwrap_or(1),
                    spec["startingDeadlineSeconds"]
                        .as_u64()
                        .map(|d| format!(" · start deadline {d}s"))
                        .unwrap_or_default(),
                )),
            ));
            rows.push(field(
                "Last run",
                Span::raw(format!(
                    "scheduled {} ago · succeeded {} ago",
                    ago(&st["lastScheduleTime"]),
                    ago(&st["lastSuccessfulTime"])
                )),
            ));
            let active: Vec<Vec<Span>> = arr(&st["active"])
                .map(|a| vec![Span::raw(text(&a["name"]))])
                .collect();
            rows.extend(lines(
                "Active",
                if active.is_empty() {
                    vec![vec![Span::styled("<none>", dim)]]
                } else {
                    active
                },
            ));
            rows.extend(image_lines(cron_images(o)));
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

    fn cell_text(w: Pane, o: &DynamicObject) -> Vec<String> {
        cells(w, o).into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn every_kind_has_one_cell_per_column() {
        let o = obj(r#"{"apiVersion": "batch/v1", "kind": "X", "metadata": {"name": "x"}}"#);
        for w in [Pane::Job, Pane::CronJob] {
            assert_eq!(columns(w).len(), cells(w, &o).len(), "{w:?}");
            assert_eq!(
                crate::panes::columns(w).len(),
                crate::panes::cells(w, &o).len(),
                "{w:?}"
            ); // via the dispatcher
            assert!(!info_rows(w, &o).is_empty());
        }
    }

    #[test]
    fn job_status_and_row() {
        let done = obj(
            r#"{"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j", "ownerReferences": [{"apiVersion": "batch/v1", "kind": "CronJob", "name": "nightly", "uid": "u"}]},
            "spec": {"completions": 3, "template": {"spec": {"containers": [{"name": "c", "image": "busybox:1"}]}}},
            "status": {"succeeded": 3, "startTime": "2026-01-01T00:00:00Z", "completionTime": "2026-01-01T00:02:05Z", "conditions": [{"type": "Complete", "status": "True"}]}}"#,
        );
        assert_eq!(status(Pane::Job, &done), "Complete");
        assert_eq!(
            cell_text(Pane::Job, &done),
            ["3/3", "2m", "0", "0", "cronjob/nightly", "busybox:1"]
        );
        let failing = obj(
            r#"{"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j"}, "status": {"failed": 2, "conditions": [{"type": "Failed", "status": "True"}]}}"#,
        );
        assert_eq!(status(Pane::Job, &failing), "Failed");
        let running = obj(
            r#"{"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "j"}, "status": {"active": 1}}"#,
        );
        assert_eq!(status(Pane::Job, &running), "Running");
        assert_eq!(cell_text(Pane::Job, &running)[0], "0/-");
    }

    #[test]
    fn cronjob_row_and_jobs_hop() {
        let cj = obj(
            r#"{"apiVersion": "batch/v1", "kind": "CronJob", "metadata": {"name": "nightly", "namespace": "ops", "uid": "cj-1"},
            "spec": {"schedule": "0 2 * * *", "suspend": true, "jobTemplate": {"spec": {"template": {"spec": {"containers": [{"name": "c", "image": "reg.io/ops/backup:2"}]}}}}},
            "status": {"active": [{"name": "nightly-1"}]}}"#,
        );
        assert_eq!(
            cell_text(Pane::CronJob, &cj),
            ["0 2 * * *", "yes", "1", "", "ops/backup:2"]
        );
        assert_eq!(status(Pane::CronJob, &cj), "Suspended");
        let Some(crate::net_panes::Hop::Open(t)) = crate::net_panes::enter(Pane::CronJob, &cj)
        else {
            panic!("expected Open")
        };
        assert_eq!((t[0].group, t[0].plural), ("batch", "jobs"));
        let job = |uid: &str| {
            obj(&format!(
                r#"{{"apiVersion": "batch/v1", "kind": "Job", "metadata": {{"name": "j", "ownerReferences": [{{"apiVersion": "batch/v1", "kind": "CronJob", "name": "nightly", "uid": "{uid}"}}]}}}}"#
            ))
        };
        assert!((t[0].keep)(&job("cj-1")) && !(t[0].keep)(&job("other")));
    }
}
