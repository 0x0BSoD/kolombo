//! Full-screen view of one container: everything from `spec.containers[x]` plus its status.

use crate::{
    ContainerRow, HISTORY, POLL_SECS, Usage, container_rows, cpu_text, field, mem_text, parse_cpu,
    parse_mem,
};
use k8s_openapi::{
    api::core::v1::{Container, ContainerStatus, Pod, Probe},
    apimachinery::pkg::util::intstr::IntOrString,
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols::Marker,
    text::{Line, Span},
    widgets::{
        Axis, Block, Cell, Chart, Dataset, GraphType, Paragraph, Row, Table, TableState, Wrap,
    },
};

/// Everything the pane draws, already turned into owned strings.
pub struct ContainerDetails {
    overview: Vec<Line<'static>>,
    resources: Vec<[String; 3]>, // resource, request, limit
    command: Vec<(&'static str, String)>,
    env: Vec<(String, String, Color)>, // name, value or source, color
    ports: Vec<[String; 3]>,
    mounts: Vec<[String; 3]>,
    probes: Vec<Line<'static>>,
    cpu: (Option<u64>, Option<u64>), // request, limit in millicores
    mem: (Option<u64>, Option<u64>), // request, limit in bytes
    pub yaml: serde_yaml_ng::Value,  // the raw container spec, for the `y` pane
}

fn shell_quote(a: &str) -> String {
    let plain = !a.is_empty()
        && a.chars()
            .all(|c| c.is_ascii_alphanumeric() || "_@%+=:,./-".contains(c));
    if plain {
        a.to_string()
    } else {
        format!("'{}'", a.replace('\'', "'\\''"))
    }
}

fn shell_join(v: &[String]) -> String {
    v.iter()
        .map(|a| shell_quote(a))
        .collect::<Vec<_>>()
        .join(" ")
}

fn port_text(p: &IntOrString) -> String {
    match p {
        IntOrString::Int(i) => i.to_string(),
        IntOrString::String(s) => s.clone(),
    }
}

fn probe_line(label: &str, probe: &Option<Probe>) -> Line<'static> {
    let Some(p) = probe else {
        return field(
            label,
            Span::styled("<none>", Style::new().fg(Color::DarkGray)),
        );
    };
    let action = if let Some(h) = &p.http_get {
        let scheme = h.scheme.as_deref().unwrap_or("HTTP").to_lowercase();
        format!(
            "http GET {scheme}://:{}{}",
            port_text(&h.port),
            h.path.as_deref().unwrap_or("/")
        )
    } else if let Some(t) = &p.tcp_socket {
        format!("tcp :{}", port_text(&t.port))
    } else if let Some(e) = &p.exec {
        format!(
            "exec {}",
            shell_join(e.command.as_deref().unwrap_or_default())
        )
    } else if let Some(g) = &p.grpc {
        format!("grpc :{}", g.port)
    } else {
        "?".into()
    };
    // The API server fills these defaults in, so a missing value means the default.
    let timing = format!(
        "  delay {}s · every {}s · timeout {}s · ok ≥{} · fail ≥{}",
        p.initial_delay_seconds.unwrap_or(0),
        p.period_seconds.unwrap_or(10),
        p.timeout_seconds.unwrap_or(1),
        p.success_threshold.unwrap_or(1),
        p.failure_threshold.unwrap_or(3),
    );
    let mut line = field(label, Span::raw(action));
    line.spans
        .push(Span::styled(timing, Style::new().fg(Color::DarkGray)));
    line
}

/// Env values come from the spec only; secret *values* are never fetched, just their source.
fn env_rows(c: &Container) -> Vec<(String, String, Color)> {
    let source = Color::Magenta;
    let mut rows = Vec::new();
    for from in c.env_from.iter().flatten() {
        let prefix = from.prefix.as_deref().unwrap_or("");
        if let Some(r) = &from.config_map_ref {
            rows.push((
                format!("{prefix}*"),
                format!("configmap {}", r.name),
                source,
            ));
        }
        if let Some(r) = &from.secret_ref {
            rows.push((format!("{prefix}*"), format!("secret {}", r.name), source));
        }
    }
    for e in c.env.iter().flatten() {
        let (value, color) = match (&e.value, &e.value_from) {
            (Some(v), _) => (v.clone(), Color::Reset),
            (None, Some(f)) => {
                let text = if let Some(r) = &f.secret_key_ref {
                    format!("secret {}/{}", r.name, r.key)
                } else if let Some(r) = &f.config_map_key_ref {
                    format!("configmap {}/{}", r.name, r.key)
                } else if let Some(r) = &f.field_ref {
                    format!("field {}", r.field_path)
                } else if let Some(r) = &f.resource_field_ref {
                    format!("resource {}", r.resource)
                } else {
                    "?".into()
                };
                (text, source)
            }
            (None, None) => (String::new(), Color::Reset),
        };
        rows.push((e.name.clone(), value, color));
    }
    rows
}

fn security_text(c: &Container) -> (String, bool) {
    let Some(s) = &c.security_context else {
        return (String::new(), false);
    };
    let mut parts = Vec::new();
    if let Some(u) = s.run_as_user {
        parts.push(format!("user {u}"));
    }
    if s.run_as_non_root == Some(true) {
        parts.push("non-root".into());
    }
    if s.privileged == Some(true) {
        parts.push("PRIVILEGED".into());
    }
    if s.read_only_root_filesystem == Some(true) {
        parts.push("read-only rootfs".into());
    }
    if s.allow_privilege_escalation == Some(false) {
        parts.push("no privilege escalation".into());
    }
    if let Some(caps) = &s.capabilities {
        if let Some(d) = caps.drop.as_ref().filter(|d| !d.is_empty()) {
            parts.push(format!("drop {}", d.join(",")));
        }
        if let Some(a) = caps.add.as_ref().filter(|a| !a.is_empty()) {
            parts.push(format!("add {}", a.join(",")));
        }
    }
    (parts.join(" · "), s.privileged == Some(true))
}

fn status_of<'a>(pod: &'a Pod, name: &str, init: bool) -> Option<&'a ContainerStatus> {
    let s = pod.status.as_ref()?;
    let list = if init {
        s.init_container_statuses.as_ref()?
    } else {
        s.container_statuses.as_ref()?
    };
    list.iter().find(|c| c.name == name)
}

/// None when the pod no longer has that container.
pub fn build(pod: &Pod, name: &str, init: bool) -> Option<ContainerDetails> {
    let spec = pod.spec.as_ref()?;
    let list = if init {
        spec.init_containers.as_ref()?
    } else {
        &spec.containers
    };
    let c = list.iter().find(|c| c.name == name)?;
    let row: Option<ContainerRow> = container_rows(pod)
        .into_iter()
        .find(|r| r.raw_name == name && r.init == init);
    let status = status_of(pod, name, init);
    let dim = Style::new().fg(Color::DarkGray);

    let mut overview = Vec::new();
    if let Some(r) = &row {
        overview.push(field(
            "State",
            Span::styled(format!("● {}", r.state), Style::new().fg(r.color)),
        ));
        overview.push(field(
            "Ready",
            if r.ready {
                Span::styled("✓ yes", Style::new().fg(Color::Green))
            } else {
                Span::styled("✗ no", Style::new().fg(Color::Red))
            },
        ));
        overview.push(field("Restarts", Span::raw(r.restarts.to_string())));
        if !r.last.is_empty() {
            overview.push(field(
                "Last",
                Span::styled(r.last.clone(), Style::new().fg(Color::Red)),
            ));
        }
    }
    overview.push(field(
        "Image",
        Span::raw(c.image.clone().unwrap_or_default()),
    ));
    overview.push(field(
        "Pull",
        Span::raw(c.image_pull_policy.clone().unwrap_or_default()),
    ));
    if let Some(digest) = status.and_then(|s| s.image_id.split_once("@sha256:").map(|(_, d)| d)) {
        overview.push(field(
            "Digest",
            Span::styled(format!("sha256:{}", &digest[..digest.len().min(12)]), dim),
        ));
    }
    if let Some(id) = status
        .and_then(|s| s.container_id.as_deref())
        .filter(|i| !i.is_empty())
    {
        let id = id.split_once("://").map_or(id, |(_, i)| i);
        overview.push(field(
            "ID",
            Span::styled(id[..id.len().min(12)].to_string(), dim),
        ));
    }
    if let Some(w) = &c.working_dir {
        overview.push(field("Workdir", Span::raw(w.clone())));
    }
    let (sec, privileged) = security_text(c);
    if !sec.is_empty() {
        let color = if privileged { Color::Red } else { Color::Reset };
        overview.push(field("Security", Span::styled(sec, Style::new().fg(color))));
    }
    if c.restart_policy.as_deref() == Some("Always") {
        overview.push(field(
            "Kind",
            Span::raw("sidecar (init container that keeps running)"),
        ));
    }
    let flags: Vec<&str> = [(c.tty, "tty"), (c.stdin, "stdin")]
        .iter()
        .filter(|(on, _)| *on == Some(true))
        .map(|(_, n)| *n)
        .collect();
    if !flags.is_empty() {
        overview.push(field("Flags", Span::raw(flags.join(" "))));
    }

    let res = c.resources.clone().unwrap_or_default();
    let (requests, limits) = (
        res.requests.unwrap_or_default(),
        res.limits.unwrap_or_default(),
    );
    let mut names: Vec<&String> = requests.keys().chain(limits.keys()).collect();
    names.sort_by_key(|n| (!matches!(n.as_str(), "cpu" | "memory"), n.to_string()));
    names.dedup();
    let show = |m: &std::collections::BTreeMap<
        _,
        k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    >,
                n: &String| { m.get(n).map_or("-".to_string(), |q| q.0.clone()) };
    let quantity =
        |m: &std::collections::BTreeMap<
            String,
            k8s_openapi::apimachinery::pkg::api::resource::Quantity,
        >,
         key: &str,
         parse: fn(&str) -> Option<u64>| { m.get(key).and_then(|q| parse(&q.0)) };
    let cpu = (
        quantity(&requests, "cpu", parse_cpu),
        quantity(&limits, "cpu", parse_cpu),
    );
    let mem = (
        quantity(&requests, "memory", parse_mem),
        quantity(&limits, "memory", parse_mem),
    );
    let resources = names
        .iter()
        .map(|n| [n.to_string(), show(&requests, n), show(&limits, n)])
        .collect();

    let mut command = Vec::new();
    if let Some(cmd) = c.command.as_ref().filter(|v| !v.is_empty()) {
        command.push(("command", shell_join(cmd)));
    }
    if let Some(args) = c.args.as_ref().filter(|v| !v.is_empty()) {
        command.push(("args", shell_join(args)));
    }

    let ports = c
        .ports
        .iter()
        .flatten()
        .map(|p| {
            [
                p.name.clone().unwrap_or_default(),
                format!(
                    "{}/{}",
                    p.container_port,
                    p.protocol.as_deref().unwrap_or("TCP")
                ),
                p.host_port.map_or(String::new(), |h| format!("host {h}")),
            ]
        })
        .collect();
    let mounts = c
        .volume_mounts
        .iter()
        .flatten()
        .map(|m| {
            let path = m
                .sub_path
                .as_ref()
                .filter(|s| !s.is_empty())
                .map_or(m.mount_path.clone(), |s| format!("{} ({s})", m.mount_path));
            [
                path,
                m.name.clone(),
                if m.read_only == Some(true) {
                    "ro".into()
                } else {
                    "rw".into()
                },
            ]
        })
        .collect();

    Some(ContainerDetails {
        overview,
        resources,
        command,
        env: env_rows(c),
        ports,
        mounts,
        probes: vec![
            probe_line("liveness", &c.liveness_probe),
            probe_line("readiness", &c.readiness_probe),
            probe_line("startup", &c.startup_probe),
        ],
        cpu,
        mem,
        yaml: serde_yaml_ng::to_value(c).ok()?,
    })
}

/// A bordered table with bold yellow headers and a `<none>` row when empty.
fn table<'a>(
    title: String,
    header: &'static [&'static str],
    widths: Vec<Constraint>,
    rows: Vec<Row<'a>>,
) -> Table<'a> {
    let dim = Style::new().fg(Color::DarkGray);
    let rows = if rows.is_empty() {
        vec![Row::new([Cell::from("<none>").style(dim)])]
    } else {
        rows
    };
    Table::new(rows, widths)
        .header(
            Row::new(header.iter().copied())
                .style(Style::new().add_modifier(Modifier::BOLD).fg(Color::Yellow)),
        )
        .block(Block::bordered().border_style(dim).title(title))
}

/// Braille line chart of one metric over the last `HISTORY` samples, with request and limit as
/// flat reference lines (only when they fit the scale, so a huge limit can't flatten the graph).
fn chart(
    frame: &mut Frame,
    area: Rect,
    name: &str,
    values: Vec<f64>,
    (req, lim): (Option<u64>, Option<u64>),
    color: Color,
    fmt: fn(f64) -> String,
) {
    let n = values.len();
    let span = (HISTORY - 1) as f64 * POLL_SECS as f64; // seconds shown on the x axis
    let line: Vec<(f64, f64)> = values
        .iter()
        .enumerate()
        .map(|(i, v)| (-((n - 1 - i) as f64) * POLL_SECS as f64, *v))
        .collect();
    let peak = values.iter().copied().fold(0.0, f64::max);
    let fits = |v: Option<u64>| v.map(|v| v as f64).filter(|v| *v <= peak.max(1.0) * 4.0);
    let (req_line, lim_line) = (fits(req), fits(lim));
    let ymax = [req_line, lim_line]
        .into_iter()
        .flatten()
        .fold(peak, f64::max)
        .max(1.0)
        * 1.15;
    let flat = |v: f64| vec![(-span, v), (0.0, v)];
    let (req_data, lim_data) = (req_line.map(flat), lim_line.map(flat));

    let mut sets = vec![
        Dataset::default()
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::new().fg(color))
            .data(&line),
    ];
    if let Some(d) = &req_data {
        sets.push(
            Dataset::default()
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::new().fg(Color::Yellow))
                .data(d),
        );
    }
    if let Some(d) = &lim_data {
        sets.push(
            Dataset::default()
                .marker(Marker::Braille)
                .graph_type(GraphType::Line)
                .style(Style::new().fg(Color::Red))
                .data(d),
        );
    }

    let dim = Style::new().fg(Color::DarkGray);
    let mut title = vec![
        Span::raw(format!(" {name} ")),
        Span::styled(
            format!("now {} ", fmt(*values.last().unwrap_or(&0.0))),
            Style::new().fg(color),
        ),
    ];
    if let Some(r) = req {
        title.push(Span::styled(
            format!("━ req {} ", fmt(r as f64)),
            Style::new().fg(Color::Yellow),
        ));
    }
    if let Some(l) = lim {
        title.push(Span::styled(
            format!("━ limit {} ", fmt(l as f64)),
            Style::new().fg(Color::Red),
        ));
    }
    let minutes = format!("-{}m", (span / 60.0).round());
    frame.render_widget(
        Chart::new(sets)
            .block(Block::bordered().border_style(dim).title(Line::from(title)))
            .x_axis(
                Axis::default()
                    .style(dim)
                    .bounds([-span, 0.0])
                    .labels([minutes, "now".to_string()]),
            )
            .y_axis(Axis::default().style(dim).bounds([0.0, ymax]).labels([
                "0".to_string(),
                fmt(ymax / 2.0),
                fmt(ymax),
            ])),
        area,
    );
}

/// Layout, top to bottom:
///   Overview | Resources + Probes     what it is and how it is limited
///   Command                           what it runs (wraps, never cut off)
///   Environment | Ports + Mounts      the long lists get the leftover height
pub fn render(
    frame: &mut Frame,
    area: Rect,
    d: &ContainerDetails,
    usage: Option<&[Usage]>,
    env_sel: &mut TableState,
    message: &str,
) {
    let dim = Style::new().fg(Color::DarkGray);
    let outer = Block::bordered()
        .title(" Container ")
        .title_bottom(format!(" {message} "));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    // Wrapped command text: count lines at the real width so the box is exactly tall enough.
    let width = inner.width.saturating_sub(2).max(1) as usize;
    let cmd_lines: usize = if d.command.is_empty() {
        1
    } else {
        d.command
            .iter()
            .map(|(_, text)| (10 + text.chars().count()).div_ceil(width).max(1))
            .sum()
    };
    let top_h = (d.overview.len() as u16 + 2).max(d.resources.len() as u16 + 3 + 5);
    // The usage row only exists when metrics-server has samples for this container.
    let usage_h = if usage.is_some() { 10 } else { 0 };
    let [top, cmd, graphs, bottom] = Layout::vertical([
        Constraint::Length(top_h),
        Constraint::Length(cmd_lines.min(6) as u16 + 2),
        Constraint::Length(usage_h),
        Constraint::Fill(1),
    ])
    .areas(inner);
    if let Some(samples) = usage {
        let [cpu, mem] =
            Layout::horizontal([Constraint::Percentage(50), Constraint::Fill(1)]).areas(graphs);
        let cpu_values = samples.iter().map(|u| u.cpu as f64).collect();
        let mem_values = samples.iter().map(|u| u.mem as f64).collect();
        chart(frame, cpu, "CPU", cpu_values, d.cpu, Color::Cyan, |v| {
            cpu_text(Some(v as u64))
        });
        chart(
            frame,
            mem,
            "Memory",
            mem_values,
            d.mem,
            Color::Magenta,
            |v| mem_text(Some(v as u64)),
        );
    }

    let [left, right] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Fill(1)]).areas(top);
    frame.render_widget(
        Paragraph::new(d.overview.clone())
            .block(Block::bordered().border_style(dim).title(" Overview ")),
        left,
    );
    let [res, probes] = Layout::vertical([
        Constraint::Length(d.resources.len().max(1) as u16 + 3),
        Constraint::Fill(1),
    ])
    .areas(right);
    let res_rows = d.resources.iter().map(|r| Row::new(r.clone())).collect();
    frame.render_widget(
        table(
            " Resources ".into(),
            &["RESOURCE", "REQUEST", "LIMIT"],
            vec![Constraint::Fill(1); 3],
            res_rows,
        ),
        res,
    );
    frame.render_widget(
        Paragraph::new(d.probes.clone())
            .block(Block::bordered().border_style(dim).title(" Probes ")),
        probes,
    );

    let lines: Vec<Line> = if d.command.is_empty() {
        vec![Line::styled("<image default entrypoint>", dim)]
    } else {
        d.command
            .iter()
            .map(|(label, text)| {
                field(
                    label,
                    Span::styled(text.clone(), Style::new().fg(Color::Green)),
                )
            })
            .collect()
    };
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(Block::bordered().border_style(dim).title(" Command ")),
        cmd,
    );

    let [env, side] =
        Layout::horizontal([Constraint::Percentage(55), Constraint::Fill(1)]).areas(bottom);
    let env_rows: Vec<Row> = d
        .env
        .iter()
        .map(|(n, v, color)| {
            Row::new([
                Cell::from(n.clone()).style(Style::new().fg(Color::Cyan)),
                Cell::from(v.clone()).style(Style::new().fg(*color)),
            ])
        })
        .collect();
    let env_table = table(
        format!(" Environment ({}) ", d.env.len()),
        &["NAME", "VALUE / SOURCE"],
        vec![Constraint::Percentage(40), Constraint::Fill(1)],
        env_rows,
    )
    .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED));
    frame.render_stateful_widget(env_table, env, env_sel);

    let [ports, mounts] = Layout::vertical([
        Constraint::Length(d.ports.len().clamp(1, 5) as u16 + 3),
        Constraint::Fill(1),
    ])
    .areas(side);
    let port_rows = d.ports.iter().map(|r| Row::new(r.clone())).collect();
    frame.render_widget(
        table(
            format!(" Ports ({}) ", d.ports.len()),
            &["NAME", "PORT", "HOST"],
            vec![Constraint::Fill(1); 3],
            port_rows,
        ),
        ports,
    );
    let mount_rows = d.mounts.iter().map(|r| Row::new(r.clone())).collect();
    frame.render_widget(
        table(
            format!(" Mounts ({}) ", d.mounts.len()),
            &["PATH", "VOLUME", "MODE"],
            vec![
                Constraint::Fill(2),
                Constraint::Fill(1),
                Constraint::Length(4),
            ],
            mount_rows,
        ),
        mounts,
    );
}

#[cfg(test)]
impl ContainerDetails {
    pub fn assert_basics(&self) {
        assert_eq!(
            self.command,
            vec![
                ("command", "sh -c".to_string()),
                ("args", "'echo hi there'".to_string())
            ]
        );
        assert_eq!(self.env[1].1, "secret db/pw"); // the source, never the value
        assert_eq!(
            self.resources[0],
            ["cpu".to_string(), "100m".into(), "-".into()]
        );
        assert_eq!(
            self.resources[1],
            ["memory".to_string(), "64Mi".into(), "128Mi".into()]
        );
        assert_eq!(self.ports[0][1], "8080/TCP");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};

    #[test]
    fn usage_graphs_render_with_and_without_samples() {
        let pod: Pod = serde_yaml_ng::from_str(
            r#"{"metadata": {"name": "p"}, "spec": {"containers": [{"name": "a", "resources": {"limits": {"cpu": "500m", "memory": "1Gi"}}}]}}"#,
        )
        .unwrap();
        let d = build(&pod, "a", false).unwrap();
        let samples: Vec<Usage> = (0..30)
            .map(|i| Usage {
                cpu: 10 + i * 3,
                mem: (100 + i) << 20,
            })
            .collect();
        let mut term = Terminal::new(TestBackend::new(120, 50)).unwrap();
        for usage in [
            Some(samples.as_slice()),
            Some(&samples[..1]),
            Some(&[][..]),
            None,
        ] {
            term.draw(|f| render(f, f.area(), &d, usage, &mut TableState::default(), ""))
                .unwrap();
        }
        let text: String = term
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect();
        assert!(!text.contains("CPU")); // last draw had no samples: no graph row
        term.draw(|f| {
            render(
                f,
                f.area(),
                &d,
                Some(samples.as_slice()),
                &mut TableState::default(),
                "",
            )
        })
        .unwrap();
        let text: String = term
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect();
        assert!(text.contains("CPU") && text.contains("limit 500m"));
    }
}
