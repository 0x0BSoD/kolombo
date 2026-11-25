//! Fullscreen "loading" screen: the Kubernetes logo (src/k8s_logo.txt) with a colour wave sweeping over it.
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::Paragraph,
};
use std::{sync::OnceLock, time::Instant};

const LOGO: &str = include_str!("k8s_logo.txt");
const DARK: (f64, f64, f64) = (30.0, 60.0, 140.0);
const LIGHT: (f64, f64, f64) = (130.0, 190.0, 255.0);

/// Logo scaled down (nearest neighbour) to fit `area`, shaded by a diagonal sine wave moving with wall time,
/// so every caller animates in sync as long as the screen is redrawn often. `label` goes underneath.
pub fn render(frame: &mut Frame, area: Rect, label: &str) {
    static START: OnceLock<Instant> = OnceLock::new();
    let t = START.get_or_init(Instant::now).elapsed().as_secs_f64();
    let src: Vec<Vec<char>> = LOGO.lines().map(|l| l.chars().collect()).collect();
    let (sw, sh) = (
        src.iter().map(Vec::len).max().unwrap_or(1),
        src.len().max(1),
    );
    let scale = (area.width as f64 / sw as f64)
        .min(area.height.saturating_sub(2) as f64 / sh as f64)
        .min(1.0);
    let (w, h) = (
        ((sw as f64 * scale) as usize).max(1),
        ((sh as f64 * scale) as usize).max(1),
    );
    let mut lines: Vec<Line> = (0..h)
        .map(|y| {
            let row = &src[(y as f64 / scale) as usize % sh];
            let spans: Vec<Span> = (0..w)
                .map(|x| {
                    let ch = row.get((x as f64 / scale) as usize).copied().unwrap_or(' ');
                    let phase = (x as f64 / w as f64 * 3.0 + y as f64 / h as f64 * 2.0)
                        * std::f64::consts::TAU
                        - t * 2.5;
                    let k = (phase.sin() + 1.0) / 2.0;
                    let c = |a: f64, b: f64| (a + (b - a) * k) as u8;
                    Span::styled(
                        ch.to_string(),
                        Style::new().fg(Color::Rgb(
                            c(DARK.0, LIGHT.0),
                            c(DARK.1, LIGHT.1),
                            c(DARK.2, LIGHT.2),
                        )),
                    )
                })
                .collect();
            Line::from(spans)
        })
        .collect();
    lines.push(Line::default());
    lines.push(Line::styled(
        label.to_string(),
        Style::new().fg(Color::DarkGray),
    ));
    let rect = area.centered(
        Constraint::Length((w as u16).max(label.chars().count() as u16)),
        Constraint::Length(lines.len() as u16),
    );
    frame.render_widget(Paragraph::new(lines).centered(), rect);
}
