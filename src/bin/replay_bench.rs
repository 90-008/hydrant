//! replay throughput benchmark.
//!
//! populates a temporary hydrant database with a configurable number of synthetic
//! events, then measures how long it takes to drain them through `Hydrant::subscribe`
//! under two config profiles:
//!
//! - **old**: `stream_replay_chunk_size = 64`, `stream_replay_chunk_pause = 2ms`
//! - **new**: `stream_replay_chunk_size = 0` (auto), `stream_replay_chunk_pause = 0ms`
//!
//! run with:
//!   cargo run --bin replay_bench
//!
//! optional env vars:
//!   REPLAY_BENCH_EVENTS=<n>    number of events to write (default: 20_000)
//!   REPLAY_BENCH_RUNS=<n>      number of timed runs per profile (default: 5)

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use futures::StreamExt;
use hydrant::config::Config;
use hydrant::control::Hydrant;

fn n_events() -> usize {
    std::env::var("REPLAY_BENCH_EVENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000)
}

fn n_runs() -> usize {
    std::env::var("REPLAY_BENCH_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Drain a cursor=0 subscriber until `total` events are received.
/// Returns (wall-clock elapsed, event count received).
async fn drain_stream(hydrant: &Hydrant, total: usize) -> (Duration, usize) {
    let mut stream = hydrant.subscribe(Some(0));
    let start = Instant::now();
    let mut count = 0usize;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => count += 1,
            Err(err) => {
                eprintln!("stream error after {count} events: {err}");
                break;
            }
        }
        if count >= total {
            break;
        }
    }
    (start.elapsed(), count)
}

fn make_config(db_path: &Path, chunk_size: usize, chunk_pause: Duration) -> Config {
    Config {
        database_path: db_path.to_path_buf(),
        enable_firehose: false,
        enable_crawler: Some(false),
        stream_replay_chunk_size: chunk_size,
        stream_replay_chunk_pause: chunk_pause,
        ..Config::default()
    }
}

fn stats(samples: &[Duration]) -> (Duration, Duration, Duration) {
    let mut sorted = samples.to_vec();
    sorted.sort();
    let min = *sorted.first().unwrap();
    let max = *sorted.last().unwrap();
    let mean = sorted.iter().sum::<Duration>() / sorted.len() as u32;
    (min, mean, max)
}

fn fmt_ms(d: Duration) -> String {
    format!("{:.1}ms", d.as_secs_f64() * 1000.0)
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "--run-profile" {
        // subprocess mode: runs a single profile and prints run times as space-separated milliseconds
        let profile = &args[2];
        let dir_path = Path::new(&args[3]);
        let n = args[4].parse::<usize>().unwrap();
        let runs = args[5].parse::<usize>().unwrap();

        let cfg = if profile == "old" {
            make_config(dir_path, 64, Duration::from_millis(2))
        } else {
            make_config(dir_path, 0, Duration::ZERO)
        };

        let hydrant = Hydrant::new(cfg).await?;
        let mut times = Vec::with_capacity(runs);
        for _ in 0..runs {
            let (elapsed, got) = drain_stream(&hydrant, n).await;
            assert_eq!(got, n, "[{profile}] expected {n} events, got {got}");
            times.push(elapsed.as_secs_f64() * 1000.0);
        }

        // output results to stdout so parent can parse them
        let formatted: Vec<String> = times.into_iter().map(|t| format!("{t}")).collect();
        println!("{}", formatted.join(" "));
        return Ok(());
    }

    // parent mode
    let n = n_events();
    let runs = n_runs();

    println!("replay_bench: {n} events, {runs} runs per profile");
    println!();

    // ── seed ─────────────────────────────────────────────────────────────────
    // write events into a temp dir once.
    let dir_path = std::env::temp_dir().join(format!(
        "hydrant_replay_bench_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    ));
    std::fs::create_dir_all(&dir_path).expect("failed to create temp dir");
    let _cleanup = TempDir(dir_path.clone());

    {
        let seed_cfg = make_config(&dir_path, 64, Duration::from_millis(2));
        let seed = Hydrant::new(seed_cfg).await?;
        let bytes = seed.seed_events_for_bench(n);
        println!(
            "wrote {n} events to {} ({:.1} KB, ~{} B/event)",
            dir_path.display(),
            bytes as f64 / 1024.0,
            bytes / n.max(1),
        );
        // dropped here — closes db cleanly
    }

    let current_exe = std::env::current_exe().expect("failed to get current executable path");

    // ── old profile: chunk_size=64, pause=2ms ────────────────────────────────
    println!();
    println!("Running old profile (64 / 2ms)...");
    let output_old = std::process::Command::new(&current_exe)
        .args([
            "--run-profile",
            "old",
            dir_path.to_str().unwrap(),
            &n.to_string(),
            &runs.to_string(),
        ])
        .output()
        .expect("failed to execute old profile subprocess");

    if !output_old.status.success() {
        eprintln!("{}", String::from_utf8_lossy(&output_old.stderr));
        miette::bail!("old profile subprocess failed");
    }

    let stdout_old = String::from_utf8(output_old.stdout).unwrap();
    let old_times: Vec<Duration> = stdout_old
        .split_whitespace()
        .map(|s| Duration::from_secs_f64(s.parse::<f64>().unwrap() / 1000.0))
        .collect();

    for (i, t) in old_times.iter().enumerate() {
        println!("  [old] run {}/{}: {}", i + 1, runs, fmt_ms(*t));
    }

    // ── new profile: chunk_size=0 (auto), pause=0ms ──────────────────────────
    println!();
    println!("Running new profile (auto / 0ms)...");
    let output_new = std::process::Command::new(&current_exe)
        .args([
            "--run-profile",
            "new",
            dir_path.to_str().unwrap(),
            &n.to_string(),
            &runs.to_string(),
        ])
        .output()
        .expect("failed to execute new profile subprocess");

    if !output_new.status.success() {
        eprintln!("{}", String::from_utf8_lossy(&output_new.stderr));
        miette::bail!("new profile subprocess failed");
    }

    let stdout_new = String::from_utf8(output_new.stdout).unwrap();
    let new_times: Vec<Duration> = stdout_new
        .split_whitespace()
        .map(|s| Duration::from_secs_f64(s.parse::<f64>().unwrap() / 1000.0))
        .collect();

    for (i, t) in new_times.iter().enumerate() {
        println!("  [new] run {}/{}: {}", i + 1, runs, fmt_ms(*t));
    }

    // ── summary ───────────────────────────────────────────────────────────────
    let (o_min, o_mean, o_max) = stats(&old_times);
    let (n_min, n_mean, n_max) = stats(&new_times);
    let speedup = o_mean.as_secs_f64() / n_mean.as_secs_f64();

    println!();
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│  replay throughput: {n} events, {runs} runs per profile");
    println!("├──────────────┬───────────┬───────────┬───────────┬───────────────┤");
    println!("│  profile     │    min    │    mean   │    max    │  Kev/s (mean) │");
    println!("├──────────────┼───────────┼───────────┼───────────┼───────────────┤");
    println!(
        "│  old(64/2ms) │  {:>8} │  {:>8} │  {:>8} │  {:>11.1}  │",
        fmt_ms(o_min),
        fmt_ms(o_mean),
        fmt_ms(o_max),
        n as f64 / o_mean.as_secs_f64() / 1000.0,
    );
    println!(
        "│  new(auto/0) │  {:>8} │  {:>8} │  {:>8} │  {:>11.1}  │",
        fmt_ms(n_min),
        fmt_ms(n_mean),
        fmt_ms(n_max),
        n as f64 / n_mean.as_secs_f64() / 1000.0,
    );
    println!("├──────────────┴───────────┴───────────┴───────────┴───────────────┤");
    println!("│  speedup (mean):  {speedup:.2}×");
    println!("└──────────────────────────────────────────────────────────────────┘");

    Ok(())
}
