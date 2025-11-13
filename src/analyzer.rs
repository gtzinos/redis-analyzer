use color_eyre::eyre::Context as _;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use redis;
use scc::HashMap;

use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::{Config, SortOrder};
use crate::key_prefix::KeyPrefix;

pub struct AnalyzerResult {
    pub root_prefix: KeyPrefix,
    pub took: Duration,
}

pub fn run(config: &mut Config) -> AnalyzerResult {
    let mut root_prefix = KeyPrefix::new("", 0, config.all_keys_count, 0);

    let now = Instant::now();

    analyze_count(config, &mut root_prefix);
    analyze_memory_usage(config, &mut root_prefix);
    reorder(config, &mut root_prefix);
    backfill_other_keys(config, &mut root_prefix);

    let took = now.elapsed();

    AnalyzerResult { root_prefix, took }
}

fn analyze_count(config: &mut Config, root_prefix: &mut KeyPrefix) {
    let separator = config.separators_regex();
    let bar = if config.progress {
        println!("Scanning all keys (single pass)");
        ProgressBar::new(config.all_keys_count as u64)
    } else {
        ProgressBar::hidden()
    };

    let scan_size = config.scan_size;

    // 1. Collect all keys in a Vec<String>
    let mut all_keys = Vec::with_capacity(config.all_keys_count);
    config.databases.iter_mut().for_each(|database| {
        bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] {wide_bar} {pos}/{len} ({percent}%) [ETA: {eta_precise}]",
                )
                .expect("Failed to set progress bar style"),
        );

        let mut scan_command = redis::cmd("SCAN")
            .cursor_arg(0)
            .arg("COUNT")
            .arg(scan_size)
            .clone();

        let iter: redis::Iter<String> = scan_command
            .clone()
            .iter(&mut database.connection)
            .expect("running scan");

        for (i, key_result) in iter.enumerate() {
            let key = match key_result.wrap_err("failed to get key") {
                Ok(key) => key,
                Err(e) => {
                    eprintln!("Error retrieving key: {}", e);
                    continue;
                }
            };
            if i % 10_000 == 0 && i > 0 {
                bar.inc(10_000);
            }
            all_keys.push(key);
        }
    });
    bar.finish();

    // 2. Build the prefix tree from all keys
    fn insert_key(node: &mut KeyPrefix, key: &str, separator: &regex::Regex, max_depth: usize) {
        let mut current = node;
        let mut depth = 0;
        let mut last_pos = 0;
        let mut iter = separator.find_iter(key).peekable();
        while let Some(sep_match) = iter.next() {
            if depth >= max_depth {
                break;
            }
            let end = sep_match.end();
            let prefix_value = &key[0..end];
            // Find or create child by index
            let pos = current.children.iter().position(|c| c.value == prefix_value);
            let idx = match pos {
                Some(i) => {
                    current.children[i].keys_count += 1;
                    i
                },
                None => {
                    current.children.push(KeyPrefix::new(prefix_value, depth + 1, 1, 0));
                    current.children.len() - 1
                }
            };
            current = &mut current.children[idx];
            depth += 1;
            last_pos = end;
        }
        // For the leaf (full key or last segment)
        let leaf_value = &key[0..];
        let pos = current.children.iter().position(|c| c.value == leaf_value);
        match pos {
            Some(i) => {
                current.children[i].keys_count += 1;
            },
            None => {
                current.children.push(KeyPrefix::new(leaf_value, depth + 1, 1, 0));
            }
        }
    }

    for key in &all_keys {
        insert_key(root_prefix, key, &separator, config.depth);
    }
}

fn analyze_memory_usage(config: &mut Config, prefix: &mut KeyPrefix) {
    let bar = if config.progress {
        println!();
        println!("Memory scanning");
        ProgressBar::new(prefix.keys_count as u64)
    } else {
        ProgressBar::hidden()
    };

    bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "[{elapsed_precise}] {wide_bar} {pos}/{len} ({percent}%) [ETA: {eta_precise}]",
            )
            .expect("failed to set progress bar style"),
    );

    let scan_size = config.scan_size;
    let memory_usage_samples = config.memory_usage_samples;
    let prefix_mutex = Arc::new(Mutex::new(prefix));

    config.databases.par_iter_mut().for_each(|database| {
        let mut cursor: u64 = 0;

        loop {
            let scan_command = redis::cmd("SCAN")
                .cursor_arg(cursor)
                .arg("COUNT")
                .arg(scan_size)
                .clone();

            let (new_cursor, keys): (u64, Vec<String>) =
                scan_command.query(&mut database.connection).expect("scan");

            cursor = new_cursor;

            let mut memory_usage_command = redis::pipe().clone();

            for key in keys.iter() {
                memory_usage_command = memory_usage_command
                    .cmd("MEMORY")
                    .arg("USAGE")
                    .arg(key)
                    .arg("SAMPLES")
                    .arg(memory_usage_samples)
                    .clone();
            }

            let memory_usages: Vec<usize> = memory_usage_command
                .query(&mut database.connection)
                .expect("memory usage command");

            bar.inc(keys.len() as u64);

            for (key, memory_usage) in keys.iter().zip(memory_usages.iter()) {
                record_memory_usage(prefix_mutex.lock().unwrap().deref_mut(), key, *memory_usage);
            }

            if cursor == 0 {
                break;
            }
        }
    });

    bar.finish();
}

fn record_memory_usage(prefix: &mut KeyPrefix, key: &str, memory_usage: usize) {
    if key.starts_with(&prefix.value) {
        prefix.memory_usage += memory_usage;

        for child in prefix.children.iter_mut() {
            record_memory_usage(child, key, memory_usage);
        }
    }
}

fn reorder(config: &Config, prefix: &mut KeyPrefix) {
    if prefix.children.is_empty() {
        return;
    }

    prefix.children.sort_by_key(|c| match config.sort_order {
        SortOrder::KeysCount => c.keys_count,
        SortOrder::MemoryUsage => c.memory_usage,
    });
    prefix.children.reverse();
}

fn backfill_other_keys(config: &Config, prefix: &mut KeyPrefix) {
    if prefix.children.is_empty() {
        return;
    }

    let mut other_prefix = KeyPrefix::new(
        "[other]",
        prefix.depth + 1,
        prefix.keys_count,
        prefix.memory_usage,
    );

    for child in prefix.children.iter() {
        other_prefix.keys_count -= child.keys_count;
        other_prefix.memory_usage -= child.memory_usage;
    }

    let other_absolute_frequency =
        other_prefix.keys_count as f32 / config.all_keys_count as f32 * 100.;

    if other_absolute_frequency > config.min_count_percentage {
        prefix.children.push(other_prefix);
    }
}
