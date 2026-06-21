use miette::{IntoDiagnostic, Result};
use std::cell::RefCell;
use std::collections::HashSet;

use super::Db;
#[cfg(feature = "indexer")]
use super::keys;

const fn kb(v: u32) -> u32 {
    v * 1024
}

impl Db {
    pub fn train_dict(&self, ks_name: &str) -> Result<()> {
        let ks = match ks_name {
            #[cfg(feature = "indexer")]
            "blocks" => &self.blocks,
            #[cfg(feature = "indexer_stream")]
            "events" => &self.events,
            #[cfg(feature = "jetstream")]
            "jetstream_events" => &self.jetstream_events,
            "repos" => &self.repos,
            #[cfg(feature = "backlinks")]
            "backlinks" => &self.backlinks,
            _ => miette::bail!("unknown keyspace for training: {ks_name}"),
        };

        let dict_size = match ks_name {
            "blocks" => kb(128),
            "events" => kb(64),
            "jetstream_events" => kb(64),
            "repos" => kb(64),
            "backlinks" => kb(64),
            _ => kb(32),
        };

        let samples: Vec<Vec<u8>> = if ks_name == "blocks" {
            #[cfg(not(feature = "indexer"))]
            miette::bail!("indexer feature required for blocks keyspace training");

            #[cfg(feature = "indexer")]
            {
                // sample up to 200 data blocks per collection, discovered lazily in the predicate
                let per_collection_limit = 200usize;
                let collection_counts: RefCell<std::collections::HashMap<Vec<u8>, usize>> =
                    RefCell::new(std::collections::HashMap::new());

                let new = ks
                    .sample_data_blocks(5000, |first, _last| {
                        let Some(sep_idx) = first.iter().position(|&b| b == keys::SEP) else {
                            return false;
                        };
                        let mut counts = collection_counts.borrow_mut();
                        let count = counts.entry(first[..sep_idx].to_vec()).or_insert(0);
                        if *count >= per_collection_limit {
                            return false;
                        }
                        *count += 1;
                        true
                    })
                    .into_diagnostic()?;

                new.into_iter().map(|s| s.to_vec()).collect()
            }
        } else {
            let mut seen_keys = HashSet::new();
            let captured_keys = RefCell::new(Vec::new());
            let new = ks
                .sample_data_blocks(1000, |first, last| {
                    let passes = !seen_keys.contains(&(first.to_vec(), last.to_vec()));
                    if passes {
                        captured_keys
                            .borrow_mut()
                            .push((first.to_vec(), last.to_vec()));
                    }
                    passes
                })
                .into_diagnostic()?;

            new.into_iter()
                .zip(captured_keys.into_inner())
                .filter_map(|(s, keys)| seen_keys.insert(keys).then_some(s.to_vec()))
                .collect()
        };

        if samples.is_empty() {
            miette::bail!("no samples found for keyspace {ks_name}, skipping training");
        }

        tracing::info!(
            "training zstd dictionary for keyspace {ks_name} ({} samples, {dict_size} bytes limit)...",
            samples.len(),
        );
        let dict_bytes =
            lsm_tree::train_zstd_dict(&samples, dict_size as usize).into_diagnostic()?;
        let path = self.path.join(format!("dict_{ks_name}.bin"));
        std::fs::write(&path, &dict_bytes).into_diagnostic()?;
        tracing::info!(
            "saved zstd dictionary for keyspace {ks_name} ({} bytes) to {}. please restart the indexer to use the new dictionary.",
            dict_bytes.len(),
            path.display()
        );

        Ok(())
    }
}
