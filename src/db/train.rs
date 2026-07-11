use miette::{IntoDiagnostic, Result};
use std::cell::RefCell;
use std::collections::HashSet;

use super::schema::Sampler;
use super::{Db, registry};

impl Db {
    pub fn train_dict(&self, ks_name: &str) -> Result<()> {
        let Some((name, cfg)) = registry::trainable()
            .into_iter()
            .find(|(name, _)| *name == ks_name)
        else {
            miette::bail!("unknown keyspace for training: {ks_name}");
        };
        let ks = self
            .keyspace_by_name(name)
            .expect("trainable keyspace is registered");

        let samples: Vec<Vec<u8>> = match cfg.sampler {
            Sampler::PerCollectionCap(per_collection_limit) => {
                // sample data blocks per collection, discovered lazily in the predicate
                let collection_counts: RefCell<std::collections::HashMap<Vec<u8>, usize>> =
                    RefCell::new(std::collections::HashMap::new());

                let new = ks
                    .sample_data_blocks(cfg.samples, |first, _last| {
                        let Some(sep_idx) = first.iter().position(|&b| b == super::keys::SEP)
                        else {
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
            Sampler::DedupKeys => {
                let mut seen_keys = HashSet::new();
                let captured_keys = RefCell::new(Vec::new());
                let new = ks
                    .sample_data_blocks(cfg.samples, |first, last| {
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
            }
        };

        if samples.is_empty() {
            miette::bail!("no samples found for keyspace {ks_name}, skipping training");
        }

        tracing::info!(
            "training zstd dictionary for keyspace {ks_name} ({} samples, {} bytes limit)...",
            samples.len(),
            cfg.dict_size,
        );
        let dict_bytes =
            lsm_tree::train_zstd_dict(&samples, cfg.dict_size as usize).into_diagnostic()?;
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
