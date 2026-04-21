use crate::db::keys;
use fjall::compaction::filter::Context;
use lsm_tree::compaction::{CompactionFilter, Factory};
use lsm_tree::compaction::{ItemAccessor, Verdict};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct CountsGcFilterFactory {
    pub drop_collection_counts: bool,
    pub delta_gc_watermark: Arc<AtomicU64>,
}

struct CountsGcFilter {
    drop_collection_counts: bool,
    delta_gc_watermark: Arc<AtomicU64>,
}

impl CompactionFilter for CountsGcFilter {
    fn filter_item(&mut self, item: ItemAccessor<'_>, _: &Context) -> lsm_tree::Result<Verdict> {
        let key = item.key();

        if self.drop_collection_counts && key.starts_with(keys::COUNT_COLLECTION_PREFIX) {
            return Ok(Verdict::Remove);
        }

        if key.starts_with(keys::COUNT_DELTA_PREFIX)
            && let Ok((id, _)) = keys::parse_count_delta_key(key)
            && id <= self.delta_gc_watermark.load(Ordering::Relaxed)
        {
            return Ok(Verdict::Destroy);
        }

        Ok(Verdict::Keep)
    }
}

impl Factory for CountsGcFilterFactory {
    fn name(&self) -> &str {
        "counts_gc"
    }

    fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
        Box::new(CountsGcFilter {
            drop_collection_counts: self.drop_collection_counts,
            delta_gc_watermark: self.delta_gc_watermark.clone(),
        })
    }
}
