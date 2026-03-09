use fjall::compaction::filter::Context;
use lsm_tree::compaction::{CompactionFilter, Factory};
use lsm_tree::compaction::{ItemAccessor, Verdict};

mod drop_all {
    use super::*;

    pub struct DropAllFilter;

    impl CompactionFilter for DropAllFilter {
        fn filter_item(&mut self, _: ItemAccessor<'_>, _: &Context) -> lsm_tree::Result<Verdict> {
            Ok(Verdict::Destroy)
        }
    }

    pub struct DropAllFilterFactory;

    impl Factory for DropAllFilterFactory {
        fn name(&self) -> &str {
            "drop_all"
        }

        fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
            Box::new(DropAllFilter)
        }
    }
}
pub use drop_all::*;

mod drop_prefix {
    use super::*;

    pub struct DropPrefixFilter {
        prefix: &'static [u8],
    }

    impl CompactionFilter for DropPrefixFilter {
        fn filter_item(
            &mut self,
            item: ItemAccessor<'_>,
            _: &Context,
        ) -> lsm_tree::Result<Verdict> {
            Ok(item
                .key()
                .starts_with(&self.prefix)
                .then_some(Verdict::Destroy)
                .unwrap_or(Verdict::Keep))
        }
    }

    pub struct DropPrefixFilterFactory {
        pub prefix: &'static [u8],
    }

    impl Factory for DropPrefixFilterFactory {
        fn name(&self) -> &str {
            "drop_prefix"
        }

        fn make_filter(&self, _: &Context) -> Box<dyn CompactionFilter> {
            Box::new(DropPrefixFilter {
                prefix: self.prefix,
            })
        }
    }
}
pub use drop_prefix::*;
