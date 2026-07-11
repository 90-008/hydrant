//! keyspace registry: derives every by-name dispatch table from one row list.
//!
//! each row pairs a `Db` field path with its [`Schema`] type. the macro
//! generates the by-name lookup, the full keyspace enumeration (`/stats`,
//! compaction), `/debug/*` key/value rendering, and the zstd dictionary
//! training table. `Db::open` verifies that every opened keyspace has a row
//! (and vice versa), so a keyspace cannot be half-registered.

use fjall::Keyspace;
use serde_json::Value;

use super::schema::{self, Schema, TrainCfg};

macro_rules! registry {
    ($( $(#[$attr:meta])* $($path:ident).+ => $schema:ty ),+ $(,)?) => {
        impl super::Db {
            /// look up any open keyspace by its on-disk name.
            pub fn keyspace_by_name(&self, name: &str) -> Option<Keyspace> {
                $( $(#[$attr])* {
                    if name == <$schema>::NAME {
                        return Some(self.$($path).+.keyspace());
                    }
                } )+
                None
            }

            /// every open keyspace with its on-disk name.
            pub fn all_keyspaces(&self) -> Vec<(&'static str, Keyspace)> {
                let mut all = Vec::new();
                $( $(#[$attr])* {
                    all.push((<$schema>::NAME, self.$($path).+.keyspace()));
                } )+
                all
            }
        }

        /// render a raw value from the named keyspace for `/debug/*`.
        pub fn debug_value(name: &str, value: &[u8]) -> Value {
            $( $(#[$attr])* {
                if name == <$schema>::NAME {
                    return <$schema>::render_value(value);
                }
            } )+
            Value::String(hex::encode(value))
        }

        /// parse a user-supplied `/debug/*` key string for the named keyspace.
        pub fn debug_parse_key(name: &str, key: &str) -> Option<Vec<u8>> {
            $( $(#[$attr])* {
                if name == <$schema>::NAME {
                    return <$schema>::parse_debug_key(key);
                }
            } )+
            Some(key.as_bytes().to_vec())
        }

        /// render raw key bytes from the named keyspace for `/debug/iter`.
        pub fn debug_render_key(name: &str, key: &[u8]) -> String {
            $( $(#[$attr])* {
                if name == <$schema>::NAME {
                    return <$schema>::render_debug_key(key);
                }
            } )+
            String::from_utf8_lossy(key).into_owned()
        }

        /// keyspaces with zstd dictionary training enabled.
        pub fn trainable() -> Vec<(&'static str, TrainCfg)> {
            let mut all = Vec::new();
            $( $(#[$attr])* {
                if let Some(cfg) = <$schema>::TRAIN {
                    all.push((<$schema>::NAME, cfg));
                }
            } )+
            all
        }

        /// every registered keyspace name, for the open-time completeness check.
        pub(super) fn names() -> Vec<&'static str> {
            let mut all = Vec::new();
            $( $(#[$attr])* {
                all.push(<$schema>::NAME);
            } )+
            all
        }
    };
}

registry! {
    repos => schema::Repos,
    repo_metadata => schema::RepoMetadata,
    cursors => schema::Cursors,
    counts => schema::Counts,
    filter => schema::Filter,
    crawler => schema::Crawler,
    #[cfg(feature = "backlinks")]
    backlinks => schema::Backlinks,
    #[cfg(feature = "indexer")]
    indexer.records => schema::Records,
    #[cfg(feature = "indexer")]
    indexer.blocks => schema::Blocks,
    #[cfg(feature = "indexer")]
    indexer.pending => schema::Pending,
    #[cfg(feature = "indexer")]
    indexer.resync => schema::Resync,
    #[cfg(feature = "indexer")]
    indexer.resync_buffer => schema::ResyncBuffer,
    #[cfg(feature = "indexer_stream")]
    stream.events => schema::Events,
    #[cfg(feature = "jetstream")]
    jetstream.events => schema::JetstreamEvents,
    #[cfg(feature = "relay")]
    relay.events => schema::RelayEvents,
}
