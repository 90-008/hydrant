# hydrant

## configuration

environment variables:
- `HYDRANT_DATABASE_PATH`: path to database folder (default: `./hydrant.db`)
- `HYDRANT_RELAY_HOST`: relay WebSocket URL (default: `wss://relay.fire.hose.cam`)
- `HYDRANT_PLC_URL`: base URL of the PLC directory (default: `https://plc.wtf`).
- `HYDRANT_FULL_NETWORK`: if set to `true`, the indexer will discover and index all repos it sees.
- `HYDRANT_CURSOR_SAVE_INTERVAL`: how often to save the Firehose cursor (default: `10s`).
