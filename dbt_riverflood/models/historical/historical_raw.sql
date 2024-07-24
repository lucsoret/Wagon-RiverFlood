-- models/create_external_table.sql
CREATE OR REPLACE EXTERNAL TABLE
  `external_historical_raw`
  OPTIONS(
    format ="TABLE_FORMAT",
    uris = ['BUCKET_PATH'],
    max_staleness = STALENESS_INTERVAL,
    metadata_cache_mode = 'CACHE_MODE'
    );
