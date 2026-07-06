-- Athena queries for the stock market Kafka pipeline
-- Database: stock_market_kafka
-- Table: stock_ticks_raw

-- 1. Create external table over JSONL files stored in S3
CREATE EXTERNAL TABLE IF NOT EXISTS stock_market_kafka.stock_ticks_raw (
  event_time string,
  symbol string,
  trade_date string,
  `open` double,
  high double,
  low double,
  `close` double,
  volume bigint,
  source string
)
PARTITIONED BY (
  year string,
  month string,
  day string,
  hour string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true'
)
LOCATION 's3://doyoung-stock-kafka-pipeline-2026/stock_ticks/raw/';


-- 2. Load Hive-style partitions from S3 paths
MSCK REPAIR TABLE stock_market_kafka.stock_ticks_raw;


-- 3. Preview raw events
SELECT *
FROM stock_market_kafka.stock_ticks_raw
LIMIT 10;


-- 4. Count total events
SELECT COUNT(*) AS total_events
FROM stock_market_kafka.stock_ticks_raw;


-- 5. Count events by stock symbol
SELECT
  symbol,
  COUNT(*) AS event_count
FROM stock_market_kafka.stock_ticks_raw
GROUP BY symbol
ORDER BY event_count DESC;


-- 6. Basic price statistics by stock symbol
SELECT
  symbol,
  AVG("close") AS avg_close,
  MIN("close") AS min_close,
  MAX("close") AS max_close,
  AVG(volume) AS avg_volume
FROM stock_market_kafka.stock_ticks_raw
GROUP BY symbol
ORDER BY symbol;


-- 7. Latest event timestamp by symbol
SELECT
  symbol,
  MAX(event_time) AS latest_event_time
FROM stock_market_kafka.stock_ticks_raw
GROUP BY symbol
ORDER BY latest_event_time DESC;


-- 8. Show registered partitions
SHOW PARTITIONS stock_market_kafka.stock_ticks_raw;
