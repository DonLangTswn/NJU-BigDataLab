-- 任务3-2: 创建分桶表
CREATE TABLE IF NOT EXISTS IOlog_bucket_221220070 (
    block_id STRING,
    io_offset INT,
    io_size INT,
    op_time STRING,
    op_name INT,
    user_namespace STRING,
    user_name STRING,
    rs_shard_id STRING,
    op_count INT,
    host_name STRING
)
CLUSTERED BY (block_id) INTO 3 BUCKETS  -- 按照 block_id 分3个桶
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;