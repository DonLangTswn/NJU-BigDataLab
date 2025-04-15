-- 预先执行部分
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;

-- 任务3-1: 创建分区表
CREATE TABLE IF NOT EXISTS IOlog_part_221220070 (
    block_id STRING,
    io_offset INT,
    io_size INT,
    op_time STRING,
    op_name INT,

    user_name STRING,
    rs_shard_id STRING,
    op_count INT,
    host_name STRING
)
PARTITIONED BY (user_namespace STRING)  -- user_namespace 作为分区字段
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;