-- 任务3-3:
-- 导入任意5条数据到分区表
INSERT INTO TABLE IOlog_part_221220070 PARTITION (user_namespace)
VALUES
('blk101', 0,    512,  '2023-01-03 08:12:45', 2, 'user3', 'shard3', 8, 'host3', '2203126709'),
('blk205', 512,  1024, '2023-01-04 14:30:22', 1, 'user4', 'shard1', 2, 'host1', '2203126710'),
('blk302', 1536, 512,  '2023-01-05 09:45:11', 2, 'user5', 'shard2', 6, 'host4', '2203126711'),
('blk418', 2048, 256,  '2023-01-06 16:20:33', 1, 'user6', 'shard3', 1, 'host2', '2203126710'),
('blk500', 2304, 768,  '2023-01-07 11:10:07', 2, 'user7', 'shard1', 4, 'host5', '2203126711');

-- 导入 IOlog.trace 到分桶表中
CREATE TABLE IF NOT EXISTS IOlog_temp_221220070 (
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
);
-- 先将本地文件读取进中间临时表
LOAD DATA INPATH '/job3/in/IOlog.trace'
INTO TABLE IOlog_temp_221220070;
-- 再将临时表中的数据导入分桶表
INSERT OVERWRITE TABLE IOlog_bucket_221220070
SELECT * FROM IOlog_temp_221220070;