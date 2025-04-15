-- 1. 查询分区表某个分区的数据
SELECT * FROM iolog_part_221220070
WHERE user_namespace = '2203126710';

-- 2. 查询分桶表中每个用户的不同主机数
SELECT user_name, COUNT(DISTINCT host_name) AS host_count
FROM iolog_bucket_221220070
GROUP BY user_name;

-- 3. 查询分桶表中每个命名空间的写操作次数
SELECT user_namespace, SUM(op_count) AS write_op_count
FROM iolog_bucket_221220070
WHERE op_name = 2
GROUP BY user_namespace;