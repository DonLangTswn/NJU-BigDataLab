CREATE EXTERNAL TABLE IF NOT EXISTS user_io_external (
    user_namespace STRING,
    total_op_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/job2/out';

SELECT * FROM user_io_external;