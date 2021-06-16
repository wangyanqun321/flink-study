CREATE TABLE MyUserTable (
  id INT,
  name STRING,
  age INT,
  dt STRING
) PARTITIONED BY(dt)
WITH (
  'connector' = 'filesystem',
  'path' = 'file:///Users/yanqunwang/ICIdeaProjects/flink-study/data/input/sql/test2.txt',
  'format' = 'csv')


CREATE TABLE test.print(
  id INT,
  name STRING,
  age INT,
  dt STRING
) WITH (
  'connector' = 'print'
)

CREATE TABLE Orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3)
) WITH (
  'connector' = 'datagen'
)

CREATE TABLE user_clicks(
  username varchar,
  click_url varchar,
  ts timeStamp,
  WATERMARK wk FOR ts as withOffset(ts, 2000)  --为Rowtime定义Watermark。
) with (
  type='filesystem',
  'path' = 'file:///Users/yanqunwang/ICIdeaProjects/flink-study/data/input/sql/test3.txt',
  'format' = 'csv'
);

CREATE TABLE datahub_stream (
    `timestamp`               TIMESTAMP,
    card_id                   VARCHAR,
    location                  VARCHAR,
    `action`                  VARCHAR,
    WATERMARK wf FOR `timestamp` AS withOffset(`timestamp`, 1000)
) with (
  type='filesystem',
  'path' = 'file:///Users/yanqunwang/ICIdeaProjects/flink-study/data/input/sql/test3.txt',
  'format' = 'csv'
);