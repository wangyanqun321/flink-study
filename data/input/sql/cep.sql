insert into rds_out
select
`start_timestamp`,
`end_timestamp`,
card_id, `event`
from datahub_stream
MATCH_RECOGNIZE (
    PARTITION BY card_id   --按card_id分区，将相同卡号的数据分发到同一个计算节点。
    ORDER BY `timestamp`   --在窗口内，对事件时间进行排序。
    MEASURES               --定义如何根据匹配成功的输入事件构造输出事件。
        e2.`action` as `event`,
        e1.`timestamp` as `start_timestamp`,   --第一次的事件时间为start_timestamp。
        LAST(e2.`timestamp`) as `end_timestamp` --最新的事件时间为end_timestamp。
    ONE ROW PER MATCH           --匹配成功输出一条。
    AFTER MATCH SKIP TO NEXT ROW --匹配后跳转到下一行。
    PATTERN (e1 e2+) WITHIN INTERVAL '10' MINUTE  --定义两个事件，e1和e2。
    DEFINE                     --定义在PATTERN中出现的patternVariable的具体含义。
        e1 as e1.action = 'Consumption',    --事件一的action标记为Consumption。
        e2 as e2.action = 'Consumption' and e2.location <> e1.location --事件二的action标记为Consumption，且事件一和事件二的location不一致。
);


CREATE TABLE cep_report (
id INT primary key auto_increment,
start_timestamp TIMESTAMP,
end_timestamp     TIMESTAMP,
card_id     VARCHAR(255),
`event`     VARCHAR(255));

2018-04-13 12:00:00,1,Beijing,Consumption
2018-04-13 12:05:00,1,Shanghai,Consumption
2018-04-13 12:10:00,1,Shenzhen,Consumption
2018-04-13 12:20:00,1,Shenzhen,Consumption

2018-04-13 12:25:00,1,Shenzhen,Consumption
2018-04-13 12:26:00,1,Shenzhen,Consumption
2018-04-13 12:27:00,1,Shenzhen,Consumption
2018-04-13 12:28:00,1,Shanghai,Consumption
2018-04-13 12:28:10,1,Shanghai,Consumption
2018-04-13 12:28:10,1,Shenzhen,Consumption
2018-04-13 12:28:11,1,Shenzhen,Consumption