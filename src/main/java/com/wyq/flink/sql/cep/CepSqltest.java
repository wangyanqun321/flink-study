package com.wyq.flink.sql.cep;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class CepSqltest {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE datahub_stream (\n" +
                "    `timestamp`  TIMESTAMP,\n" +
                "    card_id      VARCHAR,\n" +
                "    location VARCHAR,\n" +
                "    `action` VARCHAR,\n" +
                "    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '1' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'test-topic2',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format'    = 'csv'\n" +
                ")");
        // 此处有坑，mysql 连接器连不上不会直接报错，而是程序正常结束
        tEnv.executeSql("CREATE TABLE rds_out (\n" +
                "    start_timestamp TIMESTAMP,\n" +
                "    end_timestamp     TIMESTAMP,\n" +
                "    card_id     VARCHAR,\n" +
                "    `event`     VARCHAR\n" +
                ") WITH (\n" +
                "   'connector'  = 'jdbc',\n" +
                "   'url'        = 'jdbc:mysql://localhost:3306/wyq',\n" +
                "   'table-name' = 'cep_report',\n" +
                "   'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "   'username'   = 'root',\n" +
                "   'password'   = '123456'\n" +
                ")");

        tEnv.executeSql("insert into rds_out\n" +
                "select \n" +
                "`start_timestamp`, \n" +
                "`end_timestamp`, \n" +
                "card_id, `event`\n" +
                "from datahub_stream\n" +
                "MATCH_RECOGNIZE (\n" +
                "    PARTITION BY card_id   --按card_id分区，将相同卡号的数据分发到同一个计算节点。\n" +
                "    ORDER BY `timestamp`   --在窗口内，对事件时间进行排序。\n" +
                "    MEASURES               --定义如何根据匹配成功的输入事件构造输出事件。\n" +
                "        e2.`action` as `event`,   \n" +
                "        e1.`timestamp` as `start_timestamp`,   --第一次的事件时间为start_timestamp。\n" +
                "        LAST(e2.`timestamp`) as `end_timestamp` --最新的事件时间为end_timestamp。\n" +
                "    ONE ROW PER MATCH           --匹配成功输出一条。\n" +
                "    AFTER MATCH SKIP TO NEXT ROW --匹配后跳转到下一行。\n" +
                "    PATTERN (e1 e2) WITHIN INTERVAL '10' MINUTE  --定义两个事件，e1和e2。\n" +
                "    DEFINE                     --定义在PATTERN中出现的patternVariable的具体含义。\n" +
                "        e1 as e1.action = 'Consumption',    --事件一的action标记为Consumption。\n" +
                "        e2 as e2.action = 'Consumption' and e2.location <> e1.location --事件二的action标记为Consumption，且事件一和事件二的location不一致。\n" +
                ")");

    }


}
