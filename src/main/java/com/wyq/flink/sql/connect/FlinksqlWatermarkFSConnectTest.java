package com.wyq.flink.sql.connect;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinksqlWatermarkFSConnectTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String ddl = "CREATE TABLE datahub_stream (\n" +
                "    ts TIMESTAMP,\n" +
                "    card_id VARCHAR,\n" +
                "    location VARCHAR,\n" +
                "    action VARCHAR,\n" +
                "    WATERMARK wf FOR ts AS withOffset(ts, 1000)\n" +
                ") with (\n" +
                "  type='filesystem',\n" +
                "  'path' = 'file:///Users/yanqunwang/ICIdeaProjects/flink-study/data/input/sql/watermark2',\n" +
                "  'format' = 'csv'\n" +
                ");";
        tEnv.executeSql(ddl);
        Table table = tEnv.sqlQuery("select * from datahub_stream");
        DataStream<Row> rowDataStream = tEnv.toAppendStream(table, Row.class);
        rowDataStream.print();
        env.execute();
    }

}
