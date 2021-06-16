package com.wyq.flink.sql.connect;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSqlPrintConnectTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String inputDdl = "CREATE TABLE MyUserTable (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  dt STRING\n" +
                ") PARTITIONED BY(dt) WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:///Users/yanqunwang/ICIdeaProjects/flink-study/data/input/sql/whatever',\n" +
                "  'format' = 'csv', " +
                "  'partition.default-name' = 'wyq')";
        tEnv.executeSql(inputDdl);

        String outputDdl = "CREATE TABLE test.print(\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  dt STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";

        tEnv.executeSql(outputDdl);

        String sql = "insert into test.print select * from MyUserTable";

        tEnv.sqlQuery(sql);
        env.execute();
    }

}
