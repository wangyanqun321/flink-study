package com.wyq.flink.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSqlWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        DataStream<Tuple3<String, String, Long>> ds =
                //env.readTextFile("data/input/sql/watermark/test1.txt")
                env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple3<String, String, Long>> collector) throws Exception {
                        String[] split = s.split(",");
                        collector.collect(Tuple3.of(split[0], split[1], Long.valueOf(split[2])));
                    }
                });

        ds = ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((tuple, timestamp) -> tuple.f2));

        Table inputTable = tEnv.fromDataStream(ds, $("f0"), $("f1"), $("f2"));
        System.out.println(inputTable.getSchema());
        String inputTableName = inputTable.toString();

        //Table table = tEnv.sqlQuery("select TUMBLE_ROWTIME(CAST(f2 AS timestamp(0)), INTERVAL '10' SECOND) as rowtime from " + inputTableName
                //+ " group by TUMBLE_ROWTIME(CAST(f2 AS timestamp(0)), INTERVAL '10' SECOND)");
        Table table = tEnv.sqlQuery("select CAST(f2 AS timestamp) from " + inputTableName);
        tEnv.toRetractStream(table, Row.class)
                .print();

        //rowDataStream.print();
        env.execute();
    }
}
