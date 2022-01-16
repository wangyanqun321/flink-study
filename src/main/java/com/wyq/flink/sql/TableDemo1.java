package com.wyq.flink.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class TableDemo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        DataStream<Tuple2<String, String>> ds = env.readTextFile("data/input/sql/test1.txt")
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
                String[] split = s.split(",");
                collector.collect(Tuple2.of(split[0], split[1]));
            }
        });
        Table inputTable = tEnv.fromDataStream(ds, $("f0"), $("f1"));
        String inputTableName = inputTable.toString();
        Table table = tEnv.sqlQuery("select * from " + inputTableName);
        DataStream<Row> rowDataStream = tEnv.toAppendStream(table, Row.class);
        rowDataStream.print();
        env.execute();
    }

}
