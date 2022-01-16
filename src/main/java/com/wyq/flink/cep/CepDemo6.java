package com.wyq.flink.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

import static org.apache.flink.table.api.Expressions.$;

public class CepDemo6 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        DataStream<Ticker> ds = env.readTextFile("data/cep/data5.txt")
                .flatMap(new FlatMapFunction<String, Ticker>() {
                    @Override
                    public void flatMap(String line, Collector<Ticker> collector) throws Exception {
                        String[] split = line.split(",");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        collector.collect(new Ticker(split[0], sdf.parse(split[1]).getTime(),
                                Integer.parseInt(split[2]), Integer.parseInt(split[3])));
                    }
                });

        tEnv.createTemporaryView("t", ds, $("symbol"), $("rowtime"), $("price"), $("tax"));

        Table table = tEnv.sqlQuery("select * from t");

        DataStream<Row> rowDataStream = tEnv.toAppendStream(table, Row.class);
        rowDataStream.print();

        env.execute("Flink CEP via SQL example");

    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class Ticker {
        private String symbol;

        private Long rowtime;

        private Integer price;

        private Integer tax;

    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Ticker> {
        private final long maxOutOfOrderness = 5000;
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Ticker row, long previousElementTimestamp) {
            System.out.println("Row is " + row);
            long timestamp = row.rowtime;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }
    
}
