package com.wyq.flink.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class WorldCount {

    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple4<Integer, Long, String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple4<Integer, Long, String, Integer>> collector) throws Exception {
                        if (s == null) {
                            return;
                        }
                        // 1,2022-01-09 20:00:00,hello
                        // 2,2022-01-09 20:00:00,world
                        // 1,2022-01-09 20:01:00,hello
                        // 1,2022-01-09 20:02:00,hello
                        // 1,2022-01-09 20:03:00,hello
                        // 1,2022-01-09 20:04:00,hello
                        // 1,2022-01-09 20:05:00,hello
                        // 1,2022-01-09 20:05:05,hello
                        String[] line = s.split(",");
                        collector.collect(Tuple4.of(Integer.valueOf(line[0]),
                                sdf.parse(line[1], new ParsePosition(0)).getTime(), line[2], 1));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple4<Integer, Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((t, ts) -> t.f1))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .sum(3)
                .print();
        env.execute("word count");
    }

}
