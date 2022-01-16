package com.wyq.flink.window.evictor;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TimeEvictorDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple3<String, Integer, Long>> collector) throws Exception {
                        if (StringUtils.isNullOrWhitespaceOnly(line)) {
                            return;
                        }
                        String[] word = line.split(",");
                        collector.collect(Tuple3.of(word[0], Integer.valueOf(word[1]), getTime(word[2])));

                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner((t, ts) -> t.f2))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                //.trigger(ContinuousEventTimeTrigger.of(Time.minutes(5)))
                .evictor(TimeEvictor.of(Time.minutes(3)))
                .sum(1)
                .print();
        // hello,2,2022-01-16 00:00:00
        // hello,2,2022-01-16 00:01:00
        // hello,2,2022-01-16 00:04:00
        // hello,2,2022-01-16 00:05:30
        // hello,2,2022-01-16 00:05:32
        // hello,2,2022-01-16 00:05:31
        // hello,2,2022-01-16 00:10:30

        env.execute("test count window");
    }

    private static long getTime(String timeStr) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return sdf.parse(timeStr).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}
