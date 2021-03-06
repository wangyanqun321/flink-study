package com.wyq.flink.window;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class ProcessTimeWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9999);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                Arrays.stream(words).forEach(word -> collector.collect(Tuple2.of(word, 1)));
            }
        }).keyBy(t -> t.f0);

        /**
         * 滚动窗口
         */
        keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
                .sum(1)
                //.print()
        ;

        /**
         * 滑动窗口
         */
        keyedStream.window(SlidingProcessingTimeWindows.of(Time.of(10, TimeUnit.SECONDS),
                Time.of(5, TimeUnit.SECONDS)))
                .sum(1)
                .print()
        ;

        env.execute();
    }

}
