package com.wyq.flink.window.window;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
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

        //基于处理时间滚动窗口
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                //.print()
        ;

        //基于事件时间滚动窗口
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();


        //基于处理时间的滑动窗口
        keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1)
                .print()
        ;

        //基于事件时间的滑动窗口
        keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1)
                .print();

        //基于处理时间的会话窗口
        keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .sum(1)
                .print();

        // 基于事件时间的会话窗口
        keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .sum(1)
                .print();

        // 基于次数的滚动窗口
        keyedStream.countWindow(10);

        // 基于次数的滑动窗口
        keyedStream.countWindow(10, 5);

        env.execute();
    }

}
