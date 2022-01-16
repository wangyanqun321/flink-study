package com.wyq.flink.window.trigger;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class DeltaTriggerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple4<Integer, Integer, Double, Long>> carDataStream =
                env.fromElements(
                        //        id,时速,    行走里程   行走时间
                        Tuple4.of(1, 0, 0 * 1000.0, 0 * 60 * 1000L),
                        Tuple4.of(1, 50, 1 * 1000.0, 2 * 60 * 1000L),
                        Tuple4.of(1, 90, 2 * 1000.0, 2 * 60 * 1000L),
                        Tuple4.of(1, 85, 5 * 1000.0, 3 * 60 * 1000L),
                        Tuple4.of(1, 110, 8 * 1000.0, 4 * 60 * 1000L),
                        Tuple4.of(1, 100, 10 * 1000.0, 5 * 60 * 1000L),
                        Tuple4.of(1, 100, 10.1 * 1000.0, 9 * 60 * 1000L),
                        Tuple4.of(1, 90, 12 * 1000.0, 9 * 60 * 1000L),
                        Tuple4.of(1, 100, 15 * 1000.0, 9 * 60 * 1000L),
                        Tuple4.of(1, 100, 20.2 * 1000.0, 9 * 60 * 1000L)
                );
        carDataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0)))
                .keyBy(t -> t.f0)
                .window(GlobalWindows.create())
                //
                // .evictor(TimeEvictor.of(Time.minutes(2)))
                // 设置阈值为1000米(10公里)
                .trigger(DeltaTrigger.of(10000, new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
                    @Override
                    public double getDelta(Tuple4<Integer, Integer, Double, Long> oldDataPoint, Tuple4<Integer, Integer, Double, Long> newDataPoint) {
                        return newDataPoint.f2 - oldDataPoint.f2;
                    }
                }, carDataStream.getType().createSerializer(env.getConfig())))
                .max(1)
                .print();
        env.execute("delta trigger demo");
    }

    private static Long getTime(String timeStr) {
        SimpleDateFormat sdf = new SimpleDateFormat("");
        try {
            return sdf.parse(timeStr).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

}
