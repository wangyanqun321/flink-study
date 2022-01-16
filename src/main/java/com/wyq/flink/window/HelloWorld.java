package com.wyq.flink.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class HelloWorld {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("", 9999)
                .map(new MapFunction<String, Tuple4<Long, String, String, Integer>>() {

                    @Override
                    public Tuple4<Long, String, String, Integer> map(String s) throws Exception {
                        if(s != null && s.length() > 0) {
                            String[] line = s.split(" ");
                            return Tuple4.of(Long.valueOf(line[0]), line[1], line[2], Integer.valueOf(line[3]));
                        }
                        return null;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)));

    }
    
}
