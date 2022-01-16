package com.wyq.flink.window.lateness;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class LatenessDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        OutputTag<Tuple5<Long, String, String, Long, Integer>> delay = new OutputTag<Tuple5<Long, String, String, Long, Integer>>("delay"){};

        SingleOutputStreamOperator<Tuple5<Long, String, String, Long, Integer>> dataStream = env.socketTextStream("localhost", 9999)
                //1,2022-01-15 13:00:00,hello
                //1,2022-01-15 13:00:59,hello
                //1,2022-01-15 13:01:00,hello
                //1,2022-01-15 13:01:35,hello
                //1,2022-01-15 13:02:04,hello
                //1,2022-01-15 12:59:00,hello
                //1,2022-01-15 13:00:00,hello
                //1,2022-01-15 13:00:59,hello
                //1,2022-01-15 12:50:00,hello
                .flatMap(new FlatMapFunction<String, Tuple5<Long, String, String, Long, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple5<Long, String, String, Long, Integer>> collector) throws Exception {
                        if (s != null && s.length() > 0) {
                            String[] line = s.split(",");
                            collector.collect(Tuple5.of(Long.valueOf(line[0]), line[1], line[2], getTime(line[1]), 1));
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <Tuple5<Long, String, String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.f3)
                )
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(delay)
                .sum(4);
        dataStream.print();
        dataStream.getSideOutput(delay).print();
        env.execute("test lateness data");
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
