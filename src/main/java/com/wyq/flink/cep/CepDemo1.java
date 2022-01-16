package com.wyq.flink.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 入门案例：检测同一个用户两秒内连续登陆失败两次
 */
public class CepDemo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> ds = env.readTextFile("data/cep/data1.txt")
                .flatMap(new FlatMapFunction<String, Event>() {
            @Override
            public void flatMap(String line, Collector<Event> collector) throws Exception {
                String[] split = line.split(",");
                collector.collect(new Event(split[0], split[1], split[2], Long.parseLong(split[3])));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(Event element) {
                        return element.eventTime * 1000;
                    }
                })
            .keyBy(Event::getUserId);

        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return "fail".equals(event.eventType);
                    }
                })
                .next("next")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return "fail".equals(event.eventType);
                    }
                })
                // 时间间隔不超过2秒
                .within(Time.seconds(2));

        PatternStream<Event> patternStream = CEP.pattern(ds, pattern);
        patternStream.select(new PatternSelectFunction<Event, String>() {

            @Override
            public String select(Map<String, List<Event>> pattern) throws Exception {
                Event start = pattern.getOrDefault("start", null).iterator().next();
                Event next = pattern.getOrDefault("next", null).iterator().next();
                return "start: " + start.toString() + ", next: " + next.toString();
            }
        }).print("恶意登陆检测告警");

        env.execute();
    }

    @Data
    @AllArgsConstructor
    public static class Event {

        private String userId;

        private String ip;

        private String eventType;

        private long eventTime;

    }

}
