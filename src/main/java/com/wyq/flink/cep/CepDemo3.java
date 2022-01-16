package com.wyq.flink.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
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

public class CepDemo3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> ds = env.readTextFile("data/cep/data3.txt")
                .flatMap(new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String line, Collector<Event> collector) throws Exception {
                        String[] split = line.split(",");
                        collector.collect(new Event(split[0], split[1], split[2], Long.parseLong(split[3])));
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Event element) {
                        return element.eventTime * 1000;
                    }
                })
                .keyBy(Event::getUserId);
        Pattern<Event, Event> pattern = Pattern.<Event>begin("begin")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return "success".equals(event.eventType);
                    }
                })
                .next("next").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return "fail".equals(event.eventType);
                    }
                }).oneOrMore()
                // 不包含
                .until(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.ip.equals("192.168.0.3");
                    }
                });
                // 所有满足条件的事件间隔在15秒
        CEP.pattern(ds, pattern).select(new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> pattern) throws Exception {
                List<Event> begin = pattern.getOrDefault("begin", null);
                List<Event> next = pattern.getOrDefault("next", null);
                return "begin: " + begin + ", next: " + next;
            }
        }).print();
        env.execute();
    }


    @Data
    @AllArgsConstructor
    private static class Event {
        private String userId;

        private String ip;

        private String eventType;

        private long eventTime;
    }
}
