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

public class CepDemo4 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> ds = env.readTextFile("data/cep/data4.txt")
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
                        return "a".equals(event.eventType);
                    }
                })
                .followedBy("next").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.eventType.contains("b");
                    }
                }).oneOrMore()
                .followedBy("next2").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.eventType.contains("c");
                    }
                });
        CEP.pattern(ds, pattern).select(new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> pattern) throws Exception {
                List<Event> begin = pattern.getOrDefault("begin", null);
                List<Event> next = pattern.getOrDefault("next", null);
                List<Event> next2 = pattern.getOrDefault("next2", null);
                return "begin: " + begin + ", next: " + next + "next2: " + next2;
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
