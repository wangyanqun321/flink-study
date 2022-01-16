package com.wyq.flink.timeandwatermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class EventTimeAndWaterMarkOldVersion {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> ds = env.fromElements(new Event(1, "a", 111L),
                new Event(1, "b", 222L),
                new Event(3, "c", 333L));
        ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(Event event) {
                return event.eventTime;
            }
        });
    }

    @Data
    @AllArgsConstructor
    public static class Event {

        private Integer id;

        private String name;

        private Long eventTime;

    }

}
