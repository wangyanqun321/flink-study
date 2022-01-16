package com.wyq.flink.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 *  检测用户下单后15分钟没有支付
 */
public class CepDemo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> ds = env.readTextFile("data/cep/data2.txt")
                .flatMap(new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String line, Collector<Event> collector) throws Exception {
                        String[] split = line.split(",");
                        collector.collect(new Event(split[0], split[1], Long.parseLong(split[2])));
                    }
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Event element) {
                        return element.eventTime * 1000;
                    }
                })
                .keyBy(Event::getOrderId);
        Pattern<Event, Event> pattern = Pattern.<Event>begin("begin")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return "create".equals(event.eventType);
                    }
                })
                //宽松连续，中间可以发生其他事情
                .followedBy("followedBy")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return "pay".equals(event.eventType);
                    }
                })
                // 所有满足条件的事件间隔在15秒
                .within(Time.minutes(15));

        OutputTag<Event> outputTag = new OutputTag<Event>("orderTimeout", TypeInformation.of(Event.class));
        SingleOutputStreamOperator<Object> patternResult = CEP.pattern(ds, pattern).select(outputTag, new PatternTimeoutFunction<Event, Event>() {
            @Override
            public Event timeout(Map<String, List<Event>> orderPayEvents, long timeoutTimestamp) throws Exception {
                return orderPayEvents.getOrDefault("begin", null).iterator().next();
            }
        }, new PatternSelectFunction<Event, Object>() {
            @Override
            public String select(Map<String, List<Event>> orderPayEvents) throws Exception {
                return orderPayEvents.getOrDefault("followedBy", null).iterator().next().getOrderId();
            }
        });
        patternResult.print("15秒内支付的订单编号：");
        patternResult.getSideOutput(outputTag).print("15秒内没有支付的订单：");
        env.execute();
    }

    @Data
    @AllArgsConstructor
    private static class Event {
        private String orderId;

        private String eventType;

        private Long eventTime;
    }

}
