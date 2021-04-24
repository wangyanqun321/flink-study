package com.wyq.flink.timeandwatermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class EventTimeAndWatermarkDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        DataStreamSource<Order> ds = env.addSource(new RichParallelSourceFunction<Order>() {

            private boolean flag = true;

            @Override
            public void run(SourceContext<Order> sourceContext) throws Exception {

                Random random = new Random();

                while (flag) {
                    String id = UUID.randomUUID().toString();
                    int userId = random.nextInt(2);
                    int money = random.nextInt(100);
                    long eventTime = System.currentTimeMillis() - random.nextInt(5) * 1000;
                    sourceContext.collect(new Order(id, userId, money, eventTime));
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        ds.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((order, timestamp) -> order.getEventTime())
        ).keyBy(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("money")
                .print();
        env.execute();

    }


    /**
     2,1,15,2021-04-21 23:40:00
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {

        private String id;

        private Integer userId;

        private Integer money;

        private Long eventTime;

    }

}
