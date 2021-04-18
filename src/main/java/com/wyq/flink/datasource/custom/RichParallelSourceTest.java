package com.wyq.flink.datasource.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;

public class RichParallelSourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Order> ds = env.addSource(new MyOrderSource()).setParallelism(2);
        ds.print().setParallelism(1);
        env.execute();
    }

    public static class MyOrderSource extends RichParallelSourceFunction<Order> {

        private volatile boolean cancel;

        @Override
        public void run(SourceContext<Order> sourceContext) throws Exception {

            Random random = new Random();
            while (!cancel) {
                Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3),
                        random.nextInt(101), Instant.now().getEpochSecond());
                sourceContext.collect(order);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            cancel = true;
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class Order {
        private String id;

        private Integer userId;

        private Integer money;

        private long createTime;
    }

}
