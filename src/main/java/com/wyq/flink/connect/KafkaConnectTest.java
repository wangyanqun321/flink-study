package com.wyq.flink.connect;

import groovy.lang.Tuple;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KafkaConnectTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        collector.collect(Tuple2.of(s, 1));
                    }
                })
                .partitionCustom(new Partitioner<String>() {
                    @Override
                    public int partition(String s, int i) {
                        if("flink".equals(s)) {
                            return 0;
                        }
                        return 1;
                    }
                }, t -> t.f0).print();
        env.execute();
    }
}
