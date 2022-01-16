package com.wyq.flink.window.trigger;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CountTriggerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        if (StringUtils.isNullOrWhitespaceOnly(line)) {
                            return;
                        }
                        String[] words = line.split(",");
                        Map<String, Integer> localWc = new HashMap<>();
                        Arrays.stream(words).forEach(word -> localWc.put(word, localWc.getOrDefault(word, 0) + 1));
                        localWc.forEach((k, v) -> collector.collect(Tuple2.of(k, v)));
                    }
                }).keyBy(t -> t.f0)
                .countWindow(3)
                .sum(1)
                .print();
        env.execute("test count window");
    }

}
