package com.wyq.flink.window.evictor;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CountEvictorDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Date>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple3<String, Integer, Date>> collector) throws Exception {
                        if (StringUtils.isNullOrWhitespaceOnly(line)) {
                            return;
                        }
                        String[] words = line.split(",");
                        Map<String, Integer> localWc = new HashMap<>();
                        Arrays.stream(words).forEach(word -> localWc.put(word, localWc.getOrDefault(word, 0) + 1));
                        localWc.forEach((k, v) -> {
                            Tuple3<String, Integer, Date> tuple3 = Tuple3.of(k, v, new Date());
                            //System.out.println(tuple3);
                            collector.collect(tuple3);
                        });
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .evictor(CountEvictor.of(2))
                .sum(1)
                .print();
        // hello,world
        // hello,java,hello,flink
        // hello,flink
        env.execute("test count window");
    }

}
