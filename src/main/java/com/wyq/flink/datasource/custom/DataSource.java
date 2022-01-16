package com.wyq.flink.datasource.custom;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        while (isRunning) {
            Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000L + 500);
            String key = "类别" + "A" + random.nextInt(3);
            Integer value = random.nextInt(10) + 1;
            ctx.collect(Tuple2.of(key, value));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new DataSource())
                .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
                        System.out.println(input);
                        return input;
                    }
                })
                .keyBy(t -> "")
                .addSink(new SinkFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                        System.out.println("key: " + value.f0 + ", value: " + value.f1);
                    }
                });
        env.execute("test");
    }
}
