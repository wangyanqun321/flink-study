package com.wyq.flink.practice;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class DoubleElevenDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.addSource(new MySource())
                  .keyBy(t -> t.f0)
                  .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                  .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1)))
                  .aggregate(new PriceAggregate(),
                          new WindowResult())
                  .keyBy(t -> t.f2)
                  .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                  .process(new FinalResultWindow())
                  .print();
          env.execute();
    }

    public static class MySource extends RichParallelSourceFunction<Tuple2<String, Double>> {

        private boolean flag = true;

        private String[] categorys = {"女装","男装", "图书", "家电", "洗护", "美妆", "运动", "游戏", "户外"};

        private Random random = new Random();

        @Override
        public void run(SourceContext<Tuple2<String, Double>> sourceContext) throws Exception {
            while (flag) {
                int index = random.nextInt(categorys.length);
                Double price = random.nextDouble() * 100;
                sourceContext.collect(Tuple2.of(categorys[index], price));
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    public static class PriceAggregate implements AggregateFunction<Tuple2<String, Double>, Double, Double> {

        @Override
        public Double createAccumulator() {
            return 0D;
        }

        @Override
        public Double add(Tuple2<String, Double> value, Double aDouble) {
            return value.f1 + aDouble;
        }

        @Override
        public Double getResult(Double aDouble) {
            return aDouble;
        }

        @Override
        public Double merge(Double aDouble, Double acc1) {
            return aDouble + acc1;
        }
    }

    private static class WindowResult implements WindowFunction<Double, Tuple3<String, Double, String>, String, TimeWindow> {


        @Override
        public void apply(String key, TimeWindow window, Iterable<Double> input, Collector<Tuple3<String, Double, String>> out) throws Exception {
            Double price = input.iterator().next();
            out.collect(Tuple3.of(key, price, new Date().toLocaleString()));
        }
    }

    private static class FinalResultWindow extends ProcessWindowFunction<Tuple3<String, Double, String>, Object, String, TimeWindow> {

        @Override
        public void process(String dateTime, Context context, Iterable<Tuple3<String, Double, String>> elements, Collector<Object> out) throws Exception {
            Queue<Tuple3<String, Double, String>> queue = new PriorityQueue<>(3, (c1, c2) -> c1.f1 >= c2.f1 ? 1 : -1);
            double total = 0d;
            for(Tuple3<String, Double, String> t : elements) {
                total = total + t.f1;
                if(queue.size() < 3) {
                    queue.add(t);
                }else {
                    if (t.f1 >= queue.peek().f1) {
                        queue.poll();
                        queue.add(t);
                    }
                }

            }
            List<String> collect = queue.stream().sorted((c1, c2) -> c1.f1 >= c2.f1 ? -1 : 1)
                    .map(c -> "分类：" + c.f0 + "金额：" + c.f1)
                    .collect(Collectors.toList());
            System.out.println("时间：" + dateTime + "总金额：" + total);
            System.out.println("top3: " + collect);

        }
    }
}
