package com.wyq.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RebalanceDemo {

    /**
     * 解决数据倾斜问题
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> ds = env.fromSequence(1, 100);
        SingleOutputStreamOperator<Long> filter = ds.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong > 10;
            }
        });
        filter.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(indexOfThisSubtask, 1);
            }
        }).keyBy(t -> t.f0)
                .sum(1)
                .print("data skew");

        filter.rebalance().map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long aLong) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(indexOfThisSubtask, 1);
            }
        }).keyBy(t -> t.f0)
                .sum(1)
                .print("rebalance");
        env.execute();
    }


}
