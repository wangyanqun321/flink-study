package com.wyq.flink.state;

import org.apache.commons.lang.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ValueStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Tuple2<String, Long>> ds = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L)
        );

        SingleOutputStreamOperator<Tuple2<String, Long>> result1 = ds.keyBy(t -> t.f0).maxBy(1);
        //result1.print();

        ds.keyBy(t -> t.f0).map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {

            private ValueState<Long> maxValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Long> valueStateDescriptor =
                        new ValueStateDescriptor<>("maxValue", Long.class);
                maxValueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public Tuple2<String, Long> map(Tuple2<String, Long> value) throws Exception {
                if(maxValueState.value() == null || value.f1 > maxValueState.value()) {
                    maxValueState.update(value.f1);
                }
                return Tuple2.of(value.f0, maxValueState.value());
            }
        }).print();
        env.execute();

    }

}
