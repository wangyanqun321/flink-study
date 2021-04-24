package com.wyq.flink.transformation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OutputTagDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> ds = env.fromSequence(1, 10);
        OutputTag<Long> oddTag = new OutputTag<>("奇数", TypeInformation.of(Long.TYPE));
        OutputTag<Long> evenTag = new OutputTag<>("偶数", TypeInformation.of(Long.TYPE));
        SingleOutputStreamOperator<Object> process = ds.process(new ProcessFunction<Long, Object>() {
            @Override
            public void processElement(Long aLong, Context context, Collector<Object> collector) throws Exception {
                if (aLong % 2 == 0) {
                    context.output(evenTag, aLong);
                } else {
                    context.output(oddTag, aLong);
                }
            }
        });

        process.getSideOutput(oddTag).print("奇数");
        process.getSideOutput(evenTag).print("偶数");
        env.execute();
    }
}
