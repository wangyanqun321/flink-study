package com.wyq.flink.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamUnionDemo {

    /**
     * 合并流：
     * 1. 可以合并多个
     * 2. 只能合并同类型
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.fromElements("hello", "flink", "java");
        DataStreamSource<String> ds2 = env.fromElements("hello", "flink", "java");
        DataStream<String> union = ds1.union(ds2);
        union.print();
        env.execute();
    }

}
