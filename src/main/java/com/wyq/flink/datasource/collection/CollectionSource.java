package com.wyq.flink.datasource.collection;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CollectionSource {

    /**
     * 一般用于构造数据
     * 可以是元素集合，集合，数字序列等
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<String> ds1 = env.fromElements("hello world", "hello flink", "hello elements");
        ds1.print();
        DataStream<String> ds2 = env.fromCollection(Lists.newArrayList("hello world", "hello flink", "hello collection"));
        ds2.print();
        DataStream<Long> ds3 = env.fromSequence(1, 10);
        ds3.print();
        env.execute();
    }

}
