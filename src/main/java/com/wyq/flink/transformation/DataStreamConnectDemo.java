package com.wyq.flink.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class DataStreamConnectDemo {

    /**
     * 合并流：
     * 1. 只能合并单个
     * 2. 可以合并不同类型的流
     * @param args
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> ds1 = env.fromElements("hello", "flink", "java");
        DataStreamSource<Long> ds2 = env.fromSequence(1, 10);
        ConnectedStreams<String, Long> connect = ds1.connect(ds2);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String s) throws Exception {
                return s + "str";
            }

            @Override
            public String map2(Long value) throws Exception {
                return value + "int";
            }
        });
        map.print();
        env.execute();
    }
}
