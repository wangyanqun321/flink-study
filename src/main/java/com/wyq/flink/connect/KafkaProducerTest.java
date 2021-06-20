package com.wyq.flink.connect;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> ds = env.socketTextStream("localhost", 9999);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                "test-topic",                  // 目标 topic
                new SimpleStringSchema(),
                properties                  // producer 配置
                //FlinkKafkaProducer.Semantic.EXACTLY_ONCE,
                ); // 容错
        ds.addSink(myProducer);
        env.execute("kafka sink");
    }

}
