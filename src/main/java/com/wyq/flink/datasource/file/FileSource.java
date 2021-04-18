package com.wyq.flink.datasource.file;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSource {

    /**
     * 从文件、目录、压缩包中读取
     * 可以是本地文件，hdfs文件
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<String> ds1 = env.readTextFile("data/input/dir");
        ds1.print();
        DataStream<String> ds2 = env.readTextFile("data/input/words.txt");
        ds2.print();
        DataStream<String> ds3 = env.readTextFile("data/input/data.gz");
        ds3.print();
        DataStream<String> ds4 = env.readTextFile("hdfs://192.168.1.13:8020/wyq/hello.txt");
        ds4.print();
        env.execute();
    }

}
