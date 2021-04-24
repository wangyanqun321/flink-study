package com.wyq.flink.sink;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Student> ds = env.fromElements(new Student(8, "lisi"));
        ds.addSink(new MySqlSink());
        env.execute();
    }

    @Data
    @AllArgsConstructor
    public static class Student {
        private Integer id;

        private String name;

    }

    public static class MySqlSink extends RichSinkFunction<Student> {

        @Override
        public void open(Configuration parameters) throws Exception {

        }

        @Override
        public void close() throws Exception {

        }

        @Override
        public void invoke(Student value, Context context) throws Exception {
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/wyq", "root", "123456");
            PreparedStatement pst = connection.prepareStatement("insert into test(id, name) values(?, ?)");
            pst.setInt(1, value.id);
            pst.setString(2, value.name);
            pst.execute();
        }

    }

}
