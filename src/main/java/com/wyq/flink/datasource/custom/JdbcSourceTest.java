package com.wyq.flink.datasource.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.*;

public class JdbcSourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStreamSource<Person> ds = env.addSource(new JdbcSource());
        ds.print();
        env.execute();

    }

    public static class JdbcSource extends RichParallelSourceFunction<Person> {

        @Override
        public void run(SourceContext<Person> sourceContext) throws Exception {
            Connection connection = DriverManager.getConnection("jdbc:mysql://192.168.1.11:3306/test", "root", "123456");
            while (true) {
                PreparedStatement preparedStatement = connection.prepareStatement("select * from wyq");
                ResultSet resultSet = preparedStatement.executeQuery();
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    int age = resultSet.getInt("age");
                    sourceContext.collect(new Person(id, name, age));
                }
                Thread.sleep(5000);
            }

        }

        @Override
        public void cancel() {

        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Person {
        private int id;

        private String name;

        private int age;
    }

}
