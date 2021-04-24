package com.wyq.flink.connect;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JdbcConnectTest {

    /**
     * apache flink 官方连接器
     *
     * jdbc连接器
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.fromElements(Tuple2.of(10, "hello"))
                .addSink(JdbcSink.sink("insert into test(id, name) values(?, ?)", (pst, value) -> {
                    pst.setInt(1, value.f0);
                    pst.setString(2, value.f1);
                }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/wyq")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));
        env.execute();

    }

}
