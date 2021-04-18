package com.wyq.flink.example.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
bin/flink run -Dexecution.runtime-mode=BATCH -m yarn-cluster -yjm 1024 -ytm 1024 \
 -c com.wyq.flink.example.stream.WordCount examples/myjar/original-flink-study-1.0-SNAPSHOT.jar \
 --output hdfs://192.168.1.13:8020/wordcount/output2/xxx
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        String output;
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if(parameterTool.has("output")) {
            output = parameterTool.get("output");
        }else {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
            simpleDateFormat.applyPattern("yyyy-MM-dd_HH-mm-ss");
            String format = simpleDateFormat.format(new Date());
            output = "hdfs://192.168.1.13:8020/wordcount/output/" + format;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStream<String> dataSource = env.fromElements("hello java", "hello world", "hello flink", "study java flink");
        DataStream<Tuple2<String, Integer>> result = dataSource
                .flatMap((String line, Collector<String> collector) ->
                    Arrays.stream(line.split(" ")).forEach(collector::collect)).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0).sum(1);
        result.print();
        //result.writeAsText(output).setParallelism(1);

        env.execute();
    }

}
