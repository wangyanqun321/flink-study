package com.wyq.flink.datasource.custom;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class JdbcSourceTest {

    public static void main(String[] args) {
        
    }

    public static class JdbcSource extends RichParallelSourceFunction {

        @Override
        public void run(SourceContext sourceContext) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }

}
