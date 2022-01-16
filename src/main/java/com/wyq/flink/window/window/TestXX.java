package com.wyq.flink.window.window;

public class TestXX {

    public static void main(String[] args) {
        long timestamp = 1642262400000L;
        long interval = 7 * 60 * 1000;
        long start = timestamp - (timestamp % interval);
        long nextFireTimestamp = start + interval;
        System.out.println(start);
        System.out.println(nextFireTimestamp);
    }

}
