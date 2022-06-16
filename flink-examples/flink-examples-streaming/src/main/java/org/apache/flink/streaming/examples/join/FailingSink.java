package org.apache.flink.streaming.examples.join;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FailingSink<T> implements SinkFunction<T> {

    private long statTime = System.currentTimeMillis();

    @Override
    public void invoke(T value, Context context) throws Exception {
        if(System.currentTimeMillis() - statTime > 10_000){
            throw new Exception("Simulate failed");
        }
    }
}
