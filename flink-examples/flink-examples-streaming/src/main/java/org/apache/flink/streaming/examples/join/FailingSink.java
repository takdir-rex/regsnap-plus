package org.apache.flink.streaming.examples.join;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Calendar;

public class FailingSink<T> implements SinkFunction<T> {

    @Override
    public void invoke(T value, Context context) throws Exception {
        System.out.println("Sink 2: " + value);
        if(Calendar.getInstance().get(Calendar.SECOND) == 0){
//            throw new Exception("Simulate failed");
        }
    }
}
