package org.apache.flink.streaming.examples.join;

import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;

public class FailingSink<T> implements SinkFunction<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FailingSink.class);

    @Override
    public void invoke(T value, Context context) throws Exception {
        LOG.info(value.toString());
        if(Calendar.getInstance().get(Calendar.SECOND) == 0){
            throw new Exception("Simulate failed");
        }
    }
}
