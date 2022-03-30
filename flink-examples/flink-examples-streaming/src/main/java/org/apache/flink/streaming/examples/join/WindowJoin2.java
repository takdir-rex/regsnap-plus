/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.join;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.join.WindowJoinSampleData.GradeSource;
import org.apache.flink.streaming.examples.join.WindowJoinSampleData.SalarySource;

import java.io.File;

/**
 * Example illustrating a windowed stream join between two data streams.
 *
 * <p>The example works on two input streams with pairs (name, grade) and (name, salary)
 * respectively. It joins the steams based on "name" within a configurable window.
 *
 * <p>The example uses a built-in sample data generator that generates the steams of pairs at a
 * configurable rate.
 */
@SuppressWarnings("serial")
public class WindowJoin2 {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 2000);
        final long rate = params.getLong("rate", 3L);

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
        System.out.println(
                "To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]");

        Configuration conf = new Configuration();
        final File checkpointDir = new File("C:\\Users\\Takdir\\tmp\\checkpoint");
        final File savepointDir = new File("C:\\Users\\Takdir\\tmp\\savepoint");

        conf.setString(StateBackendOptions.STATE_BACKEND, "filesystem");
        conf.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        conf.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //setting for concurrent partial snapshots
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(0);
        env.getCheckpointConfig().setCheckpointTimeout(60000); //checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0); //shutdown job if failure found
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(Integer.MAX_VALUE); //as much as possible

        env.disableOperatorChaining();


//                env.enableCheckpointing(2000);
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // create the data sources for both grades and salaries
        DataStream<Tuple2<String, Integer>> grades =
                GradeSource.getSource(env, rate)
                        .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create()).uid("Op1a").name("WM grades").snapshotGroup("snap1");

        DataStream<Tuple2<String, Integer>> salaries =
                SalarySource.getSource(env, rate)
                        .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create()).uid("Op1b").name("WM salaries").snapshotGroup("snap1");

        // run the actual window join program
        // for testability, this functionality is in a separate method.
        DataStream<Tuple3<String, Integer, Integer>> joinedStream =
                runWindowJoin(grades, salaries, windowSize);

        ((SingleOutputStreamOperator) joinedStream).uid("join").name("Join").snapshotGroup("snap2");

        // print the results with a single thread, rather than in parallel
        joinedStream.print().setParallelism(1).uid("Sink").name("Sink").snapshotGroup("snap2");

//        System.out.println(env.getExecutionPlan());

        // execute program
        env.execute("Windowed Join Example");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowJoin(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {

        return grades.join(salaries)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(
                        new JoinFunction<
                                Tuple2<String, Integer>,
                                Tuple2<String, Integer>,
                                Tuple3<String, Integer, Integer>>() {

                            @Override
                            public Tuple3<String, Integer, Integer> join(
                                    Tuple2<String, Integer> first, Tuple2<String, Integer> second) {
                                return new Tuple3<String, Integer, Integer>(
                                        first.f0, first.f1, second.f1);
                            }
                        });
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    /**
     * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
     * In a real use case you should use proper timestamps and an appropriate {@link
     * WatermarkStrategy}.
     */
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}
