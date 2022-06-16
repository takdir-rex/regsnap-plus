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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingResultPartition;
import org.apache.flink.runtime.io.network.partition.BoundedBlockingSubpartitionType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/** An {@link InputGate} with a specific index. */
public abstract class IndexedInputGate extends InputGate implements CheckpointableInput {

    private ResultSubpartition backupPartition = null;

    /** Returns the index of this input gate. Only supported on */
    public abstract int getGateIndex();

    /** Returns the list of channels that have not received EndOfPartitionEvent. */
    public abstract List<InputChannelInfo> getUnfinishedChannels();

    @Override
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        for (int index = 0, numChannels = getNumberOfInputChannels();
                index < numChannels;
                index++) {
            getChannel(index).checkpointStarted(barrier);
        }
    }

    @Override
    public void checkpointStopped(long cancelledCheckpointId) {
        for (int index = 0, numChannels = getNumberOfInputChannels();
                index < numChannels;
                index++) {
            getChannel(index).checkpointStopped(cancelledCheckpointId);
        }
    }

    @Override
    public int getInputGateIndex() {
        return getGateIndex();
    }

    @Override
    public void blockConsumption(InputChannelInfo channelInfo) {
        // Unused. Network stack is blocking consumption automatically by revoking credits.
    }

    @Override
    public void convertToPriorityEvent(int channelIndex, int sequenceNumber) throws IOException {
        getChannel(channelIndex).convertToPriorityEvent(sequenceNumber);
    }

    public abstract int getBuffersInUseCount();

    public abstract void announceBufferSize(int bufferSize);

    public void setupBackupPartition() {
        try {
            String pathname = "C:\\Users\\Takdir\\tmp\\inputlog\\gate_" + getGateIndex();
            Files.createDirectories(Paths.get(pathname));
            BoundedBlockingResultPartition parent =
                    (BoundedBlockingResultPartition)
                            new ResultPartitionBuilder()
                                    .setResultPartitionType(ResultPartitionType.BLOCKING_PERSISTENT)
                                    .setBoundedBlockingSubpartitionType(
                                            BoundedBlockingSubpartitionType.FILE)
                                    .setResultPartitionIndex(getGateIndex())
                                    .setFileChannelManager(
                                            new FileChannelManagerImpl(
                                                    new String[]{
                                                            new File(pathname).toString()},
                                                    "data"))
                                    .setNetworkBufferSize(32 * 1024)
                                    .build();
            backupPartition = parent.getAllPartitions()[0];
        } catch (IOException e) {
            System.err.println(e);
        }
    }

    protected BufferOrEvent backup(BufferOrEvent bufferOrEvent){
        if(backupPartition != null){
            //persist to the storage
            try {
                backupPartition.add(new BufferConsumer(bufferOrEvent.getBuffer(), bufferOrEvent.getSize()));
            } catch (IOException e) {
                System.err.println(e);
            }
        }
        return bufferOrEvent;
    }
}
