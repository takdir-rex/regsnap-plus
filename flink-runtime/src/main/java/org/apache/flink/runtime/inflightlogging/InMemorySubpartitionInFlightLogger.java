/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional debugrmation
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.inflightlogging;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class InMemorySubpartitionInFlightLogger implements InFlightLog {

    private static final Logger LOG =
            LoggerFactory.getLogger(InMemorySubpartitionInFlightLogger.class);

    private final SortedMap<Long, List<Buffer>> slicedLog;
    private BufferPool inFlightBufferPool;

    public InMemorySubpartitionInFlightLogger() {
        slicedLog = new TreeMap<>();
    }

    public synchronized void log(Buffer buffer, long epochID, boolean isFinished) {
        List<Buffer> epochLog =
                slicedLog.computeIfAbsent(
                        epochID,
                        k -> {
                            return new LinkedList<>();
                        });
        epochLog.add(buffer);
        //		LOG.debug("Logged a new buffer for epoch {}", epochID);
    }

    @Override
    public synchronized InFlightLogIterator<Buffer> getInFlightIterator(
            long startEpochID, int ignoreBuffers) {
        // The lower network stack recycles buffers, so for each replay, we must increase reference
        // counts
        increaseReferenceCountsUnsafe(startEpochID);
        ReplayIterator replayIterator = new ReplayIterator(startEpochID, slicedLog);

        for (int i = 0; i < ignoreBuffers; i++) replayIterator.next().recycleBuffer();

        return replayIterator;
    }

    @Override
    public synchronized void close() {
        for (List<Buffer> epoch : slicedLog.values()) {
            for (Buffer b : epoch) {
                while (!b.isRecycled()) {
                    b.recycleBuffer();
                }
            }
            epoch.clear();
        }
        slicedLog.clear();
    }

    @Override
    public synchronized void prune(long epochID) {
        slicedLog.headMap(epochID).values().forEach(buffers -> {
            for (Buffer b : buffers) {
                while (!b.isRecycled()) {
                    b.recycleBuffer();
                }
            }
            buffers.clear();
        });

        slicedLog.headMap(epochID).forEach((id, buffers) -> {
            for (Buffer b : buffers) {
                while (!b.isRecycled()) {
                    b.recycleBuffer();
                }
            }
            buffers.clear();
            slicedLog.remove(id);
            LOG.debug("CheckpointId {} was pruned from inflight logs", id);
        });
    }

    private void increaseReferenceCountsUnsafe(Long epochID) {
        for (List<Buffer> epoch : slicedLog.tailMap(epochID).values())
            for (Buffer buffer : epoch) buffer.retainBuffer();
    }

    public static class ReplayIterator extends InFlightLogIterator<Buffer> {
        private long startKey;
        private long currentKey;
        private ListIterator<Buffer> currentIterator;
        private SortedMap<Long, List<Buffer>> logToReplay;
        private int numberOfBuffersLeft;

        public ReplayIterator(long epochToStartFrom, SortedMap<Long, List<Buffer>> fullLog) {

            // Failed at checkpoint x, so we replay starting at epoch x
            this.startKey = epochToStartFrom;
            this.currentKey = epochToStartFrom;
            this.logToReplay = fullLog.tailMap(epochToStartFrom);
            LOG.info(" Getting iterator starting  from checkpointId {}", currentKey);
            //			LOG.debug(" Getting iterator starting  from epochID {} with log state {} and sublog
            // state {}", currentKey, representLogAsString(fullLog),
            // representLogAsString(this.logToReplay));
            if (this.logToReplay.get(currentKey) != null) {
                this.currentIterator = this.logToReplay.get(currentKey).listIterator();
                this.numberOfBuffersLeft =
                        this.logToReplay.values().stream()
                                .mapToInt(List::size)
                                .sum(); // add up the sizes
            } else {
                this.currentIterator = null;
                this.numberOfBuffersLeft = 0;
            }
            //			LOG.debug("State of log: {}\nlog tailmap {}\nIterator creation {}: ",
            // representLogAsString(fullLog), representLogAsString(this.logToReplay),
            // this.toString());
        }

        private void advanceToNextNonEmptyIteratorIfNeeded() {
            while (currentIterator != null
                    && !currentIterator.hasNext()
                    && currentKey < logToReplay.lastKey()) {
                this.currentIterator = logToReplay.get(++currentKey).listIterator();
                while (currentIterator.hasPrevious()) currentIterator.previous();
            }
        }

        private void advanceToPreviousNonEmptyIteratorIfNeeded() {
            while (currentIterator != null
                    && !currentIterator.hasPrevious()
                    && currentKey > logToReplay.firstKey()) {
                this.currentIterator = logToReplay.get(--currentKey).listIterator();
                while (currentIterator.hasNext())
                    currentIterator.next(); // fast forward the iterator
            }
        }

        @Override
        public boolean hasNext() {
            advanceToNextNonEmptyIteratorIfNeeded();
            return currentIterator != null && currentIterator.hasNext();
        }

        @Override
        public Buffer next() {
            advanceToNextNonEmptyIteratorIfNeeded();
            Buffer toReturn = currentIterator.next();
            numberOfBuffersLeft--;
            advanceToNextNonEmptyIteratorIfNeeded();
            return toReturn;
        }

        @Override
        public Buffer peekNext() {
            advanceToNextNonEmptyIteratorIfNeeded();
            Buffer toReturn = currentIterator.next();
            currentIterator.previous();
            advanceToNextNonEmptyIteratorIfNeeded();
            return toReturn;
        }

        @Override
        public void close() {
            while (this.hasNext()) this.next().recycleBuffer();
        }

        @Override
        public int numberRemaining() {
            return numberOfBuffersLeft;
        }

        @Override
        public long getEpoch() {
            return currentKey;
        }

        @Override
        public String toString() {
            return "ReplayIterator{"
                    + "startKey="
                    + startKey
                    + ", currentKey="
                    + currentKey
                    + ", currentIterator="
                    + currentIterator
                    + ", logToReplay="
                    + representLogAsString(logToReplay)
                    + ", numberOfBuffersLeft="
                    + numberOfBuffersLeft
                    + ", reduceCount="
                    + logToReplay.values().stream().mapToInt(List::size).sum()
                    + ", lists="
                    + logToReplay.values().stream()
                            .map(
                                    l ->
                                            "["
                                                    + l.stream()
                                                            .map(x -> "*")
                                                            .collect(Collectors.joining(", "))
                                                    + "]")
                            .collect(Collectors.joining("; "))
                    + '}';
        }
    }

    private static String representLogAsString(SortedMap<Long, List<Buffer>> toStringify) {
        return "{"
                + toStringify.entrySet().stream()
                        .map(
                                e ->
                                        e.getKey()
                                                + " -> "
                                                + "["
                                                + e.getValue().stream()
                                                        .map(x -> "*")
                                                        .collect(Collectors.joining(", "))
                                                + "]")
                        .collect(Collectors.joining(", "))
                + "}";
    }
}
