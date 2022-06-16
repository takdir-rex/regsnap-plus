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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A failover strategy that proposes to restart involved snapshot group when a vertex fails. */
public class RestartSnapshotGroupFailoverStrategy implements FailoverStrategy {

    /** The log object used for debugging. */
    private static final Logger LOG =
            LoggerFactory.getLogger(RestartSnapshotGroupFailoverStrategy.class);

    private final SchedulingTopology topology;

    public RestartSnapshotGroupFailoverStrategy(final SchedulingTopology topology) {
        this.topology = checkNotNull(topology);
    }

    /**
     * Returns all vertices on any task failure.
     *
     * @param executionVertexId ID of the failed task
     * @param cause cause of the failure
     * @return set of IDs of vertices to restart
     */
    @Override
    public Set<ExecutionVertexID> getTasksNeedingRestart(
            ExecutionVertexID executionVertexId, Throwable cause) {
        LOG.info("Calculating tasks to restart to recover the failed task {}.", executionVertexId);
        final String failedSnapshotGroup = topology.getVertex(executionVertexId).getSnapshotGroup();
        return IterableUtils.toStream(topology.getVertices())
                .filter(v -> v.getSnapshotGroup() == failedSnapshotGroup)
                .map(SchedulingExecutionVertex::getId)
                .collect(Collectors.toSet());
    }

    /** The factory to instantiate {@link RestartSnapshotGroupFailoverStrategy}. */
    public static class Factory implements FailoverStrategy.Factory {

        @Override
        public FailoverStrategy create(
                final SchedulingTopology topology,
                final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

            return new RestartSnapshotGroupFailoverStrategy(topology);
        }
    }
}
