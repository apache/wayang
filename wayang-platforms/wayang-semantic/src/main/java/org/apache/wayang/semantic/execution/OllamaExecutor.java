/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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

package org.apache.wayang.semantic.execution;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.PartialExecution;
import org.apache.wayang.core.platform.PushExecutorTemplate;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Formats;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.semantic.operators.OllamaExecutionOperator;
import org.apache.wayang.semantic.platform.OllamaPlatform;

/**
 * {@link Executor} implementation for the {@link OllamaPlatform}.
 */
public class OllamaExecutor extends PushExecutorTemplate {

    private final OllamaPlatform platform;

    public OllamaExecutor(final OllamaPlatform platform, final Job job) {
        super(job);
        this.platform = platform;
    }

    @Override
    public OllamaPlatform getPlatform() {
        return this.platform;
    }

    @Override
    protected Tuple<List<ChannelInstance>, PartialExecution> execute(
            final ExecutionTask task,
            final List<ChannelInstance> inputChannelInstances,
            final OptimizationContext.OperatorContext producerOperatorContext,
            final boolean isRequestEagerExecution
    ) {
        final ChannelInstance[] outputChannelInstances = task.getOperator().createOutputChannelInstances(
                this, task, producerOperatorContext, inputChannelInstances
        );

        final Collection<ExecutionLineageNode> executionLineageNodes;
        final Collection<ChannelInstance> producedChannelInstances;
        this.job.reportProgress(task.getOperator().getName(), 50);
        final long startTime = System.currentTimeMillis();
        try {
            final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> results =
                    cast(task.getOperator()).evaluate(
                            toArray(inputChannelInstances),
                            outputChannelInstances,
                            this,
                            producerOperatorContext
                    );
            executionLineageNodes = results.getField0();
            producedChannelInstances = results.getField1();
        } catch (Exception e) {
            throw new WayangException(String.format("Executing %s failed.", task), e);
        }
        final long endTime = System.currentTimeMillis();
        final long executionDuration = endTime - startTime;

        this.job.reportProgress(task.getOperator().getName(), 100);

        final PartialExecution partialExecution = this.createPartialExecution(executionLineageNodes, executionDuration);

        if (partialExecution == null && executionDuration > 10) {
            this.logger.warn("Execution of {} took suspiciously long ({}).", task, Formats.formatDuration(executionDuration));
        }

        this.registerMeasuredCardinalities(producedChannelInstances);

        if (isRequestEagerExecution && partialExecution == null) {
            this.logger.info("{} was not executed eagerly as requested.", task);
        }

        return new Tuple<>(Arrays.asList(outputChannelInstances), partialExecution);
    }

    private static OllamaExecutionOperator cast(final ExecutionOperator executionOperator) {
        return (OllamaExecutionOperator) executionOperator;
    }

    private static ChannelInstance[] toArray(final List<ChannelInstance> channelInstances) {
        final ChannelInstance[] array = new ChannelInstance[channelInstances.size()];
        return channelInstances.toArray(array);
    }
}
