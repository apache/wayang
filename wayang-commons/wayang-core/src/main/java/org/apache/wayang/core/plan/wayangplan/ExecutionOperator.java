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

package org.apache.wayang.core.plan.wayangplan;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.costs.LoadProfile;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.platform.lineage.LazyExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.logging.log4j.LogManager;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * An execution operator is handled by a certain platform.
 */
public interface ExecutionOperator extends ElementaryOperator {

    /**
     * @return the platform that can run this operator
     */
    Platform getPlatform();

    /**
     * @return a copy of this instance; it's {@link Slot}s will not be connected
     */
    ExecutionOperator copy();

    /**
     * @return this instance or, if it was derived via {@link #copy()}, the original instance
     */
    ExecutionOperator getOriginal();

    /**
     * Developers of {@link ExecutionOperator}s can provide a default {@link LoadProfileEstimator} via this method.
     *
     * @param configuration in which the {@link LoadProfile} should be estimated.
     * @return an {@link Optional} that might contain the {@link LoadProfileEstimator} (but {@link Optional#empty()}
     * by default)
     */
    default Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        Collection<String> configurationKeys = this.getLoadProfileEstimatorConfigurationKeys();
        LoadProfileEstimator mainEstimator = createLoadProfileEstimators(configuration, configurationKeys);
        return Optional.ofNullable(mainEstimator);
    }

    /**
     * Creates a {@link LoadProfileEstimator} according to the {@link Configuration}.
     *
     * @param configuration     the {@link Configuration}
     * @param configurationKeys keys for the specification within the {@link Configuration}
     * @return the {@link LoadProfileEstimator} or {@code null} if none could be created
     */
    static LoadProfileEstimator createLoadProfileEstimators(Configuration configuration, Collection<String> configurationKeys) {
        LoadProfileEstimator mainEstimator = null;
        for (String configurationKey : configurationKeys) {
            final LoadProfileEstimator loadProfileEstimator = LoadProfileEstimators.createFromSpecification(configurationKey, configuration);
            if (mainEstimator == null) {
                mainEstimator = loadProfileEstimator;
            } else {
                mainEstimator.nest(loadProfileEstimator);
            }
        }
        return mainEstimator;
    }

    /**
     * Provide the {@link Configuration} keys for the {@link LoadProfileEstimator} specification of this instance.
     *
     * @return the {@link Configuration} keys
     */
    default Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        final String singleKey = this.getLoadProfileEstimatorConfigurationKey();
        return singleKey == null ?
                Collections.emptyList() :
                Collections.singletonList(singleKey);
    }

    /**
     * @deprecated Use {@link #getLoadProfileEstimatorConfigurationKeys()}
     */
    @Deprecated
    default String getLoadProfileEstimatorConfigurationKey() {
        return null;
    }

    /**
     * Provides a base key for configuring input and output limits.
     * @return the limit base key or {@code null}
     */
    default String getLimitBaseKey() {
        // By default, try to infer the key.
        String loadProfileEstimatorConfigurationKey = this.getLoadProfileEstimatorConfigurationKey();
        if (loadProfileEstimatorConfigurationKey != null && loadProfileEstimatorConfigurationKey.endsWith(".load")) {
            return loadProfileEstimatorConfigurationKey
                    .substring(0, loadProfileEstimatorConfigurationKey.length() - 5)
                    .concat(".limit");
        }
        return null;
    }

    /**
     * Tells whether this instance should not be executed on the face of the given
     * {@link OptimizationContext.OperatorContext}. For instance, when this instance
     * might not be able to handle the amount of data.
     *
     * @param operatorContext an {@link OptimizationContext.OperatorContext} within which this instance might be
     *                        executed
     * @return whether this instance should <b>not</b> be used for execution
     */
    default boolean isFiltered(OptimizationContext.OperatorContext operatorContext) {
        assert operatorContext.getOperator() == this;

        // By default, we look for configuration keys formed like this:
        //   <my.operator.limit.key>.<input/output name>
        // If such a key exists, we compare it to values in the operatorContext.
        String limitBaseKey = this.getLimitBaseKey();
        if (limitBaseKey != null) {
            Configuration configuration = operatorContext.getOptimizationContext().getConfiguration();

            // Check the inputs.
            for (InputSlot<?> input : this.getAllInputs()) {
                String key = limitBaseKey + "." + input.getName();
                long limit = configuration.getLongProperty(key, -1);
                if (limit >= 0) {
                    CardinalityEstimate cardinality = operatorContext.getInputCardinality(input.getIndex());
                    if (cardinality != null && cardinality.getGeometricMeanEstimate() > limit) return true;
                }
            }

            // Check the outputs.
            for (OutputSlot<?> output : this.getAllOutputs()) {
                String key = limitBaseKey + "." + output.getName();
                long limit = configuration.getLongProperty(key, -1);
                if (limit >= 0) {
                    CardinalityEstimate cardinality = operatorContext.getOutputCardinality(output.getIndex());
                    if (cardinality != null && cardinality.getGeometricMeanEstimate() > limit) return true;
                }
            }
        }

        return false;
    }

    /**
     * Display the supported {@link Channel}s for a certain {@link InputSlot}.
     *
     * @param index the index of the {@link InputSlot}
     * @return an {@link List} of {@link Channel}s' {@link Class}es, ordered by their preference of use
     */
    List<ChannelDescriptor> getSupportedInputChannels(int index);

    /**
     * Display the supported {@link Channel}s for a certain {@link OutputSlot}.
     *
     * @param index the index of the {@link OutputSlot}
     * @return an {@link List} of {@link Channel}s' {@link Class}es, ordered by their preference of use
     * @see #getOutputChannelDescriptor(int)
     * @deprecated {@link ExecutionOperator}s should only support a single {@link ChannelDescriptor}
     */
    @Deprecated
    List<ChannelDescriptor> getSupportedOutputChannels(int index);

    /**
     * Display the {@link Channel} used to implement a certain {@link OutputSlot}.
     *
     * @param index index of the {@link OutputSlot}
     * @return the {@link ChannelDescriptor} for the mentioned {@link Channel}
     */
    default ChannelDescriptor getOutputChannelDescriptor(int index) {
        final List<ChannelDescriptor> supportedOutputChannels = this.getSupportedOutputChannels(index);
        assert !supportedOutputChannels.isEmpty() : String.format("No supported output channels for %s.", this);
        if (supportedOutputChannels.size() > 1) {
            LogManager.getLogger(this.getClass()).warn("Treat {} as the only supported channel for {}.",
                    supportedOutputChannels.get(0), this.getOutput(index)
            );
        }
        return supportedOutputChannels.get(0);
    }

    /**
     * Create output {@link ChannelInstance}s for this instance.
     *
     * @param task                    the {@link ExecutionTask} in which this instance is being wrapped
     * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for this instance
     * @param inputChannelInstances   the input {@link ChannelInstance}s for the {@code task}
     * @return
     */
    default ChannelInstance[] createOutputChannelInstances(Executor executor,
                                                           ExecutionTask task,
                                                           OptimizationContext.OperatorContext producerOperatorContext,
                                                           List<ChannelInstance> inputChannelInstances) {

        assert task.getOperator() == this;
        ChannelInstance[] channelInstances = new ChannelInstance[task.getNumOuputChannels()];
        for (int outputIndex = 0; outputIndex < channelInstances.length; outputIndex++) {
            final Channel outputChannel = task.getOutputChannel(outputIndex);
            final ChannelInstance outputChannelInstance = outputChannel.createInstance(executor, producerOperatorContext, outputIndex);
            channelInstances[outputIndex] = outputChannelInstance;
        }
        return channelInstances;
    }

    /**
     * Models eager execution by marking all {@link LazyExecutionLineageNode}s as executed and collecting all marked ones.
     *
     * @param inputs          the input {@link ChannelInstance}s
     * @param outputs         the output {@link ChannelInstance}s
     * @param operatorContext the executed {@link OptimizationContext.OperatorContext}
     * @return the executed {@link OptimizationContext.OperatorContext} and produced {@link ChannelInstance}s
     */
    static Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> modelEagerExecution(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            OptimizationContext.OperatorContext operatorContext) {

        final ExecutionLineageNode executionLineageNode = new ExecutionLineageNode(operatorContext);
        executionLineageNode.addAtomicExecutionFromOperatorContext();
        LazyExecutionLineageNode.connectAll(inputs, executionLineageNode, outputs);

        final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> collectors;
        if (outputs.length == 0) {
            collectors = executionLineageNode.collectAndMark();
        } else {
            collectors = new Tuple<>(new LinkedList<>(), new LinkedList<>());
            for (ChannelInstance output : outputs) {
                output.getLineage().collectAndMark(collectors.getField0(), collectors.getField1());
            }
        }
        return collectors;
    }

    /**
     * Models eager execution by marking all {@link LazyExecutionLineageNode}s as executed and collecting all marked ones.
     * However, the output {@link ChannelInstance}s are not yet produced.
     *
     * @param inputs          the input {@link ChannelInstance}s
     * @param outputs         the output {@link ChannelInstance}s
     * @param operatorContext the executed {@link OptimizationContext.OperatorContext}
     * @return the executed {@link OptimizationContext.OperatorContext} and produced {@link ChannelInstance}s
     */
    static Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> modelQuasiEagerExecution(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            OptimizationContext.OperatorContext operatorContext) {

        final ExecutionLineageNode executionLineageNode = new ExecutionLineageNode(operatorContext);
        executionLineageNode.addAtomicExecutionFromOperatorContext();
        LazyExecutionLineageNode.connectAll(inputs, executionLineageNode, outputs);

        return executionLineageNode.collectAndMark();
    }

    /**
     * Models lazy execution by not marking any {@link LazyExecutionLineageNode}s.
     *
     * @param inputs          the input {@link ChannelInstance}s
     * @param outputs         the output {@link ChannelInstance}s
     * @param operatorContext the executed {@link OptimizationContext.OperatorContext}
     * @return the executed {@link OptimizationContext.OperatorContext} and produced {@link ChannelInstance}s
     */
    static Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>>
    modelLazyExecution(ChannelInstance[] inputs,
                       ChannelInstance[] outputs,
                       OptimizationContext.OperatorContext operatorContext) {

        final ExecutionLineageNode executionLineageNode = new ExecutionLineageNode(operatorContext);
        executionLineageNode.addAtomicExecutionFromOperatorContext();
        LazyExecutionLineageNode.connectAll(inputs, executionLineageNode, outputs);

        return new Tuple<>(Collections.emptyList(), Collections.emptyList());
    }

}
