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

package org.apache.wayang.core.optimizer.channels;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelDescriptor;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Default implementation of the {@link ChannelConversion}. Can be used without further subclassing.
 */
public class DefaultChannelConversion extends ChannelConversion {

    private final BiFunction<Channel, Configuration, ExecutionOperator> executionOperatorFactory;

    /**
     * For debug purposes.
     */
    private final String name;

    public DefaultChannelConversion(
            ChannelDescriptor sourceChannelDescriptor,
            ChannelDescriptor targetChannelDescriptor,
            Supplier<ExecutionOperator> executionOperatorFactory) {
        this(
                sourceChannelDescriptor,
                targetChannelDescriptor,
                executionOperatorFactory,
                "via " + executionOperatorFactory.get().getClass().getSimpleName()
        );
    }

    public DefaultChannelConversion(
            ChannelDescriptor sourceChannelDescriptor,
            ChannelDescriptor targetChannelDescriptor,
            Supplier<ExecutionOperator> executionOperatorFactory,
            String name) {
        this(
                sourceChannelDescriptor,
                targetChannelDescriptor,
                (sourceChannel, configuration) -> executionOperatorFactory.get(),
                name
        );
    }

    public DefaultChannelConversion(
            ChannelDescriptor sourceChannelDescriptor,
            ChannelDescriptor targetChannelDescriptor,
            BiFunction<Channel, Configuration, ExecutionOperator> executionOperatorFactory) {
        this(
                sourceChannelDescriptor,
                targetChannelDescriptor,
                executionOperatorFactory,
                "via " + executionOperatorFactory.getClass().getSimpleName()
        );
    }

    public DefaultChannelConversion(
            ChannelDescriptor sourceChannelDescriptor,
            ChannelDescriptor targetChannelDescriptor,
            BiFunction<Channel, Configuration, ExecutionOperator> executionOperatorFactory,
            String name) {
        super(sourceChannelDescriptor, targetChannelDescriptor);
        this.executionOperatorFactory = executionOperatorFactory;
        this.name = name;
    }

    @Override
    public Channel convert(Channel sourceChannel,
                           Configuration configuration,
                           Collection<OptimizationContext> optimizationContexts,
                           CardinalityEstimate optCardinality) {
        // Create the ExecutionOperator.
        final ExecutionOperator executionOperator = this.executionOperatorFactory.apply(sourceChannel, configuration);
        assert executionOperator.getNumInputs() <= 1 && executionOperator.getNumOutputs() <= 1;
        executionOperator.setAuxiliary(true);

        if (sourceChannel != null && sourceChannel.getProducerSlot() != null && executionOperator.getNumInputs() > 0) {
            org.apache.wayang.core.types.DataSetType<?> sourceChannelType = sourceChannel.getProducerSlot().getType();
            org.apache.wayang.core.types.DataSetType<?> inputType = executionOperator.getInput(0).getType();
            if (sourceChannelType != null && inputType != null && !inputType.isSupertypeOf(sourceChannelType)) {
                try {
                    java.lang.reflect.Method adaptTypeMethod = executionOperator.getClass().getMethod("adaptType", org.apache.wayang.core.types.DataSetType.class);
                    adaptTypeMethod.invoke(executionOperator, sourceChannelType);
                } catch (NoSuchMethodException e) {
                    throw new IllegalArgumentException(String.format(
                            "Cannot convert channel %s of type %s with %s of type %s: type mismatch.",
                            sourceChannel, sourceChannelType, executionOperator, inputType));
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "Cannot convert channel %s of type %s with %s of type %s: type mismatch.",
                            sourceChannel, sourceChannelType, executionOperator, inputType), e);
                }
            }
        }

        // Set up the Channels and the ExecutionTask.
        final ExecutionTask task = new ExecutionTask(executionOperator, 1, 1);
        sourceChannel.addConsumer(task, 0);
        final Channel outputChannel = task.initializeOutputChannel(0, configuration);
        sourceChannel.addSibling(outputChannel);
        setCardinalityAndTimeEstimates(sourceChannel, optimizationContexts, optCardinality, task);

        return outputChannel;
    }

    @Override
    public void update(Channel sourceChannel,
                       Channel targetChannel,
                       Collection<OptimizationContext> optimizationContexts,
                       CardinalityEstimate cardinality) {
        ExecutionTask conversionTask = targetChannel.getProducer();
        this.setCardinalityAndTimeEstimates(sourceChannel, optimizationContexts, cardinality, conversionTask);
    }

    /**
     * Update the key figure estimates for the given {@link ExecutionTask}.
     *
     * @param sourceChannel        provides the {@link CardinalityEstimate}
     * @param optimizationContexts in which the estimates should be updates; also provides the estimates for the {@code sourceChannel}
     * @param optCardinality       overrides the {@link CardinalityEstimate} or else {@code null}
     * @param task                 whose key figure estimates should be updated
     */
    private void setCardinalityAndTimeEstimates(Channel sourceChannel,
                                                Collection<OptimizationContext> optimizationContexts,
                                                CardinalityEstimate optCardinality,
                                                ExecutionTask task) {
        // Enrich the optimizationContexts.
        for (OptimizationContext optimizationContext : optimizationContexts) {
            final CardinalityEstimate cardinality = optCardinality == null ?
                    this.determineCardinality(sourceChannel, optimizationContext) :
                    optCardinality;
            this.setCardinalityAndTimeEstimate(task, optimizationContext, cardinality);
        }
    }

    /**
     * Try to extract the {@link CardinalityEstimate} for a {@link Channel} within an {@link OptimizationContext}.
     *
     * @param channel             whose {@link CardinalityEstimate} is sought
     * @param optimizationContext provides {@link CardinalityEstimate}s
     * @return the {@link CardinalityEstimate}
     */
    private CardinalityEstimate determineCardinality(Channel channel, OptimizationContext optimizationContext) {
        final ExecutionOperator sourceOperator = channel.getProducerOperator();
        final OptimizationContext.OperatorContext sourceOpCtx = optimizationContext.getOperatorContext(sourceOperator);
        assert sourceOpCtx != null : String.format("No OperatorContext found for %s.", sourceOperator);

        final OutputSlot<?> producerSlot = channel.getProducerSlot();
        if (producerSlot != null) {
            return sourceOpCtx.getOutputCardinality(producerSlot.getIndex());
        } else if (sourceOperator.getNumInputs() == 1) {
            return sourceOpCtx.getInputCardinality(0);
        }
        throw new IllegalStateException(String.format("Could not determine cardinality of %s.", channel));
    }

    /**
     * Sets the {@link CardinalityEstimate}s for a conversion {@link ExecutionTask} in a given {@link OptimizationContext}.
     *
     * @param conversionTask      the conversion {@link ExecutionTask} that should have at most one input and one output
     * @param optimizationContext stores the {@link CardinalityEstimate}
     * @param cardinality         the {@link CardinalityEstimate}
     */
    private void setCardinalityAndTimeEstimate(ExecutionTask conversionTask,
                                               OptimizationContext optimizationContext,
                                               CardinalityEstimate cardinality) {
        final ExecutionOperator operator = conversionTask.getOperator();
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(operator);

        if (operator.getNumInputs() > 0) {
            assert operator.getNumInputs() == 1;
            operatorContext.setInputCardinality(0, cardinality);
        }

        if (operator.getNumOutputs() > 0) {
            assert operator.getNumOutputs() == 1;
            operatorContext.setOutputCardinality(0, cardinality);
        }

        operatorContext.updateCostEstimate();
    }

    @Override
    public ProbabilisticDoubleInterval estimateConversionCost(CardinalityEstimate cardinality,
                                                              int numExecutions,
                                                              OptimizationContext optimizationContext) {
        // Create OperatorContext.
        final ExecutionOperator executionOperator = this.executionOperatorFactory.apply(null, optimizationContext.getConfiguration());
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(executionOperator);

        // Initialize cardinality and number of executions.
        operatorContext.setNumExecutions(numExecutions);
        this.setCardinality(operatorContext, cardinality);

        // Estimate time.
        operatorContext.updateCostEstimate();
        return operatorContext.getCostEstimate();
    }

    @Override
    public boolean isFiltered(CardinalityEstimate cardinality, int numExecutions, OptimizationContext optimizationContext) {
        // Create OperatorContext.
        final ExecutionOperator executionOperator = this.executionOperatorFactory.apply(null, optimizationContext.getConfiguration());
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(executionOperator);

        // Initialize cardinality and number of executions.
        operatorContext.setNumExecutions(numExecutions);
        this.setCardinality(operatorContext, cardinality);

        return executionOperator.isFiltered(operatorContext);
    }

    private void setCardinality(OptimizationContext.OperatorContext operatorContext, CardinalityEstimate cardinality) {
        final int numInputs = operatorContext.getOperator().getNumInputs();
        for (int inputIndex = 0; inputIndex < numInputs; inputIndex++) {
            operatorContext.setInputCardinality(inputIndex, cardinality);
        }
        final int numOutputs = operatorContext.getOperator().getNumOutputs();
        for (int outputIndex = 0; outputIndex < numOutputs; outputIndex++) {
            operatorContext.setOutputCardinality(outputIndex, cardinality);
        }
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.name);
    }
}
