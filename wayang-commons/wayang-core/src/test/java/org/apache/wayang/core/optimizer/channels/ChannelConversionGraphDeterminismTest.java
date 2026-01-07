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
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationUtils;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.test.DummyExecutionOperator;
import org.apache.wayang.core.test.DummyExternalReusableChannel;
import org.apache.wayang.core.test.DummyNonReusableChannel;
import org.apache.wayang.core.test.DummyReusableChannel;
import org.apache.wayang.core.test.MockFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChannelConversionGraphDeterminismTest {

    private static Supplier<ExecutionOperator> createDummyExecutionOperatorFactory(ChannelDescriptor channelDescriptor) {
        return () -> {
            ExecutionOperator execOp = new DummyExecutionOperator(1, 1, false);
            execOp.getSupportedOutputChannels(0).add(channelDescriptor);
            return execOp;
        };
    }

    private static DefaultChannelConversion conversion(ChannelDescriptor source, ChannelDescriptor target) {
        return new DefaultChannelConversion(source, target, createDummyExecutionOperatorFactory(target));
    }

    @Test
    void channelConversionSelectionIsStable() {
        List<String> first = computeJunctionFingerprint();
        List<String> second = computeJunctionFingerprint();
        assertEquals(first, second, "Channel conversion choices must be deterministic.");
    }

    private static List<String> computeJunctionFingerprint() {
        Configuration configuration = new Configuration();
        ChannelConversionGraph graph = new ChannelConversionGraph(configuration);
        graph.add(conversion(DummyReusableChannel.DESCRIPTOR, DummyNonReusableChannel.DESCRIPTOR));
        graph.add(conversion(DummyReusableChannel.DESCRIPTOR, DummyExternalReusableChannel.DESCRIPTOR));
        graph.add(conversion(DummyExternalReusableChannel.DESCRIPTOR, DummyNonReusableChannel.DESCRIPTOR));
        graph.add(conversion(DummyNonReusableChannel.DESCRIPTOR, DummyReusableChannel.DESCRIPTOR));

        Job job = MockFactory.createJob(configuration);
        OptimizationContext optimizationContext = new DefaultOptimizationContext(job);

        DummyExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyReusableChannel.DESCRIPTOR);
        optimizationContext.addOneTimeOperator(sourceOperator)
                .setOutputCardinality(0, new CardinalityEstimate(1000, 1000, 1d));

        DummyExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        DummyExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyExternalReusableChannel.DESCRIPTOR);

        Junction junction = graph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext,
                false
        );

        return describeJunction(junction);
    }

    private static List<String> describeJunction(Junction junction) {
        List<String> descriptorList = new ArrayList<>();
        descriptorList.add(describeChannel(junction.getSourceChannel(), true));
        for (int i = 0; i < junction.getNumTargets(); i++) {
            descriptorList.add(describeChannel(junction.getTargetChannel(i), false));
        }
        return descriptorList;
    }

    private static String describeChannel(Channel channel, boolean isSourceChannel) {
        if (channel == null) {
            return "null";
        }
        List<String> descriptors = new ArrayList<>();
        Channel cursor = channel;
        while (cursor != null) {
            descriptors.add(cursor.getDescriptor().toString() + (cursor.isCopy() ? ":copy" : ":orig"));
            ExecutionTask producer = cursor.getProducer();
            if (producer == null || producer.getNumInputChannels() == 0) {
                break;
            }
            // If we are describing the top-level source channel (junction entry), stop once we reach the producer that
            // has no inputs. For target channels, follow until the conversion tree ends.
            if (isSourceChannel) {
                cursor = producer.getNumInputChannels() == 0 ? null : producer.getInputChannel(0);
            } else if (producer.getNumInputChannels() == 0) {
                cursor = null;
            } else {
                cursor = producer.getInputChannel(0);
            }
        }
        Collections.reverse(descriptors);
        return descriptors.stream().collect(Collectors.joining("->"));
    }
}
