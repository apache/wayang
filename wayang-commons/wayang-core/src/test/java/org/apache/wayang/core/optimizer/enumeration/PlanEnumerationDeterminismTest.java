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

package org.apache.wayang.core.optimizer.enumeration;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.configuration.ExplicitCollectionProvider;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.ConstantLoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadEstimate;
import org.apache.wayang.core.optimizer.costs.LoadProfile;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.test.DummyExecutionOperator;
import org.apache.wayang.core.test.DummyReusableChannel;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test that exercises {@link PlanEnumeration#concatenate(OutputSlot, Collection, Map, OptimizationContext, org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement)}
 * to ensure that plan combinations are produced deterministically.
 */
class PlanEnumerationDeterminismTest {

    @Test
    void concatenationProducesStablePlanOrdering() {
        Configuration configuration = new Configuration();
        configuration.setPruningStrategyClassProvider(
                new ExplicitCollectionProvider<Class<PlanEnumerationPruningStrategy>>(configuration)
        );
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);

        DummyExecutionOperator producer = new DummyExecutionOperator(0, 1, false);
        DummyExecutionOperator consumer = new DummyExecutionOperator(1, 0, false);
        registerChannelDescriptors(producer, Collections.singletonList(consumer));
        registerLoadEstimator(configuration, producer, 10);
        registerLoadEstimator(configuration, consumer, 5);

        List<String> firstRun = enumerateDeterministicIds(job, producer, Collections.singletonList(consumer), 3, 2);
        List<String> secondRun = enumerateDeterministicIds(job, producer, Collections.singletonList(consumer), 3, 2);

        assertTrue(firstRun.size() > 1, "Expected multiple plan implementations.");
        assertEquals(firstRun, secondRun, "Enumeration order must be deterministic.");
    }

    @Test
    void concatenationWithMultipleTargetsRemainsStable() {
        Configuration configuration = new Configuration();
        configuration.setPruningStrategyClassProvider(
                new ExplicitCollectionProvider<Class<PlanEnumerationPruningStrategy>>(configuration)
        );
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);

        DummyExecutionOperator producer = new DummyExecutionOperator(0, 1, false);
        DummyExecutionOperator consumerA = new DummyExecutionOperator(1, 0, false);
        DummyExecutionOperator consumerB = new DummyExecutionOperator(1, 0, false);
        registerChannelDescriptors(producer, Arrays.asList(consumerA, consumerB));
        registerLoadEstimator(configuration, producer, 10);
        registerLoadEstimator(configuration, consumerA, 7);
        registerLoadEstimator(configuration, consumerB, 3);

        List<String> firstRun = enumerateDeterministicIds(job, producer, Arrays.asList(consumerA, consumerB), 4, 2);
        List<String> secondRun = enumerateDeterministicIds(job, producer, Arrays.asList(consumerA, consumerB), 4, 2);

        assertEquals(firstRun, secondRun, "Enumeration order with multiple targets must be deterministic.");
    }

    private static List<String> enumerateDeterministicIds(Job job,
                                                          ExecutionOperator producer,
                                                          List<? extends ExecutionOperator> consumers,
                                                          int numBaseCopies,
                                                          int numTargetCopies) {
        DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        optimizationContext.addOneTimeOperator(producer);
        consumers.forEach(optimizationContext::addOneTimeOperator);

        PlanEnumeration baseEnumeration = PlanEnumeration.createSingleton(producer, optimizationContext);
        duplicatePlanImplementations(baseEnumeration, numBaseCopies);

        Map<InputSlot<?>, PlanEnumeration> targets = new LinkedHashMap<>();
        consumers.forEach(consumer -> {
            PlanEnumeration targetEnumeration = PlanEnumeration.createSingleton((ExecutionOperator) consumer, optimizationContext);
            duplicatePlanImplementations(targetEnumeration, numTargetCopies);
            targets.put(consumer.getInput(0), targetEnumeration);
        });

        PlanEnumeration concatenated = baseEnumeration.concatenate(
                producer.getOutput(0),
                Collections.<Channel>emptyList(),
                targets,
                optimizationContext,
                null
        );

        return concatenated.getPlanImplementations().stream()
                .map(PlanImplementation::getDeterministicIdentifier)
                .collect(Collectors.toList());
    }

    private static void duplicatePlanImplementations(PlanEnumeration enumeration, int desiredCount) {
        PlanImplementation template = enumeration.getPlanImplementations().iterator().next();
        while (enumeration.getPlanImplementations().size() < desiredCount) {
            enumeration.add(new PlanImplementation(template));
        }
    }

    private static void registerLoadEstimator(Configuration configuration,
                                              ExecutionOperator operator,
                                              long cpuCost) {
        ConstantLoadProfileEstimator estimator = new ConstantLoadProfileEstimator(
                new LoadProfile(new LoadEstimate(cpuCost), new LoadEstimate(1))
        );
        configuration.getOperatorLoadProfileEstimatorProvider().set(operator, estimator);
    }

    private static void registerChannelDescriptors(DummyExecutionOperator producer,
                                                   List<DummyExecutionOperator> consumers) {
        producer.getSupportedOutputChannels(0).add(DummyReusableChannel.DESCRIPTOR);
        consumers.forEach(consumer -> consumer.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR));
    }
}
