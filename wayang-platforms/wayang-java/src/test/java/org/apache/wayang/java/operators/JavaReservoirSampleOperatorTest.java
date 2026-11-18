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

package org.apache.wayang.java.operators;

import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link JavaReservoirSampleOperator}.
 */
class JavaReservoirSampleOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    void testExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final int sampleSize = 5;

        // Build the distinct operator.
        JavaReservoirSampleOperator<Integer> sampleOperator =
                new JavaReservoirSampleOperator<>(
                        iterationNumber -> sampleSize,
                        DataSetType.createDefaultUnchecked(Integer.class),
                        iterationNumber -> 42L
                );
        sampleOperator.setDatasetSize(10);

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createCollectionChannelInstance()};

        // Execute.
        evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        assertEquals(sampleSize, result.size());
    }

    @Test
    void testExecutionWithUnknownDatasetSize() {
        // Prepare test data. The dataset size is deliberately left unknown
        // (setDatasetSize is never called), which is the normal case for a
        // reservoir sample over a stream.
        Stream<Integer> inputStream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final int sampleSize = 5;

        // Build the operator.
        JavaReservoirSampleOperator<Integer> sampleOperator =
                new JavaReservoirSampleOperator<>(
                        iterationNumber -> sampleSize,
                        DataSetType.createDefaultUnchecked(Integer.class),
                        iterationNumber -> 42L
                );

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createCollectionChannelInstance()};

        // Execute.
        evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome: a sample of the requested size, not the whole input.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        assertEquals(sampleSize, result.size());
    }

    @Test
    void testLargerSampleExecution() {
        // Prepare test data.
        Stream<Integer> inputStream = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Build the distinct operator.
        JavaRandomSampleOperator<Integer> sampleOperator =
                new JavaRandomSampleOperator<>(
                        iterationNumber -> 15, // sample size larger than dataset size
                        DataSetType.createDefaultUnchecked(Integer.class),
                        iteration -> 42L
                );
        sampleOperator.setDatasetSize(10);

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createStreamChannelInstance(inputStream)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};

        // Execute.
        evaluate(sampleOperator, inputs, outputs);

        // Verify the outcome.
        final List<Integer> result = outputs[0].<Integer>provideStream().collect(Collectors.toList());
        assertEquals(10, result.size());

    }

}
