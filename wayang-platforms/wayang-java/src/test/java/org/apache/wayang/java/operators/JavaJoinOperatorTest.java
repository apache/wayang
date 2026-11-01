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

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link JavaJoinOperator}.
 */
class JavaJoinOperatorTest extends JavaExecutionOperatorTestBase {

    @ParameterizedTest
    @CsvSource(value = {
            "100, 200", "200, 100", "100, 100",
            "NULL, NULL", "NULL, 100", "100, NULL",
            "2147483648, 2147483649", "2147483649, 2147483648"
    }, nullValues = "NULL")
    void testExecution(Long estimate0, Long estimate1) {
        // Prepare test data.
        Stream<Tuple2<Integer, String>> inputStream0 = Arrays.asList(
                new Tuple2<>(1, "b"), new Tuple2<>(1, "c"), new Tuple2<>(2, "d"), new Tuple2<>(3, "e")
        ).stream();
        Stream<Tuple2<String, Integer>> inputStream1 = Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)
        ).stream();

        // Build the join operator.
        JavaJoinOperator<Tuple2<Integer, String>, Tuple2<String, Integer>, Integer> join =
                new JavaJoinOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field1"));

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{
                createStreamChannelInstance(inputStream0),
                createStreamChannelInstance(inputStream1)
        };
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        OptimizationContext.OperatorContext operatorContext = createOperatorContext(join);
        operatorContext.setInputCardinality(0, estimate0 == null ? null : new CardinalityEstimate(estimate0, estimate0, 1d));
        operatorContext.setInputCardinality(1, estimate1 == null ? null : new CardinalityEstimate(estimate1, estimate1, 1d));
        join.evaluate(inputs, outputs, createExecutor(), operatorContext);

        // Verify the outcome.
        final List<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>> result =
                outputs[0].<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>>provideStream()
                        .collect(Collectors.toList());
        Collections.sort(result, (joinTuple1, joinTuple2) -> {
            int cmp = joinTuple1.getField0().getField0().compareTo(joinTuple2.getField0().getField0());
            if (cmp == 0) {
                cmp = joinTuple1.getField0().getField1().compareTo(joinTuple2.getField0().getField1());
            }
            if (cmp == 0) {
                cmp = joinTuple1.getField1().getField0().compareTo(joinTuple2.getField1().getField0());
            }
            if (cmp == 0) {
                cmp = joinTuple1.getField1().getField1().compareTo(joinTuple2.getField1().getField1());
            }
            return cmp;
        });
        final List<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>> expectedResult = Arrays.asList(
                new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("x", 1)),
                new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("y", 1)),
                new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("x", 1)),
                new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("y", 1)),
                new Tuple2<>(new Tuple2<>(2, "d"), new Tuple2<>("z", 2))
        );

        assertEquals(expectedResult, result);
    }

    @ParameterizedTest
    @CsvSource({"8, true", "8, false", "2048, true", "2048, false", "0, true", "0, false"})
    void testExecutionWithDifferentKeyCounts(int numKeys, boolean materializeFirst) {
        JavaJoinOperator<Integer, Integer, Integer> join = new JavaJoinOperator<>(
                DataSetType.createDefault(Integer.class), DataSetType.createDefault(Integer.class),
                new TransformationDescriptor<>(key -> key, Integer.class, Integer.class),
                new TransformationDescriptor<>(key -> key, Integer.class, Integer.class));
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{
                createStreamChannelInstance(IntStream.range(0, numKeys).boxed()
                        .flatMap(key -> Stream.of(key, key))),
                createStreamChannelInstance(IntStream.range(0, numKeys).boxed()
                        .flatMap(key -> Stream.of(key, key, key)))
        };
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        OptimizationContext.OperatorContext operatorContext = createOperatorContext(join);
        // Exercise both build sides, including estimates below the actual number of distinct keys.
        CardinalityEstimate smallerEstimate = new CardinalityEstimate(1, 1, 1d);
        CardinalityEstimate largerEstimate = new CardinalityEstimate(2, 2, 1d);
        operatorContext.setInputCardinality(0, materializeFirst ? smallerEstimate : largerEstimate);
        operatorContext.setInputCardinality(1, materializeFirst ? largerEstimate : smallerEstimate);

        join.evaluate(inputs, outputs, createExecutor(), operatorContext);

        Map<Tuple2<Integer, Integer>, Long> result = outputs[0].<Tuple2<Integer, Integer>>provideStream()
                .collect(Collectors.groupingBy(tuple -> tuple, Collectors.counting()));
        Map<Tuple2<Integer, Integer>, Long> expected = IntStream.range(0, numKeys).boxed()
                .collect(Collectors.toMap(key -> new Tuple2<>(key, key), key -> 6L));
        assertEquals(expected, result);
    }

}
