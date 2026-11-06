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

package org.apache.wayang.flink.operators;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test suite for {@link FlinkReduceByOperator}.
 */
class FlinkReduceByOperatorTest extends FlinkOperatorTestBase {

    @Test
    void testExecution() throws Exception {
        // Prepare test data.
        List<Tuple2<String, Integer>> inputList = Arrays.stream("aaabbccccdeefff".split(""))
                .map(string -> new Tuple2<>(string, 1))
                .collect(Collectors.toList());
        DataSetChannel.Instance input = this.createDataSetChannelInstance(inputList);
        DataSetChannel.Instance output = this.createDataSetChannelInstance();


        // Build the reduce operator.
        FlinkReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator =
                new FlinkReduceByOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(String.class),
                                "field0"),
                        new ReduceDescriptor<>(
                                (a, b) -> {
                                    a.field1 += b.field1;
                                    return a;
                                }, DataUnitType.createGroupedUnchecked(Tuple2.class),
                                DataUnitType.createBasicUnchecked(Tuple2.class)
                        ));

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        try {
            this.evaluate(reduceByOperator, inputs, outputs);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        // Verify the outcome.
        final Iterable<Tuple2<String, Integer>> result = output.<Tuple2<String, Integer>>provideDataSet().collect();
        final Set<Tuple2<String, Integer>> resultSet = new HashSet<>();
        result.forEach(resultSet::add);
        final Tuple2[] expectedResults = {
                new Tuple2<>("a", 3),
                new Tuple2<>("b", 2),
                new Tuple2<>("c", 4),
                new Tuple2<>("d", 1),
                new Tuple2<>("e", 2),
                new Tuple2<>("f", 3)
        };
        Arrays.stream(expectedResults)
                .forEach(expected -> assertTrue(resultSet.contains(expected), "Not contained: " + expected));
        assertEquals(expectedResults.length, resultSet.size());

    }
}
