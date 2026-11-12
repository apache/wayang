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
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test suite for {@link JavaJoinOperator}.
 */
class JavaCoGroupOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    void testExecution() {
        // Prepare test data.
        CollectionChannel.Instance input0 = createCollectionChannelInstance(Arrays.asList(
                new Tuple2<>(1, "b"),
                new Tuple2<>(1, "c"),
                new Tuple2<>(2, "d"),
                new Tuple2<>(3, "e")
        ));
        CollectionChannel.Instance input1 = this.createCollectionChannelInstance(Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)));
        CollectionChannel.Instance output = createCollectionChannelInstance();

        // Build the Cartesian operator.
        JavaCoGroupOperator<Tuple2<Integer, String>, Tuple2<String, Integer>, Integer> coGroup =
                new JavaCoGroupOperator<>(
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
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{input0, input1};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{output};
        evaluate(coGroup, inputs, outputs);

        // Verify the outcome.
        final Collection<Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>>> result =
                output.provideCollection();
        Collection<Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>> expectedGroups =
                new ArrayList<>(Arrays.asList(
                        new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Arrays.asList(new Tuple2<>(1, "b"), new Tuple2<>(1, "c")),
                                Arrays.asList(new Tuple2<>("x", 1), new Tuple2<>("y", 1))
                        ),
                        new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Collections.singletonList(new Tuple2<>(2, "d")),
                                Collections.singletonList(new Tuple2<>("z", 2))
                        ), new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Collections.singletonList(new Tuple2<>(3, "e")),
                                Collections.emptyList()
                        ),
                        new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Collections.emptyList(),
                                Collections.singletonList(new Tuple2<>("w", 4))
                        )
                ));

        ResultLoop:
        for (Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>> resultCoGroup : result) {
            for (Iterator<Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>> i = expectedGroups.iterator();
                 i.hasNext(); ) {
                Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>> expectedGroup = i.next();
                if (this.compare(expectedGroup, resultCoGroup)) {
                    i.remove();
                    continue ResultLoop;
                }
            }
            fail(String.format("Unexpected group: %s", resultCoGroup));
        }
        assertTrue(
                expectedGroups.isEmpty(),
                String.format("Missing groups: %s", expectedGroups)
        );
    }

    @Test
    void testExecutionWithLowCardinalityKeys() {
        Collection<Tuple2<Integer, String>> coll0 = new ArrayList<>();
        Collection<Tuple2<String, Integer>> coll1 = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            coll0.add(new Tuple2<>(i % 2, "a" + i));
            coll1.add(new Tuple2<>("b" + i, i % 2));
        }

        CollectionChannel.Instance input0 = createCollectionChannelInstance(coll0);
        CollectionChannel.Instance input1 = createCollectionChannelInstance(coll1);
        CollectionChannel.Instance output = createCollectionChannelInstance();

        JavaCoGroupOperator<Tuple2<Integer, String>, Tuple2<String, Integer>, Integer> coGroup =
                new JavaCoGroupOperator<>(
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

        JavaChannelInstance[] inputs = new JavaChannelInstance[]{input0, input1};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{output};
        evaluate(coGroup, inputs, outputs);

        final Collection<Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>>> result =
                output.provideCollection();
        assertEquals(2, result.size());
        for (Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>> group : result) {
            assertEquals(500, WayangCollections.asSet(group.field0).size());
            assertEquals(500, WayangCollections.asSet(group.field1).size());
        }
    }

    private boolean compare(Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>> expected,
                            Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>> actual) {
        return this.compareGroup(expected.field0, actual.field0) && this.compareGroup(expected.field1, actual.field1);
    }

    private <T> boolean compareGroup(Collection<T> expected, Iterable<T> actual) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        return WayangCollections.asSet(expected).equals(WayangCollections.asSet(actual));
    }

}
