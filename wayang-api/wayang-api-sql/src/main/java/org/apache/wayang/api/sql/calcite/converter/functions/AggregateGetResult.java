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

package org.apache.wayang.api.sql.calcite.converter.functions;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.util.ReflectionUtils;

public class AggregateGetResult implements FunctionDescriptor.SerializableFunction<Record, Record> {
        private final List<SqlKind> aggregateKindList;
        private final Set<Integer> groupingfields;

        public AggregateGetResult(final List<AggregateCall> aggregateCalls, final Set<Integer> groupingfields) {
                this.aggregateKindList = aggregateCalls.stream()
                                .map(call -> call.getAggregation().getKind())
                                .collect(Collectors.toList());
                this.groupingfields = groupingfields;
        }

        @Override
        public Record apply(final Record record) {
                final int recordSize = record.size();
                final int aggregateCallOffset = recordSize - aggregateKindList.size() - 1;

                final Object[] fields = groupingfields.stream()
                                .map(record::getField)
                                .toArray();

                final Object[] aggregateCallFields = IntStream.range(0, aggregateKindList.size())
                                .mapToObj(i -> aggregateKindList.get(i).equals(SqlKind.AVG)
                                                ? average(record.getField(i + aggregateCallOffset))
                                                : record.getField(i + aggregateCallOffset))
                                .toArray();

                final Object[] combinedFields = Stream.concat(Arrays.stream(fields), Arrays.stream(aggregateCallFields))
                                .toArray();

                return new Record(combinedFields);
        }

        /**
         * Divides the running sum of an AVG by its number of non-null values, see {@link AggregateAddCols}.
         *
         * @param sumAndCount the running sum and the number of non-null values
         * @return the average, or null if all values were null
         */
        private static Object average(final Object sumAndCount) {
                final Object[] partial = (Object[]) sumAndCount;
                final int count = (Integer) partial[1];
                return count == 0 ? null : ReflectionUtils.toDouble(partial[0]) / count;
        }
}