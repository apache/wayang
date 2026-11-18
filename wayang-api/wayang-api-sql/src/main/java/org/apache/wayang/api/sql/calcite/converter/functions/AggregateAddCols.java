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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.function.FunctionDescriptor;

public class AggregateAddCols implements FunctionDescriptor.SerializableFunction<Record, Record> {
    final List<Tuple2<SqlKind, List<Integer>>> aggregateCalls;

    public AggregateAddCols(final List<AggregateCall> aggregateCalls) {
        this.aggregateCalls = aggregateCalls.stream().map(call -> new Tuple2<>(call.getAggregation().getKind(), call.getArgList())).collect(Collectors.toList());
    }

    @Override
    public Record apply(final Record record) {
        final int l = record.size();
        final int newRecordSize = l + aggregateCalls.size() + 1;
        final Object[] resValues = new Object[newRecordSize];

        for (int i = 0; i < l; i++) {
            resValues[i] = record.getField(i);
        }

        int i = l;
        for (final Tuple2<SqlKind, List<Integer>> aggregateCall : aggregateCalls) {
            final SqlKind kind = aggregateCall.field0;
            final List<Integer> argList = aggregateCall.field1;

            switch (kind) {
                case COUNT:
                    // COUNT(*) has no arguments, COUNT(col) only counts rows where col is not null
                    resValues[i] = argList.stream().allMatch(index -> record.getField(index) != null) ? 1 : 0;
                    break;
                case AVG:
                    // running sum and number of non-null values, divided in AggregateGetResult
                    final Object value = record.getField(argList.get(0));
                    resValues[i] = new Object[] { value, value == null ? 0 : 1 };
                    break;
                default:
                    resValues[i] = record.getField(argList.get(0));
            }
            i++;
        }

        resValues[newRecordSize - 1] = 1;
        return new Record(resValues);
    }
}