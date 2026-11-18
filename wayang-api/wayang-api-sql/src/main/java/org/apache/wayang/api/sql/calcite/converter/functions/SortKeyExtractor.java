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

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;

public class SortKeyExtractor implements FunctionDescriptor.SerializableFunction<Record, Record> {
    final List<Direction> directions;
    final List<NullDirection> nullDirections;
    final List<Integer> collationIndexes;

    public SortKeyExtractor(final List<Direction> collationDirections, final List<Integer> collationIndexes) {
        this(collationDirections,
                collationDirections.stream().map(Direction::defaultNullDirection).collect(Collectors.toList()),
                collationIndexes);
    }

    public SortKeyExtractor(final List<Direction> collationDirections, final List<NullDirection> nullDirections,
            final List<Integer> collationIndexes) {
        this.directions = collationDirections;
        this.nullDirections = nullDirections;
        this.collationIndexes = collationIndexes;
    }

    @Override
    public Record apply(final Record record) {
        return new Record(collationIndexes.stream().map(record::getField).toArray()) {
            @Override
            public int compareTo(final Record that) throws IllegalStateException {
                assert (directions.size() == collationIndexes.size()) : "Mismatch between the amount of collation indexes and directions";

                for (int i = 0; i < directions.size(); i++) {
                    final Direction direction = directions.get(i);
                    final Comparable thisField = (Comparable) this.getValues()[i];
                    final Comparable thatField = (Comparable) that.getValues()[i];

                    // NULLS FIRST / NULLS LAST places nulls regardless of the sort direction
                    if (thisField == null || thatField == null) {
                        if (thisField == thatField) continue;
                        final boolean nullsFirst = nullDirections.get(i) == NullDirection.FIRST;
                        return (thisField == null) == nullsFirst ? -1 : 1;
                    }

                    // == 0, < -1, > 1
                    final int comparison = direction.isDescending() ? -thisField.compareTo(thatField) : thisField.compareTo(thatField);
                    
                    if (comparison != 0) return comparison;
                }
                
                return 0;
            }
        };
    }
}
