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

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;

public class FilterPredicateImpl implements FunctionDescriptor.SerializablePredicate<Record> {
    class FilterCallTreeFactory implements CallTreeFactory {
        public SerializableFunction<List<Object>, Object> deriveOperation(final SqlKind kind, final SqlTypeName returnType) {
            return input -> switch (kind) {
                case NOT -> !(boolean) input.get(0);
                case IS_NOT_NULL -> !isEqualTo(input.get(0), null);
                case IS_NULL -> isEqualTo(input.get(0), null);
                case LIKE -> like((String) input.get(0), (String) input.get(1));
                case NOT_EQUALS -> !isEqualTo(input.get(0), input.get(1));
                case EQUALS -> isEqualTo(input.get(0), input.get(1));
                case GREATER_THAN -> isGreaterThan(input.get(0), input.get(1));
                case LESS_THAN -> isLessThan(input.get(0), input.get(1));
                case GREATER_THAN_OR_EQUAL ->
                    isGreaterThan(input.get(0), input.get(1)) || isEqualTo(input.get(0), input.get(1));
                case LESS_THAN_OR_EQUAL ->
                    isLessThan(input.get(0), input.get(1)) || isEqualTo(input.get(0), input.get(1));
                case AND -> input.stream().allMatch(obj -> Boolean.class.cast(obj).booleanValue());
                case OR -> input.stream().anyMatch(obj -> Boolean.class.cast(obj).booleanValue());
                case MINUS -> widenToDouble.apply(input.get(0)) - widenToDouble.apply(input.get(1));
                case PLUS -> widenToDouble.apply(input.get(0)) + widenToDouble.apply(input.get(1));
                case CAST -> SqlRuntimeCast.castValue(input.get(0), returnType);
                case SEARCH -> {
                    if (input.get(0) instanceof final ImmutableRangeSet range) {
                        assert input.get(1) instanceof Comparable
                                : "field is not comparable: " + input.get(1).getClass();
                        final Comparable field = ensureComparable.apply(input.get(1));
                        final Comparable left = ensureComparable.apply(range.span().lowerEndpoint());
                        final Comparable right = ensureComparable.apply(range.span().upperEndpoint());
                        final Range<Comparable> newRange = Range.closed(left, right);
                        yield newRange.contains(field);
                    } else if (input.get(1) instanceof final ImmutableRangeSet range) {
                        assert input.get(0) instanceof Comparable
                                : "field is not comparable: " + input.get(0).getClass();
                        final Comparable field = ensureComparable.apply(input.get(0));
                        final Comparable left = ensureComparable.apply(range.span().lowerEndpoint());
                        final Comparable right = ensureComparable.apply(range.span().upperEndpoint());
                        final Range<Comparable> newRange = Range.closed(left, right);
                        yield newRange.contains(field);
                    } else {
                        throw new UnsupportedOperationException("No range set found in SARG, input1: "
                                + input.get(0).getClass() + ", input2: " + input.get(1).getClass());
                    }
                }
                default -> throw new UnsupportedOperationException("Kind not supported: " + kind);
            };
        }

        /**
         * Java equivalent of SQL like clauses
         * 
         * @param s1
         * @param s2
         * @return true if {@code s1} like {@code s2}
         */
        private static boolean like(final String s1, final String s2) {
            return s1 == null || s2 == null ? false : new SqlFunctions.LikeFunction().like(s1, s2);

        }

        /**
         * Java equivalent of sql greater than clauses
         * 
         * @param o1
         * @param o2
         * @return true if {@code o1 > o2}
         */
        private static boolean isGreaterThan(final Object o1, final Object o2) {
            return ensureComparable.apply(o1).compareTo(ensureComparable.apply(o2)) > 0;
        }

        /**
         * Java equivalent of sql less than clauses
         * 
         * @param o1
         * @param o2
         * @return true if {@code o1 < o2}
         */
        private static boolean isLessThan(final Object o1, final Object o2) {
            return ensureComparable.apply(o1).compareTo(ensureComparable.apply(o2)) < 0;
        }

        /**
         * Java equivalent of SQL equals clauses
         * 
         * @param o1
         * @param o2
         * @return true if {@code o1 == o2}
         */
        private static boolean isEqualTo(final Object o1, final Object o2) {
            return Objects.equals(ensureComparable.apply(o1), ensureComparable.apply(o2));
        }
    }

    private final Node callTree;

    /**
     * Widens number types to double
     * 
     * @throws UnsupportedOperationException if conversion was not possible
     */
    final static SerializableFunction<Object, Double> widenToDouble = field -> {
        if (field instanceof final Number number) {
            return number.doubleValue();
        } else if (field instanceof final Date date) {
            return (double) date.getTime();
        } else if (field instanceof final Calendar calendar) {
            return (double) calendar.getTime().getTime();
        } else {
            throw new UnsupportedOperationException("Could not widen to double, field class: " + field.getClass());
        }
    };

    /**
     * Widening conversions, all numbers to double
     */
    final static SerializableFunction<Object, Comparable> ensureComparable = field -> {
        if (field instanceof final Number number) {
            return number.doubleValue();
        } else if (field instanceof final Date date) {
            return (double) date.getTime();
        } else if (field instanceof final Calendar calendar) {
            return (double) calendar.getTime().getTime();
        } else if (field instanceof final String string) {
            return string;
        } else if (field instanceof final NlsString nlsString) {
            return nlsString.getValue();
        }  else if (field instanceof final Character character) {
            return character.toString();
        } else if (field instanceof final DateString dateString) {
            return (double) dateString.getMillisSinceEpoch();
        } else if (field == null) {
            return null;
        } else {
            throw new UnsupportedOperationException(
                    "Type not supported in filter comparisons yet: " + field.getClass());
        }
    };

    public FilterPredicateImpl(final RexNode condition) {
        this.callTree = new FilterCallTreeFactory().fromRexNode(condition);
    }

    @Override
    public boolean test(final Record rec) {
        return (boolean) callTree.evaluate(rec);
    }
}