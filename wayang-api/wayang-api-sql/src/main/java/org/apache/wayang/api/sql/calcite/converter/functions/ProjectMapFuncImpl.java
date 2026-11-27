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

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.basic.data.Record;

public class ProjectMapFuncImpl implements
        FunctionDescriptor.SerializableFunction<Record, Record> {

    /**
     * Operations that are NULL if any operand is NULL
     */
    static final EnumSet<SqlKind> NULL_IF_ANY_OPERAND_IS_NULL = EnumSet.of(
            SqlKind.PLUS,
            SqlKind.MINUS,
            SqlKind.TIMES,
            SqlKind.DIVIDE);

    private final List<Node> projectionSyntaxTrees;

    public ProjectMapFuncImpl(final List<RexNode> projects) {
        final ProjectCallTreeFactory treeFactory = new ProjectCallTreeFactory();
        this.projectionSyntaxTrees = projects.stream().map(treeFactory::fromRexNode).toList();
    }

    class ProjectCallTreeFactory implements CallTreeFactory {
        public SerializableFunction<List<Object>, Object> deriveOperation(final SqlKind kind, final SqlTypeName returnType) {
            return input -> {
                // as in SQL, arithmetic with a NULL operand gives NULL
                if (NULL_IF_ANY_OPERAND_IS_NULL.contains(kind) && input.stream().anyMatch(Objects::isNull)) {
                    return null;
                }
                return switch (kind) {
                    case PLUS   -> asDouble(input.get(0)) + asDouble(input.get(1));
                    case MINUS  -> asDouble(input.get(0)) - asDouble(input.get(1));
                    case TIMES  -> asDouble(input.get(0)) * asDouble(input.get(1));
                    case DIVIDE -> asDouble(input.get(0)) / asDouble(input.get(1));
                    default -> throw new UnsupportedOperationException(
                                "Operation not supported in projection function RexCall: " + kind);
                };
            };
        }

        double asDouble(final Object o){
            assert o instanceof Number : "Cannot perform arithmetic on non-numbers: " + o.getClass();
            return ((Number) o).doubleValue();
        }
    }

    @Override
    public Record apply(final Record rec) {
        return new Record(projectionSyntaxTrees.stream().map(call -> call.evaluate(rec)).toArray());
    }
}
