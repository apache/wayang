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

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

import com.google.common.collect.ImmutableRangeSet;

/**
 * AST of the {@link RexCall} arithmetic, composed into serializable nodes;
 * {@link Call}, {@link InputRef}, {@link Literal}
 */
interface CallTreeFactory extends Serializable {
    public default Node fromRexNode(final RexNode node) {
        if (node instanceof final RexCall call) {
            return new Call(call, this);
        } else if (node instanceof final RexInputRef inputRef) {
            return new InputRef(inputRef);
        } else if (node instanceof final RexLiteral literal) {
            return new Literal(literal);
        } else {
            throw new UnsupportedOperationException("Unsupported RexNode in filter condition: " + node);
        }
    }

    /**
     * Derives the java operator for a given {@link SqlKind}, and turns it into a
     * serializable function
     * 
     * @param kind {@link SqlKind} from {@link RexCall} SqlOperator
     * @param returnType return type of the {@link RexCall}
     * @return a serializable function of +, -, * or /
     * @throws UnsupportedOperationException on unrecognized {@link SqlKind}
     */
    public SerializableFunction<List<Object>, Object> deriveOperation(final SqlKind kind, final SqlTypeName returnType);
}

interface Node extends Serializable {
    public Object evaluate(final Record rec);
}

final class Call implements Node {
    private final List<Node> operands;
    final SerializableFunction<List<Object>, Object> operation;

    protected Call(final RexCall call, final CallTreeFactory tree) {
        operands = call.getOperands().stream().map(tree::fromRexNode).toList();
        operation = tree.deriveOperation(call.getKind(), call.getType().getSqlTypeName());
    }

    @Override
    public Object evaluate(final Record rec) {
        return operation.apply(
                operands.stream()
                        .map(op -> op.evaluate(rec))
                        .toList());
    }
}

final class Literal implements Node {
    final Serializable value;

    Literal(final RexLiteral literal) {
        value = switch (literal.getTypeName()) {
            case DATE         -> literal.getValueAs(Calendar.class);
            case INTEGER      -> literal.getValueAs(Double.class);
            case INTERVAL_DAY -> literal.getValueAs(BigDecimal.class).doubleValue();
            case DECIMAL      -> literal.getValueAs(BigDecimal.class).doubleValue();
            case CHAR         -> literal.getValueAs(String.class);
            case SARG         -> {
                final Sarg<?> sarg = literal.getValueAs(Sarg.class);
                assert sarg.rangeSet instanceof Serializable : "Sarg RangeSet was not serializable.";
                yield new SargValue((ImmutableRangeSet<?>) sarg.rangeSet, sarg.nullAs);
            }
            default -> throw new UnsupportedOperationException(
                    "Literal conversion to Java not implemented, type: " + literal.getTypeName());
        };
    }

    @Override
    public Object evaluate(final Record rec) {
        return value;
    }
}

/**
 * Serializable form of a {@link Sarg}: its ranges, and what the search returns for a
 * NULL value (for example TRUE for {@code x IN (1, 2) OR x IS NULL}).
 */
record SargValue(ImmutableRangeSet<?> rangeSet, RexUnknownAs nullAs) implements Serializable {
}

final class InputRef implements Node {
    private final int key;

    InputRef(final RexInputRef inputRef) {
        this.key = inputRef.getIndex();
    }

    @Override
    public Object evaluate(final Record rec) {
        return rec.getField(key);
    }
}