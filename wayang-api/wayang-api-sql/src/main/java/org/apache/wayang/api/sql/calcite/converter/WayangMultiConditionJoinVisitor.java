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

package org.apache.wayang.api.sql.calcite.converter;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import org.apache.wayang.api.sql.calcite.converter.functions.JoinFlattenResult;
import org.apache.wayang.api.sql.calcite.converter.functions.MultiConditionJoinKeyExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.api.sql.calcite.rel.WayangTableScan;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.ReflectionUtils;

public class WayangMultiConditionJoinVisitor extends WayangRelNodeVisitor<WayangJoin> implements Serializable {

    /**
     * Visitor that visits join statements that has multiple conditions like:
     * AND(=($1,$2),=($2,$3))
     * Note that this doesnt support nway joins or multijoins.
     * 
     * @param wayangRelConverter
     */
    public WayangMultiConditionJoinVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    public Operator visit(WayangJoin wayangRelNode) {
        final Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0));
        final Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1));
        final RexNode condition = ((Join) wayangRelNode).getCondition();
        final RexCall call = (RexCall) condition;

        final List<RexCall> subConditions = call.operands.stream()
                .map(RexCall.class::cast)
                .collect(Collectors.toList());

        final List<RexInputRef> leftTableInputRefs = subConditions.stream()
                .map(sub -> sub.getOperands().stream()
                        .map(RexInputRef.class::cast)
                        .min((left, right) -> Integer.compare(left.getIndex(), right.getIndex()))
                        .get())
                .collect(Collectors.toList());

        final Integer[] leftTableKeyIndexes = leftTableInputRefs.stream()
                .map(RexInputRef::getIndex)
                .toArray(Integer[]::new);

        final List<RexInputRef> rightTableInputRefs = subConditions.stream()
                .map(sub -> sub.getOperands().stream()
                        .map(RexInputRef.class::cast)
                        .max((left, right) -> Integer.compare(left.getIndex(), right.getIndex()))
                        .get())
                .collect(Collectors.toList());

        final Integer[] rightTableKeyIndexes = rightTableInputRefs.stream()
                .map(RexInputRef::getIndex)
                .map(key -> key - wayangRelNode.getLeft().getRowType().getFieldCount())
                .toArray(Integer[]::new);

        final List<RelDataTypeField> leftFields = leftTableInputRefs.stream()
                .map(ref -> wayangRelNode.getLeft().getRowType().getFieldList().get(ref.getIndex()))
                .collect(Collectors.toList());

        final List<RelDataTypeField> rightFields = rightTableInputRefs.stream()
                .map(ref -> wayangRelNode.getRight().getRowType().getFieldList().get(ref.getIndex() - wayangRelNode.getLeft().getRowType().getFieldCount()))
                .collect(Collectors.toList());

        final String leftTableName = extractTableName(wayangRelNode.getLeft());
        final String rightTableName = extractTableName(wayangRelNode.getRight());

        final String leftFieldNames = leftFields.stream()
                .map(RelDataTypeField::getName)
                .collect(Collectors.joining(","));

        final String rightFieldNames = rightFields.stream()
                .map(RelDataTypeField::getName)
                .collect(Collectors.joining(","));

        final JoinOperator<Record, Record, Record> join = getJoinOperator(
                leftTableKeyIndexes,
                rightTableKeyIndexes,
                wayangRelNode,
                leftTableName,
                leftFieldNames,
                rightTableName,
                rightFieldNames);

        // records with a NULL in any key field never match, so they are dropped before the join
        filterNullJoinKeys(childOpLeft, wayangRelNode.getLeft(), leftTableKeyIndexes).connectTo(0, join, 0);
        filterNullJoinKeys(childOpRight, wayangRelNode.getRight(), rightTableKeyIndexes).connectTo(0, join, 1);

        final SerializableFunction<Tuple2<Record, Record>, Record> mp = new JoinFlattenResult();

        final MapOperator<Tuple2<Record, Record>, Record> mapOperator = new MapOperator<Tuple2<Record, Record>, Record>(
                mp,
                ReflectionUtils.specify(Tuple2.class),
                Record.class);

        join.connectTo(0, mapOperator, 0);

        return mapOperator;
    }

    private String extractTableName(org.apache.calcite.rel.RelNode relNode) {
        if (relNode instanceof WayangTableScan) {
            return ((WayangTableScan) relNode).getTableName();
        }
        if (relNode.getInputs() != null && !relNode.getInputs().isEmpty()) {
            return extractTableName(relNode.getInput(0));
        }
        return "UNKNOWN";
    }

    protected JoinOperator<Record, Record, Record> getJoinOperator(final Integer[] leftKeyIndexes,
            final Integer[] rightKeyIndexes,
            final WayangJoin wayangRelNode, final String leftTableName, final String leftFieldNames,
            final String rightTableName, final String rightFieldNames) {

        if (wayangRelNode.getInputs().size() != 2)
            throw new UnsupportedOperationException("Join had an unexpected amount of inputs, found: "
                    + wayangRelNode.getInputs().size() + ", expected: 2");

        final TransformationDescriptor<Record, Record> leftProjectionDescriptor = new TransformationDescriptor<Record, Record>(
                new MultiConditionJoinKeyExtractor(leftKeyIndexes),
                Record.class, Record.class)
                .withSqlImplementation(leftTableName, leftFieldNames);

        final TransformationDescriptor<Record, Record> rightProjectionDescriptor = new TransformationDescriptor<Record, Record>(
                new MultiConditionJoinKeyExtractor(rightKeyIndexes),
                Record.class, Record.class)
                .withSqlImplementation(rightTableName, rightFieldNames);

        final JoinOperator<Record, Record, Record> join = new JoinOperator<>(
                leftProjectionDescriptor,
                rightProjectionDescriptor);

        return join;
    }
}
