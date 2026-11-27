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

import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import org.apache.wayang.api.sql.calcite.converter.functions.JoinFlattenResult;
import org.apache.wayang.api.sql.calcite.converter.functions.JoinKeyExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.ReflectionUtils;

public class WayangJoinVisitor extends WayangRelNodeVisitor<WayangJoin> {

    WayangJoinVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangJoin wayangRelNode) {
        final Operator childOpLeft = wayangRelConverter.convert(wayangRelNode.getInput(0));
        final Operator childOpRight = wayangRelConverter.convert(wayangRelNode.getInput(1));

        final RexNode condition = wayangRelNode.getCondition();
        final RexCall call = (RexCall) condition;

        final List<Integer> keys = call.getOperands().stream()
            .map(RexInputRef.class::cast)
            .map(RexInputRef::getIndex)
            .toList();

        assert (keys.size() == 2) : "Amount of keys found in join was not 2, got: " + keys.size();
        
        if (!condition.isA(SqlKind.EQUALS)) {
            throw new UnsupportedOperationException("Only equality joins supported");
        }

        // offset of the index in the right child
        final int offset = wayangRelNode.getInput(0).getRowType().getFieldCount();

        final int leftKeyIndex  = keys.get(0) < keys.get(1) ? keys.get(0)          : keys.get(1);
        final int rightKeyIndex = keys.get(0) < keys.get(1) ? keys.get(1) - offset : keys.get(0) - offset;
        
        final JoinOperator<Record, Record, Object> join = new JoinOperator<>(
                new TransformationDescriptor<>(new JoinKeyExtractor(leftKeyIndex), Record.class, Object.class),
                new TransformationDescriptor<>(new JoinKeyExtractor(rightKeyIndex), Record.class, Object.class));

        // call connectTo on both operators (left and right), without the records whose key is NULL
        filterNullJoinKeys(childOpLeft, wayangRelNode.getInput(0), leftKeyIndex).connectTo(0, join, 0);
        filterNullJoinKeys(childOpRight, wayangRelNode.getInput(1), rightKeyIndex).connectTo(0, join, 1);

        // Join returns Tuple2 - map to a Record
        final MapOperator<Tuple2<Record, Record>, Record> mapOperator = new MapOperator<>(
                new JoinFlattenResult(),
                ReflectionUtils.specify(Tuple2.class),
                Record.class);
                
        join.connectTo(0, mapOperator, 0);

        return mapOperator;
    }
}
