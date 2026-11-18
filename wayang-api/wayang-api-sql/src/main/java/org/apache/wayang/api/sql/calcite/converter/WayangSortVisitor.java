/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.calcite.converter;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;

import org.apache.wayang.api.sql.calcite.converter.functions.SortFilter;
import org.apache.wayang.api.sql.calcite.converter.functions.SortKeyExtractor;
import org.apache.wayang.api.sql.calcite.rel.WayangSort;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.Operator;

public class WayangSortVisitor extends WayangRelNodeVisitor<WayangSort> {

    WayangSortVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangSort wayangRelNode) {
        assert (wayangRelNode.getInputs().size() == 1)
                : "Sorts must only have one input, but found: " + wayangRelNode.getInputs().size();

        final Operator childOp = wayangRelConverter.convert(wayangRelNode.getInput());

        // TODO: implement fetch & offset for java
        final RexLiteral fetch = (RexLiteral) wayangRelNode.fetch;
        final RexInputRef offset =  (RexInputRef) wayangRelNode.offset;

        // if (fetch != null || offset != null) throw new
        // UnsupportedOperationException("Offset and fetch currently not supported,
        // these appear via LIMIT statements in SQL");

        final RelCollation collation = wayangRelNode.getCollation();

        final List<Direction> collationDirections = collation.getFieldCollations().stream()
                .map(fieldCol -> fieldCol.getDirection())
                .collect(Collectors.toList());

        final List<NullDirection> nullDirections = collation.getFieldCollations().stream()
                .map(fieldCol -> fieldCol.nullDirection == NullDirection.UNSPECIFIED
                        ? fieldCol.getDirection().defaultNullDirection()
                        : fieldCol.nullDirection)
                .collect(Collectors.toList());

        final List<Integer> collationIndexes = collation.getFieldCollations().stream()
                .map(fieldCol -> fieldCol.getFieldIndex())
                .collect(Collectors.toList());

        final TransformationDescriptor<Record, Record> td = new TransformationDescriptor<Record, Record>(
                new SortKeyExtractor(
                        collationDirections,
                        nullDirections,
                        collationIndexes),
                Record.class, Record.class);

        final SortOperator<Record, Record> sort = new SortOperator<Record, Record>(td);

        childOp.connectTo(0, sort, 0);


        final SortFilter sortFilter = new SortFilter(
                fetch != null ? RexLiteral.intValue(fetch) : Integer.MAX_VALUE,
                offset != null ? RexLiteral.intValue(offset) : 0);

        final FilterOperator<Record> filter = new FilterOperator<Record>(sortFilter, Record.class);

        sort.connectTo(0, filter, 0);

        return filter;
    }

}
