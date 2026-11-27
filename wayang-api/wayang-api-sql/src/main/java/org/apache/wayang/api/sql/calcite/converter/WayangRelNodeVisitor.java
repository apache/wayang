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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.apache.wayang.api.sql.calcite.converter.functions.JoinKeyNotNullFilter;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;

abstract class WayangRelNodeVisitor<T extends RelNode> {

    final WayangRelConverter wayangRelConverter;

    WayangRelNodeVisitor(WayangRelConverter wayangRelConverter) {
        this.wayangRelConverter = wayangRelConverter;
    }

    abstract Operator visit(T wayangRelNode);

    /**
     * Drops the records with a NULL join key before a join, as in SQL a NULL key never matches,
     * not even another NULL. If none of the key fields can be NULL, no filter is needed.
     *
     * @param input      operator that produces the join input
     * @param inputNode  the join input, for the nullability of its fields
     * @param keyIndexes indexes of the key fields in the join input
     * @return a filter on {@code input}, or {@code input} itself if no key field is nullable
     */
    static Operator filterNullJoinKeys(final Operator input, final RelNode inputNode, final Integer... keyIndexes) {
        final List<RelDataTypeField> fields = inputNode.getRowType().getFieldList();
        if (Arrays.stream(keyIndexes).noneMatch(index -> fields.get(index).getType().isNullable())) {
            return input;
        }

        final FilterOperator<Record> filter = new FilterOperator<>(new JoinKeyNotNullFilter(keyIndexes), Record.class);
        input.connectTo(0, filter, 0);

        return filter;
    }
}
