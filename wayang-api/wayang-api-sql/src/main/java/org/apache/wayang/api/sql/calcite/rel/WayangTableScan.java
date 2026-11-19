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

package org.apache.wayang.api.sql.calcite.rel;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Table;
import org.apache.wayang.api.sql.calcite.convention.WayangConvention;

import java.util.ArrayList;
import java.util.List;

public class WayangTableScan extends TableScan implements WayangRel {

    /**
     * Ordinals of the columns this scan reads. Defaults to every column of the table.
     */
    private final int[] fields;

    public WayangTableScan(RelOptCluster cluster,
                           RelTraitSet traitSet,
                           List<RelHint> hints,
                           RelOptTable table,
                           int[] fields) {
        super(cluster, traitSet, hints, table);
        if(fields==null) {
            int size = table.getRowType().getFieldCount();
            fields = new int[size];
            for (int i = 0; i < size; i++) {
                fields[i] = i;
            }
        }
        this.fields = fields;

    }

    public WayangTableScan(RelOptCluster cluster,
                           RelTraitSet traitSet,
                           List<RelHint> hints,
                           RelOptTable table) {
        this(cluster, traitSet, hints, table, null);
    }

    public static WayangTableScan create(RelOptCluster cluster, RelOptTable relOptTable) {
        return create(cluster, relOptTable, null);
    }


    public static WayangTableScan create(RelOptCluster cluster, RelOptTable relOptTable, int[] fields) {
        final Table table = relOptTable.unwrap(Table.class);
        Class elementType = Object.class;
        final RelTraitSet traitSet = cluster.traitSetOf(WayangConvention.INSTANCE)
                .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                    if (table != null) {
                        return table.getStatistic().getCollations();
                    }
                    return ImmutableList.of();
                });
        return new WayangTableScan(cluster, traitSet, ImmutableList.of(), relOptTable, fields);
    }

    @Override
    public String toString() {
        return "Wayang TableScan ["+ getQualifiedName() + "]";
    }

    public String getQualifiedName() {
        return table.getQualifiedName().size() == 1 
            ? table.getQualifiedName().get(0) 
            : table.getQualifiedName().get(1);
    }

    public String getTableName() {
        return table.getQualifiedName().size() == 1 
            ? table.getQualifiedName().get(0) 
            : table.getQualifiedName().get(1);
    }

    /**
     * @return the columns selected by {@link #fields}, in that order
     */
    public List<String> getColumnNames() {
        final List<String> names = table.getRowType().getFieldNames();
        final List<String> projected = new ArrayList<>(this.fields.length);
        for (int field : this.fields) {
            projected.add(names.get(field));
        }
        return projected;
    }

    public int[] getFields() {
        return this.fields.clone();
    }
}
