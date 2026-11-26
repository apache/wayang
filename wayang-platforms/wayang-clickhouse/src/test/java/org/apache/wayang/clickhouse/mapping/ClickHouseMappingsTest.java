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

package org.apache.wayang.clickhouse.mapping;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.clickhouse.operators.ClickHouseFilterOperator;
import org.apache.wayang.clickhouse.operators.ClickHouseGlobalReduceOperator;
import org.apache.wayang.clickhouse.operators.ClickHouseJoinOperator;
import org.apache.wayang.clickhouse.operators.ClickHouseProjectionOperator;
import org.apache.wayang.clickhouse.operators.ClickHouseReduceByOperator;
import org.apache.wayang.clickhouse.operators.ClickHouseSortOperator;
import org.apache.wayang.clickhouse.operators.ClickHouseTableSinkOperator;
import org.apache.wayang.clickhouse.operators.ClickHouseTableSource;
import org.apache.wayang.clickhouse.platform.ClickHousePlatform;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Applies each ClickHouse {@link Mapping} to a small logical plan and checks
 * that the matching operator is replaced by its ClickHouse counterpart. No
 * ClickHouse server is needed.
 */
class ClickHouseMappingsTest {

    private final FunctionCompiler compiler = new FunctionCompiler();

    @Test
    void filterWithSqlImplementationIsMapped() {
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>((Record r) -> true, Record.class)
                        .withSqlImplementation("region = 'AMER'"));

        Operator mapped = this.applyToUnary(new FilterMapping(), filter);

        ClickHouseFilterOperator clickHouseFilter = assertInstanceOf(ClickHouseFilterOperator.class, mapped);
        assertEquals("region = 'AMER'", clickHouseFilter.createSqlClause(null, this.compiler));
        assertSame(ClickHousePlatform.getInstance(), clickHouseFilter.getPlatform());
    }

    @Test
    void filterWithoutSqlImplementationIsNotMapped() {
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>((Record r) -> true, Record.class));

        Operator mapped = this.applyToUnary(new FilterMapping(), filter);

        assertSame(filter, mapped);
    }

    @Test
    void projectionIsMapped() {
        MapOperator<Record, Record> projection = new MapOperator<>(
                ProjectionDescriptor.createForRecords(
                        new RecordType("order_id", "region", "amount"), "region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class));

        Operator mapped = this.applyToUnary(new ProjectionMapping(), projection);

        ClickHouseProjectionOperator clickHouseProjection =
                assertInstanceOf(ClickHouseProjectionOperator.class, mapped);
        assertEquals("region, amount", clickHouseProjection.createSqlClause(null, this.compiler));
    }

    @Test
    void mapWithoutProjectionDescriptorIsNotMapped() {
        MapOperator<Record, Record> map = new MapOperator<>(
                new TransformationDescriptor<>((Record r) -> r, Record.class, Record.class),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class));

        Operator mapped = this.applyToUnary(new ProjectionMapping(), map);

        assertSame(map, mapped);
    }

    @Test
    void globalReduceWithSqlImplementationIsMapped() {
        GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));

        Operator mapped = this.applyToUnary(new GlobalReduceMapping(), reduce);

        ClickHouseGlobalReduceOperator clickHouseReduce =
                assertInstanceOf(ClickHouseGlobalReduceOperator.class, mapped);
        assertEquals("SUM(amount) AS total_amount", clickHouseReduce.createSqlClause(null, this.compiler));
    }

    @Test
    void globalReduceWithoutSqlImplementationIsNotMapped() {
        GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                new ReduceDescriptor<>((a, b) -> a, Record.class),
                DataSetType.createDefault(Record.class));

        Operator mapped = this.applyToUnary(new GlobalReduceMapping(), reduce);

        assertSame(reduce, mapped);
    }

    @Test
    void reduceByWithSqlImplementationIsMapped() {
        ReduceByOperator<Record, Record> reduceBy = new ReduceByOperator<>(
                new TransformationDescriptor<>((Record r) -> r, Record.class, Record.class)
                        .withSqlImplementation("region", "region"),
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));

        Operator mapped = this.applyToUnary(new ReduceByMapping(), reduceBy);

        ClickHouseReduceByOperator clickHouseReduceBy =
                assertInstanceOf(ClickHouseReduceByOperator.class, mapped);
        assertEquals("region,SUM(amount) AS total_amount",
                clickHouseReduceBy.createSqlClause(null, this.compiler));
    }

    @Test
    void reduceByWithoutSqlAggregateIsNotMapped() {
        ReduceByOperator<Record, Record> reduceBy = new ReduceByOperator<>(
                new TransformationDescriptor<>((Record r) -> r, Record.class, Record.class)
                        .withSqlImplementation("region", "region"),
                new ReduceDescriptor<>((a, b) -> a, Record.class),
                DataSetType.createDefault(Record.class));

        Operator mapped = this.applyToUnary(new ReduceByMapping(), reduceBy);

        assertSame(reduceBy, mapped);
    }

    @Test
    void sortWithSqlImplementationIsMapped() {
        SortOperator<Record, Record> sort = new SortOperator<>(
                new TransformationDescriptor<>((Record r) -> r, Record.class, Record.class)
                        .withSqlImplementation("amount", "DESC"),
                DataSetType.createDefault(Record.class));

        Operator mapped = this.applyToUnary(new SortMapping(), sort);

        ClickHouseSortOperator clickHouseSort = assertInstanceOf(ClickHouseSortOperator.class, mapped);
        assertEquals(" ORDER BY amount DESC", clickHouseSort.createSqlClause(null, this.compiler));
    }

    @Test
    void sortWithoutSqlImplementationIsNotMapped() {
        SortOperator<Record, Record> sort = new SortOperator<>(
                new TransformationDescriptor<>((Record r) -> r, Record.class, Record.class),
                DataSetType.createDefault(Record.class));

        Operator mapped = this.applyToUnary(new SortMapping(), sort);

        assertSame(sort, mapped);
    }

    @Test
    void joinWithSqlImplementationIsMapped() {
        JoinOperator<Record, Record, Record> join = this.createJoin(true);

        Operator mapped = this.applyToJoin(join);

        ClickHouseJoinOperator<?> clickHouseJoin = assertInstanceOf(ClickHouseJoinOperator.class, mapped);
        assertEquals("JOIN customers ON customers.cust_id=orders.customer_id",
                clickHouseJoin.createSqlClause(null, this.compiler));
    }

    @Test
    void joinWithoutSqlImplementationIsNotMapped() {
        JoinOperator<Record, Record, Record> join = this.createJoin(false);

        Operator mapped = this.applyToJoin(join);

        assertSame(join, mapped);
    }

    @Test
    void tableSinkIsMappedAndKeepsEngineProperty() {
        Properties properties = new Properties();
        properties.setProperty(ClickHouseTableSinkOperator.ENGINE_PROPERTY, "Log");
        TableSink<Record> sink = new TableSink<>(properties, "overwrite", "wayang_it.result", "order_id");
        new ClickHouseTableSource("orders", "order_id").connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan(sink);

        this.apply(new TableSinkMapping(), plan);

        ClickHouseTableSinkOperator clickHouseSink = assertInstanceOf(
                ClickHouseTableSinkOperator.class, plan.getSinks().iterator().next());
        assertEquals("wayang_it.result", clickHouseSink.getTableName());
        assertEquals("CREATE TABLE \"wayang_it\".\"result\" ENGINE = Log AS",
                clickHouseSink.createSqlClause(null, this.compiler));
    }

    @Test
    void everyMappingTargetsTheClickHousePlatform() {
        assertFalse(Mappings.ALL.isEmpty());
        for (Mapping mapping : Mappings.ALL) {
            for (PlanTransformation transformation : mapping.getTransformations()) {
                assertEquals(1, transformation.getTargetPlatforms().size());
                assertSame(ClickHousePlatform.getInstance(),
                        transformation.getTargetPlatforms().iterator().next());
            }
        }
    }

    /**
     * Builds {@code source -> operator -> sink}, applies the mapping and
     * returns the operator that now feeds the sink.
     */
    private Operator applyToUnary(Mapping mapping, Operator operator) {
        ClickHouseTableSource source = new ClickHouseTableSource("orders", "order_id", "region", "amount");
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(new ArrayList<>(), Record.class);
        source.connectTo(0, operator, 0);
        operator.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan(sink);

        this.apply(mapping, plan);

        return plan.getSinks().iterator().next().getEffectiveOccupant(0).getOwner();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Operator applyToJoin(JoinOperator<Record, Record, Record> join) {
        ClickHouseTableSource orders = new ClickHouseTableSource("orders", "order_id", "customer_id");
        ClickHouseTableSource customers = new ClickHouseTableSource("customers", "cust_id", "name");
        LocalCallbackSink sink = LocalCallbackSink.createCollectingSink(
                new ArrayList<>(), DataSetType.createDefaultUnchecked(Tuple2.class));
        orders.connectTo(0, join, 0);
        customers.connectTo(0, join, 1);
        join.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan(sink);

        this.apply(new JoinMapping(), plan);

        return plan.getSinks().iterator().next().getEffectiveOccupant(0).getOwner();
    }

    private JoinOperator<Record, Record, Record> createJoin(boolean withSql) {
        TransformationDescriptor<Record, Record> leftKey = new TransformationDescriptor<>(
                (Record r) -> new Record(r.getField(1)), Record.class, Record.class);
        TransformationDescriptor<Record, Record> rightKey = new TransformationDescriptor<>(
                (Record r) -> new Record(r.getField(0)), Record.class, Record.class);
        if (withSql) {
            leftKey = leftKey.withSqlImplementation("orders", "customer_id");
            rightKey = rightKey.withSqlImplementation("customers", "cust_id");
        }
        return new JoinOperator<>(leftKey, rightKey);
    }

    private void apply(Mapping mapping, WayangPlan plan) {
        for (PlanTransformation transformation : mapping.getTransformations()) {
            transformation.thatReplaces().transform(plan, Operator.FIRST_EPOCH + 1);
        }
    }
}
