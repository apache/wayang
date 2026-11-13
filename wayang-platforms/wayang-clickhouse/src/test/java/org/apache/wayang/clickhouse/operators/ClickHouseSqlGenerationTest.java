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

package org.apache.wayang.clickhouse.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * SQL-generation unit tests for ClickHouse operators. These do not require a
 * live ClickHouse server.
 */
class ClickHouseSqlGenerationTest {

    private final FunctionCompiler compiler = new FunctionCompiler();

    @Test
    void tableSourceEmitsTableName() {
        ClickHouseTableSource source = new ClickHouseTableSource("analytics.orders", "order_id", "region");
        assertEquals("analytics.orders", source.createSqlClause(null, compiler));
        assertSame(org.apache.wayang.clickhouse.platform.ClickHousePlatform.getInstance(), source.getPlatform());
    }

    @Test
    void filterEmitsSqlPredicate() {
        ClickHouseFilterOperator filter = new ClickHouseFilterOperator(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> {
                            throw new UnsupportedOperationException();
                        },
                        Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        assertEquals("region = 'AMER'", filter.createSqlClause(null, compiler));
    }

    @Test
    void projectionEmitsFieldList() {
        ClickHouseProjectionOperator projection = new ClickHouseProjectionOperator("order_id", "amount");
        assertEquals("order_id, amount", projection.createSqlClause(null, compiler));
    }

    @Test
    void joinEmitsJoinClause() {
        ClickHouseJoinOperator<Record> join = new ClickHouseJoinOperator<>(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(0)), Record.class, Record.class
                ).withSqlImplementation("orders", "customer_id"),
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(0)), Record.class, Record.class
                ).withSqlImplementation("customers", "cust_id")
        );
        assertEquals("JOIN customers ON customers.cust_id=orders.customer_id",
                join.createSqlClause(null, compiler));
    }

    @Test
    void globalReduceEmitsAggregate() {
        ClickHouseGlobalReduceOperator reduce = new ClickHouseGlobalReduceOperator(
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount")
        );
        assertEquals("SUM(amount) AS total_amount", reduce.createSqlClause(null, compiler));
    }

    @Test
    void reduceByEmitsKeyAndAggregate() {
        ClickHouseReduceByOperator reduceBy = new ClickHouseReduceByOperator(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(0)), Record.class, Record.class
                ).withSqlImplementation("region", "region"),
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount")
        );
        assertEquals("region,SUM(amount) AS total_amount", reduceBy.createSqlClause(null, compiler));
    }

    @Test
    void sortEmitsOrderBy() {
        ClickHouseSortOperator sort = new ClickHouseSortOperator(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(0)), Record.class, Record.class
                ).withSqlImplementation("amount", "ASC")
        );
        assertEquals(" ORDER BY amount ASC", sort.createSqlClause(null, compiler));
    }

    @Test
    void tableSinkOverwriteIncludesEngine() {
        ClickHouseTableSinkOperator sink = new ClickHouseTableSinkOperator(
                "wayang_it.result", new String[]{"order_id", "amount"});
        sink.setMode("overwrite");
        assertEquals(
                "CREATE TABLE \"wayang_it\".\"result\" ENGINE = MergeTree ORDER BY tuple() AS",
                sink.createSqlClause(null, compiler));
    }

    @Test
    void tableSinkInsertOmitsEngine() {
        ClickHouseTableSinkOperator sink = new ClickHouseTableSinkOperator(
                "wayang_it.result", new String[]{"order_id", "amount"});
        sink.setMode("append");
        assertEquals("INSERT INTO \"wayang_it\".\"result\"", sink.createSqlClause(null, compiler));
    }

    @Test
    void tableSinkReadsEngineFromProperties() {
        Properties props = new Properties();
        props.setProperty(ClickHouseTableSinkOperator.ENGINE_PROPERTY,
                "ReplacingMergeTree ORDER BY order_id");
        TableSink<Record> logical = new TableSink<>(
                props, "overwrite", "wayang_it.result", "order_id", "amount");
        ClickHouseTableSinkOperator sink = new ClickHouseTableSinkOperator(logical);
        assertEquals(
                "CREATE TABLE \"wayang_it\".\"result\" ENGINE = ReplacingMergeTree ORDER BY order_id AS",
                sink.createSqlClause(null, compiler));
    }
}
