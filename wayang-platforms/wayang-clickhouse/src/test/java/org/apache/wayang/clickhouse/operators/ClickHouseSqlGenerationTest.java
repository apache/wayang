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
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * SQL-generation unit tests for PR-1 ClickHouse operators. These do not require
 * a live ClickHouse server.
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
}
