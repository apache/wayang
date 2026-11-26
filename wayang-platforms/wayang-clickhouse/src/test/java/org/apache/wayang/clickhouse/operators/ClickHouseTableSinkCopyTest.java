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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * Ensures {@link ClickHouseTableSinkOperator#createCopy()} preserves the ENGINE.
 */
class ClickHouseTableSinkCopyTest {

    @Test
    void createCopyPreservesCustomEngine() {
        ClickHouseTableSinkOperator original = new ClickHouseTableSinkOperator(
                "wayang_it.result",
                new String[]{"order_id"},
                "ReplacingMergeTree ORDER BY order_id");
        original.setMode("overwrite");

        ClickHouseTableSinkOperator copy = original.createCopy();

        assertNotSame(original, copy);
        assertEquals("ReplacingMergeTree ORDER BY order_id", copy.getEngine());
        assertEquals("overwrite", copy.getMode());
        assertEquals(
                "CREATE TABLE \"wayang_it\".\"result\" ENGINE = ReplacingMergeTree ORDER BY order_id AS",
                copy.createSqlClause(null, null));
    }
}
