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

package org.apache.wayang.duckdb.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.jdbc.operators.JdbcTableSinkOperator;

/**
 * DuckDB implementation of the {@link JdbcTableSinkOperator}. The sink stays
 * entirely within DuckDB: the composed query is wrapped in a
 * {@code CREATE TABLE ... AS} (mode {@code overwrite}) or {@code INSERT INTO ...}
 * statement.
 *
 * <p>Table names can be unqualified or schema-qualified, e.g.
 * {@code orders} or {@code main.orders}.
 */
public class DuckDBTableSinkOperator extends JdbcTableSinkOperator implements DuckDBExecutionOperator {

    public DuckDBTableSinkOperator(String tableName, String[] columnNames) {
        super(tableName, columnNames);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public DuckDBTableSinkOperator(TableSink<Record> that) {
        super(that);
    }
}
