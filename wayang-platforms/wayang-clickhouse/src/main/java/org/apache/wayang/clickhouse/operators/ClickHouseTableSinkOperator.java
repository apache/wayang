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
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcTableSinkOperator;

import java.sql.Connection;

/**
 * ClickHouse implementation of the {@link JdbcTableSinkOperator}.
 *
 * <p>ClickHouse requires an {@code ENGINE} clause for {@code CREATE TABLE ... AS
 * SELECT}. The default is {@value #DEFAULT_ENGINE}; override via the
 * {@code engine} constructor argument or the
 * {@code wayang.clickhouse.tablesink.engine} property when constructing the sink.
 */
public class ClickHouseTableSinkOperator extends JdbcTableSinkOperator implements ClickHouseExecutionOperator {

    public static final String DEFAULT_ENGINE = "MergeTree ORDER BY tuple()";

    public static final String ENGINE_PROPERTY = "wayang.clickhouse.tablesink.engine";

    private final String engine;

    public ClickHouseTableSinkOperator(String tableName, String[] columnNames) {
        this(tableName, columnNames, DEFAULT_ENGINE);
    }

    public ClickHouseTableSinkOperator(String tableName, String[] columnNames, String engine) {
        super(tableName, columnNames);
        this.engine = normalizeEngine(engine);
    }

    public ClickHouseTableSinkOperator(TableSink<Record> that) {
        super(that);
        this.engine = engineFrom(that);
    }

    public String getEngine() {
        return this.engine;
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        String mode = this.getMode();
        String targetTable = this.getQuotedTableName(connection);
        if ("overwrite".equals(mode)) {
            return "CREATE TABLE " + targetTable + " ENGINE = " + this.engine + " AS";
        }
        return "INSERT INTO " + targetTable;
    }

    private static String normalizeEngine(String engine) {
        if (engine == null || engine.trim().isEmpty()) {
            return DEFAULT_ENGINE;
        }
        return engine.trim();
    }

    private static String engineFrom(TableSink<Record> that) {
        if (that.getProperties() != null) {
            String configured = that.getProperties().getProperty(ENGINE_PROPERTY);
            if (configured != null && !configured.trim().isEmpty()) {
                return configured.trim();
            }
        }
        return DEFAULT_ENGINE;
    }
}
