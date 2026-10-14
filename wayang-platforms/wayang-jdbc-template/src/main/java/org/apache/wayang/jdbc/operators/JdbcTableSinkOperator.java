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

package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.util.SqlIdentifierUtils;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Abstract JDBC-based implementation of {@link TableSink} that operates within
 * the {@link org.apache.wayang.jdbc.channels.SqlQueryChannel} ecosystem.
 * Instead of pulling data into Java/Spark memory and inserting via JDBC,
 * this operator wraps the composed SQL query in a CREATE TABLE AS SELECT
 * or INSERT INTO ... SELECT statement, keeping all data within the database.
 */
public abstract class JdbcTableSinkOperator extends TableSink<Record> implements JdbcExecutionOperator {

    public JdbcTableSinkOperator(String tableName, String[] columnNames) {
        super(null, null, tableName, columnNames);
    }

    public JdbcTableSinkOperator(TableSink<Record> that) {
        super(that);
    }

    /**
     * Returns the escaped and quoted table name appropriate for the given database connection.
     *
     * @param connection database connection, or {@code null} for standard ANSI quoting
     * @return escaped and quoted table name
     */
    public String getQuotedTableName(Connection connection) {
        return SqlIdentifierUtils.escapeAndQuoteIdentifier(this.getTableName(), connection);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        String mode = this.getMode();
        String targetTable = this.getQuotedTableName(connection);
        if ("overwrite".equals(mode)) {
            return "CREATE TABLE " + targetTable + " AS";
        }
        return "INSERT INTO " + targetTable;
    }

    /**
     * Returns a SQL suffix appended after the composed SELECT query.
     * Default is empty, which works for most databases (PostgreSQL, SQLite, MySQL).
     * Subclasses can potentiallyoverride for dialect-specific syntax (e.g., HSQLDB that we used for the tests requires
     * parenthesized subquery form: {@code CREATE TABLE x AS (SELECT ...)}).
     */
    public String createSqlSuffix() {
        return "";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.getPlatform().getSqlQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.tablesink.load", this.getPlatform().getPlatformId());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        return JdbcExecutionOperator.super.createLoadProfileEstimator(configuration);
    }
}