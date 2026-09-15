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

package org.apache.wayang.jdbc.server;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.api.sql.context.SqlQueryResult;
import org.apache.wayang.core.api.Configuration;

import java.sql.SQLException;

/**
 * Delegates JDBC server query execution to the Wayang SQL API.
 */
public class WayangSqlQueryExecutor implements SqlQueryExecutor {

    private final SqlContext sqlContext;

    public WayangSqlQueryExecutor(final Configuration configuration) throws SQLException {
        this(new SqlContext(configuration));
    }

    public WayangSqlQueryExecutor(final SqlContext sqlContext) {
        if (sqlContext == null) {
            throw new IllegalArgumentException("SQL context must not be null.");
        }
        this.sqlContext = sqlContext;
    }

    /**
     * Returns the SQL context used by this executor so that server components
     * can use the same configured Calcite catalog for metadata discovery.
     */
    public SqlContext getSqlContext() {
        return this.sqlContext;
    }

    @Override
    public SqlQueryResult execute(final String sql) throws SqlParseException {
        return this.sqlContext.executeSqlWithMetadata(sql);
    }
}
