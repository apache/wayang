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

package org.apache.wayang.jdbc.protocol.message;

/**
 * Sends a SQL query from the JDBC driver to the Wayang JDBC server.
 */
public class ExecuteQueryRequest {

    private String connectionId;

    private String statementId;

    private String sql;

    private int fetchSize;

    public ExecuteQueryRequest() {
    }

    public ExecuteQueryRequest(
            final String connectionId,
            final String statementId,
            final String sql,
            final int fetchSize
    ) {
        this.connectionId = connectionId;
        this.statementId = statementId;
        this.sql = sql;
        this.fetchSize = fetchSize;
    }

    public String getConnectionId() {
        return this.connectionId;
    }

    public void setConnectionId(final String connectionId) {
        this.connectionId = connectionId;
    }

    public String getStatementId() {
        return this.statementId;
    }

    public void setStatementId(final String statementId) {
        this.statementId = statementId;
    }

    public String getSql() {
        return this.sql;
    }

    public void setSql(final String sql) {
        this.sql = sql;
    }

    public int getFetchSize() {
        return this.fetchSize;
    }

    public void setFetchSize(final int fetchSize) {
        this.fetchSize = fetchSize;
    }
}
