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

import java.util.ArrayList;
import java.util.List;

/**
 * Carries the first batch of rows and metadata for a query.
 */
public class QueryResultResponse {

    private String connectionId;

    private String statementId;

    private List<ColumnInfo> columns = new ArrayList<>();

    private List<List<Object>> rows = new ArrayList<>();

    private boolean hasMoreRows;

    private String cursorId;

    public QueryResultResponse() {
    }

    public QueryResultResponse(
            final String connectionId,
            final String statementId,
            final List<ColumnInfo> columns,
            final List<List<Object>> rows,
            final boolean hasMoreRows,
            final String cursorId
    ) {
        this.connectionId = connectionId;
        this.statementId = statementId;
        this.columns = columns;
        this.rows = rows;
        this.hasMoreRows = hasMoreRows;
        this.cursorId = cursorId;
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

    public List<ColumnInfo> getColumns() {
        return this.columns;
    }

    public void setColumns(final List<ColumnInfo> columns) {
        this.columns = columns;
    }

    public List<List<Object>> getRows() {
        return this.rows;
    }

    public void setRows(final List<List<Object>> rows) {
        this.rows = rows;
    }

    public boolean isHasMoreRows() {
        return this.hasMoreRows;
    }

    public void setHasMoreRows(final boolean hasMoreRows) {
        this.hasMoreRows = hasMoreRows;
    }

    public String getCursorId() {
        return this.cursorId;
    }

    public void setCursorId(final String cursorId) {
        this.cursorId = cursorId;
    }
}
