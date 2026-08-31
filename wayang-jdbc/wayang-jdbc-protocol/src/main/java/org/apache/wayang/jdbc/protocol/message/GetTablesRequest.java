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
 * Requests table metadata for {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}.
 */
public class GetTablesRequest {

    private String connectionId;

    private String catalogPattern;

    private String schemaPattern;

    private String tableNamePattern;

    private List<String> tableTypes = new ArrayList<>();

    public GetTablesRequest() {
    }

    public GetTablesRequest(
            final String connectionId,
            final String catalogPattern,
            final String schemaPattern,
            final String tableNamePattern,
            final List<String> tableTypes
    ) {
        this.connectionId = connectionId;
        this.catalogPattern = catalogPattern;
        this.schemaPattern = schemaPattern;
        this.tableNamePattern = tableNamePattern;
        this.tableTypes = tableTypes;
    }

    public String getConnectionId() {
        return this.connectionId;
    }

    public void setConnectionId(final String connectionId) {
        this.connectionId = connectionId;
    }

    public String getCatalogPattern() {
        return this.catalogPattern;
    }

    public void setCatalogPattern(final String catalogPattern) {
        this.catalogPattern = catalogPattern;
    }

    public String getSchemaPattern() {
        return this.schemaPattern;
    }

    public void setSchemaPattern(final String schemaPattern) {
        this.schemaPattern = schemaPattern;
    }

    public String getTableNamePattern() {
        return this.tableNamePattern;
    }

    public void setTableNamePattern(final String tableNamePattern) {
        this.tableNamePattern = tableNamePattern;
    }

    public List<String> getTableTypes() {
        return this.tableTypes;
    }

    public void setTableTypes(final List<String> tableTypes) {
        this.tableTypes = tableTypes;
    }
}
