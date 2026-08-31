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
 * Requests schema metadata for {@link java.sql.DatabaseMetaData#getSchemas()}.
 */
public class GetSchemasRequest {

    private String connectionId;

    private String catalog;

    private String schemaPattern;

    public GetSchemasRequest() {
    }

    public GetSchemasRequest(final String connectionId) {
        this.connectionId = connectionId;
    }

    public GetSchemasRequest(
            final String connectionId,
            final String catalog,
            final String schemaPattern
    ) {
        this.connectionId = connectionId;
        this.catalog = catalog;
        this.schemaPattern = schemaPattern;
    }

    public String getConnectionId() {
        return this.connectionId;
    }

    public void setConnectionId(final String connectionId) {
        this.connectionId = connectionId;
    }

    public String getCatalog() {
        return this.catalog;
    }

    public void setCatalog(final String catalog) {
        this.catalog = catalog;
    }

    public String getSchemaPattern() {
        return this.schemaPattern;
    }

    public void setSchemaPattern(final String schemaPattern) {
        this.schemaPattern = schemaPattern;
    }
}
