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

package org.apache.wayang.api.sql.context;

import java.util.List;

/**
 * Metadata for one table in the configured SQL catalog.
 */
public final class SqlTableMetadata {

    private final String name;

    private final String type;

    private final List<SqlColumn> columns;

    public SqlTableMetadata(
            final String name,
            final String type,
            final List<SqlColumn> columns
    ) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Table name must not be empty.");
        }
        if (type == null || type.isEmpty()) {
            throw new IllegalArgumentException("Table type must not be empty.");
        }
        if (columns == null) {
            throw new IllegalArgumentException("Columns must not be null.");
        }
        this.name = name;
        this.type = type;
        this.columns = List.copyOf(columns);
    }

    public String getName() {
        return this.name;
    }

    public String getType() {
        return this.type;
    }

    public List<SqlColumn> getColumns() {
        return this.columns;
    }
}
