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

/**
 * Metadata for a column produced by the Wayang SQL API.
 */
public class SqlColumn {

    private final String name;

    private final String label;

    private final String typeName;

    private final int jdbcType;

    private final int precision;

    private final int scale;

    private final boolean nullable;

    public SqlColumn(
            final String name,
            final String label,
            final String typeName,
            final int jdbcType,
            final int precision,
            final int scale,
            final boolean nullable
    ) {
        this.name = name;
        this.label = label;
        this.typeName = typeName;
        this.jdbcType = jdbcType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    public String getName() {
        return this.name;
    }

    public String getLabel() {
        return this.label;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public int getJdbcType() {
        return this.jdbcType;
    }

    public int getPrecision() {
        return this.precision;
    }

    public int getScale() {
        return this.scale;
    }

    public boolean isNullable() {
        return this.nullable;
    }
}
