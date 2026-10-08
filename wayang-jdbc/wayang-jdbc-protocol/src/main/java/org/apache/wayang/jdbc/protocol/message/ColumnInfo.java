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
 * JDBC-facing metadata for one result column.
 */
public class ColumnInfo {

    private String columnName;

    private String columnLabel;

    private String tableName;

    private String schemaName;

    private String typeName;

    private int jdbcType;

    private int nullable;

    private int precision;

    private int scale;

    public ColumnInfo() {
    }

    public ColumnInfo(
            final String columnName,
            final String columnLabel,
            final String tableName,
            final String schemaName,
            final String typeName,
            final int jdbcType,
            final int nullable,
            final int precision,
            final int scale
    ) {
        this.columnName = columnName;
        this.columnLabel = columnLabel;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.typeName = typeName;
        this.jdbcType = jdbcType;
        this.nullable = nullable;
        this.precision = precision;
        this.scale = scale;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public void setColumnName(final String columnName) {
        this.columnName = columnName;
    }

    public String getColumnLabel() {
        return this.columnLabel;
    }

    public void setColumnLabel(final String columnLabel) {
        this.columnLabel = columnLabel;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTypeName() {
        return this.typeName;
    }

    public void setTypeName(final String typeName) {
        this.typeName = typeName;
    }

    public int getJdbcType() {
        return this.jdbcType;
    }

    public void setJdbcType(final int jdbcType) {
        this.jdbcType = jdbcType;
    }

    public int getNullable() {
        return this.nullable;
    }

    public void setNullable(final int nullable) {
        this.nullable = nullable;
    }

    public int getPrecision() {
        return this.precision;
    }

    public void setPrecision(final int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return this.scale;
    }

    public void setScale(final int scale) {
        this.scale = scale;
    }
}
