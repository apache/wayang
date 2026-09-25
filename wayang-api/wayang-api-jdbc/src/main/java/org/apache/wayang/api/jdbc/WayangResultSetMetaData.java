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

package org.apache.wayang.api.jdbc;

import org.apache.wayang.basic.data.Record;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * JDBC ResultSetMetaData implementation for Apache Wayang.
 *
 * Describes the columns in a WayangResultSet:
 * - How many columns are there?
 * - What are their names?
 * - What are their SQL types?
 *
 * Tools like DBeaver call this to render column headers in their UI.
 */
public class WayangResultSetMetaData implements ResultSetMetaData {

    /** Column names in order */
    private final List<String> columnNames;

    /** The actual data rows — used to infer column types */
    private final List<Record> records;

    public WayangResultSetMetaData(final List<String> columnNames, final List<Record> records) {
        this.columnNames = columnNames;
        this.records = records;
    }

    /**
     * Returns the number of columns in this ResultSet.
     */
    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    /**
     * Returns the name of the column at the given index (1-based).
     */
    @Override
    public String getColumnName(final int column) throws SQLException {
        checkColumn(column);
        return columnNames.get(column - 1);
    }

    /**
     * Returns the label (display name) of the column — same as name for Wayang.
     */
    @Override
    public String getColumnLabel(final int column) throws SQLException {
        return getColumnName(column);
    }

    /**
     * Infers the SQL type of a column by looking at the first non-null value.
     * Returns Types.VARCHAR as fallback if no data is available.
     */
    @Override
    public int getColumnType(final int column) throws SQLException {
        checkColumn(column);
        if (records.isEmpty()) return Types.VARCHAR;

        // Look at first row to infer type
        final Object val = records.get(0).getValues()[column - 1];
        if (val == null)        return Types.VARCHAR;
        if (val instanceof Integer || val instanceof Long)  return Types.BIGINT;
        if (val instanceof Double || val instanceof Float)  return Types.DOUBLE;
        if (val instanceof Boolean)                         return Types.BOOLEAN;
        if (val instanceof java.sql.Date)                   return Types.DATE;
        if (val instanceof java.sql.Timestamp)              return Types.TIMESTAMP;
        return Types.VARCHAR; // default — treat everything else as string
    }

    /**
     * Returns the SQL type name as a string e.g. "VARCHAR", "BIGINT".
     */
    @Override
    public String getColumnTypeName(final int column) throws SQLException {
        switch (getColumnType(column)) {
            case Types.BIGINT:    return "BIGINT";
            case Types.DOUBLE:    return "DOUBLE";
            case Types.BOOLEAN:   return "BOOLEAN";
            case Types.DATE:      return "DATE";
            case Types.TIMESTAMP: return "TIMESTAMP";
            default:              return "VARCHAR";
        }
    }

    /**
     * Returns the Java class name for the column type.
     */
    @Override
    public String getColumnClassName(final int column) throws SQLException {
        switch (getColumnType(column)) {
            case Types.BIGINT:    return Long.class.getName();
            case Types.DOUBLE:    return Double.class.getName();
            case Types.BOOLEAN:   return Boolean.class.getName();
            case Types.DATE:      return java.sql.Date.class.getName();
            case Types.TIMESTAMP: return java.sql.Timestamp.class.getName();
            default:              return String.class.getName();
        }
    }

    @Override
    public String getTableName(final int column) throws SQLException {
        return "wayang";
    }

    @Override
    public String getSchemaName(final int column) throws SQLException {
        return "wayang";
    }

    @Override
    public String getCatalogName(final int column) throws SQLException {
        return "wayang";
    }

    @Override
    public int getColumnDisplaySize(final int column) throws SQLException {
        return 255;
    }

    @Override
    public int getPrecision(final int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(final int column) throws SQLException {
        return 0;
    }

    @Override
    public boolean isAutoIncrement(final int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(final int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(final int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(final int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(final int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(final int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isReadOnly(final int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(final int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(final int column) throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    /** Validates column index is in range */
    private void checkColumn(final int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Column index out of range: " + column +
                    ". Total columns: " + columnNames.size());
        }
    }
}