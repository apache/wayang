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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class WayangResultSet implements ResultSet {

    private final List<Record> records;
    private int cursor = -1;
    private boolean closed = false;
    private boolean wasNull = false;
    private final String sql;
    private List<String> columnNames;

    public WayangResultSet(final Collection<Record> records, final String sql) {
        this.records = new ArrayList<>(records);
        this.sql = sql;
        this.columnNames = extractColumnNames();
    }

    private List<String> extractColumnNames() {
        final List<String> names = new ArrayList<>();
        if (!records.isEmpty()) {
            final Record lastRecord = records.get(records.size() - 1);
            boolean isHeader = false;
            if (lastRecord.getValues() != null) {
                isHeader = true;
                for (final Object val : lastRecord.getValues()) {
                    if (val != null && !(val instanceof String)) {
                        isHeader = false;
                        break;
                    }
                }
            }
            if (isHeader) {
                for (final Object val : lastRecord.getValues()) {
                    names.add(val != null ? val.toString() : "col_" + names.size());
                }
                records.remove(records.size() - 1);
            }
        }
        if (names.isEmpty() && !records.isEmpty()) {
            final int colCount = records.get(0).getValues().length;
            for (int i = 0; i < colCount; i++) names.add("col_" + i);
        }
        return names;
    }

    /**
     * Allows overriding column names externally (used by DatabaseMetaData methods).
     */
    public void overrideColumnNames(final List<String> names) {
        this.columnNames = new ArrayList<>(names);
    }

    private Record currentRecord() throws SQLException {
        if (cursor < 0 || cursor >= records.size())
            throw new SQLException("No current row. Call next() first.");
        return records.get(cursor);
    }

    private Object getValue(final int columnIndex) throws SQLException {
        final Object[] values = currentRecord().getValues();
        if (columnIndex < 1 || columnIndex > values.length)
            throw new SQLException("Column index out of range: " + columnIndex);
        final Object val = values[columnIndex - 1];
        wasNull = (val == null);
        return val;
    }

    private Object getValue(final String columnLabel) throws SQLException {
        final int index = columnNames.indexOf(columnLabel);
        if (index == -1) throw new SQLException("Column not found: " + columnLabel);
        return getValue(index + 1);
    }

    @Override public boolean next() throws SQLException { checkClosed(); cursor++; return cursor < records.size(); }
    @Override public boolean wasNull() throws SQLException { return wasNull; }
    @Override public boolean isBeforeFirst() throws SQLException { return cursor == -1; }
    @Override public boolean isAfterLast() throws SQLException { return cursor >= records.size(); }
    @Override public boolean isFirst() throws SQLException { return cursor == 0; }
    @Override public boolean isLast() throws SQLException { return cursor == records.size() - 1; }
    @Override public int getRow() throws SQLException { return cursor + 1; }
    @Override public void beforeFirst() throws SQLException { cursor = -1; }
    @Override public void afterLast() throws SQLException { cursor = records.size(); }
    @Override public boolean first() throws SQLException { cursor = 0; return !records.isEmpty(); }
    @Override public boolean last() throws SQLException { cursor = records.size() - 1; return !records.isEmpty(); }
    @Override public boolean absolute(int row) throws SQLException { cursor = row - 1; return cursor >= 0 && cursor < records.size(); }
    @Override public boolean relative(int rows) throws SQLException { cursor += rows; return cursor >= 0 && cursor < records.size(); }
    @Override public boolean previous() throws SQLException { cursor--; return cursor >= 0; }
    @Override public void close() throws SQLException { closed = true; }
    @Override public boolean isClosed() throws SQLException { return closed; }

    @Override public String getString(int i) throws SQLException { Object v = getValue(i); return v == null ? null : v.toString(); }
    @Override public String getString(String s) throws SQLException { Object v = getValue(s); return v == null ? null : v.toString(); }
    @Override public boolean getBoolean(int i) throws SQLException { Object v = getValue(i); if (v == null) return false; if (v instanceof Boolean) return (Boolean)v; return Boolean.parseBoolean(v.toString()); }
    @Override public boolean getBoolean(String s) throws SQLException { Object v = getValue(s); if (v == null) return false; if (v instanceof Boolean) return (Boolean)v; return Boolean.parseBoolean(v.toString()); }
    @Override public byte getByte(int i) throws SQLException { Object v = getValue(i); if (v == null) return 0; if (v instanceof Number) return ((Number)v).byteValue(); return Byte.parseByte(v.toString()); }
    @Override public byte getByte(String s) throws SQLException { Object v = getValue(s); if (v == null) return 0; if (v instanceof Number) return ((Number)v).byteValue(); return Byte.parseByte(v.toString()); }
    @Override public short getShort(int i) throws SQLException { Object v = getValue(i); if (v == null) return 0; if (v instanceof Number) return ((Number)v).shortValue(); return Short.parseShort(v.toString()); }
    @Override public short getShort(String s) throws SQLException { Object v = getValue(s); if (v == null) return 0; if (v instanceof Number) return ((Number)v).shortValue(); return Short.parseShort(v.toString()); }
    @Override public int getInt(int i) throws SQLException { Object v = getValue(i); if (v == null) return 0; if (v instanceof Number) return ((Number)v).intValue(); return Integer.parseInt(v.toString()); }
    @Override public int getInt(String s) throws SQLException { Object v = getValue(s); if (v == null) return 0; if (v instanceof Number) return ((Number)v).intValue(); return Integer.parseInt(v.toString()); }
    @Override public long getLong(int i) throws SQLException { Object v = getValue(i); if (v == null) return 0L; if (v instanceof Number) return ((Number)v).longValue(); return Long.parseLong(v.toString()); }
    @Override public long getLong(String s) throws SQLException { Object v = getValue(s); if (v == null) return 0L; if (v instanceof Number) return ((Number)v).longValue(); return Long.parseLong(v.toString()); }
    @Override public float getFloat(int i) throws SQLException { Object v = getValue(i); if (v == null) return 0f; if (v instanceof Number) return ((Number)v).floatValue(); return Float.parseFloat(v.toString()); }
    @Override public float getFloat(String s) throws SQLException { Object v = getValue(s); if (v == null) return 0f; if (v instanceof Number) return ((Number)v).floatValue(); return Float.parseFloat(v.toString()); }
    @Override public double getDouble(int i) throws SQLException { Object v = getValue(i); if (v == null) return 0.0; if (v instanceof Number) return ((Number)v).doubleValue(); return Double.parseDouble(v.toString()); }
    @Override public double getDouble(String s) throws SQLException { Object v = getValue(s); if (v == null) return 0.0; if (v instanceof Number) return ((Number)v).doubleValue(); return Double.parseDouble(v.toString()); }
    @Override public BigDecimal getBigDecimal(int i) throws SQLException { Object v = getValue(i); if (v == null) return null; if (v instanceof BigDecimal) return (BigDecimal)v; return new BigDecimal(v.toString()); }
    @Override public BigDecimal getBigDecimal(String s) throws SQLException { Object v = getValue(s); if (v == null) return null; if (v instanceof BigDecimal) return (BigDecimal)v; return new BigDecimal(v.toString()); }
    @Override public BigDecimal getBigDecimal(int i, int scale) throws SQLException { BigDecimal bd = getBigDecimal(i); return bd == null ? null : bd.setScale(scale); }
    @Override public BigDecimal getBigDecimal(String s, int scale) throws SQLException { BigDecimal bd = getBigDecimal(s); return bd == null ? null : bd.setScale(scale); }
    @Override public Object getObject(int i) throws SQLException { return getValue(i); }
    @Override public Object getObject(String s) throws SQLException { return getValue(s); }
    @Override public Object getObject(int i, Map<String,Class<?>> m) throws SQLException { return getObject(i); }
    @Override public Object getObject(String s, Map<String,Class<?>> m) throws SQLException { return getObject(s); }
    @Override public <T> T getObject(int i, Class<T> t) throws SQLException { Object v = getValue(i); if (v == null) return null; if (t.isInstance(v)) return t.cast(v); throw new SQLException("Cannot convert"); }
    @Override public <T> T getObject(String s, Class<T> t) throws SQLException { Object v = getValue(s); if (v == null) return null; if (t.isInstance(v)) return t.cast(v); throw new SQLException("Cannot convert"); }

    @Override public Date getDate(int i) throws SQLException { Object v = getValue(i); if (v == null) return null; if (v instanceof Date) return (Date)v; return Date.valueOf(v.toString()); }
    @Override public Date getDate(String s) throws SQLException { Object v = getValue(s); if (v == null) return null; if (v instanceof Date) return (Date)v; return Date.valueOf(v.toString()); }
    @Override public Date getDate(int i, Calendar c) throws SQLException { return getDate(i); }
    @Override public Date getDate(String s, Calendar c) throws SQLException { return getDate(s); }
    @Override public Time getTime(int i) throws SQLException { Object v = getValue(i); if (v == null) return null; if (v instanceof Time) return (Time)v; return Time.valueOf(v.toString()); }
    @Override public Time getTime(String s) throws SQLException { Object v = getValue(s); if (v == null) return null; if (v instanceof Time) return (Time)v; return Time.valueOf(v.toString()); }
    @Override public Time getTime(int i, Calendar c) throws SQLException { return getTime(i); }
    @Override public Time getTime(String s, Calendar c) throws SQLException { return getTime(s); }
    @Override public Timestamp getTimestamp(int i) throws SQLException { Object v = getValue(i); if (v == null) return null; if (v instanceof Timestamp) return (Timestamp)v; return Timestamp.valueOf(v.toString()); }
    @Override public Timestamp getTimestamp(String s) throws SQLException { Object v = getValue(s); if (v == null) return null; if (v instanceof Timestamp) return (Timestamp)v; return Timestamp.valueOf(v.toString()); }
    @Override public Timestamp getTimestamp(int i, Calendar c) throws SQLException { return getTimestamp(i); }
    @Override public Timestamp getTimestamp(String s, Calendar c) throws SQLException { return getTimestamp(s); }

    @Override public ResultSetMetaData getMetaData() throws SQLException { return new WayangResultSetMetaData(columnNames, records); }
    @Override public int findColumn(String s) throws SQLException { int i = columnNames.indexOf(s); if (i == -1) throw new SQLException("Column not found: " + s); return i + 1; }

    @Override public SQLWarning getWarnings() throws SQLException { return null; }
    @Override public void clearWarnings() throws SQLException {}
    @Override public void setFetchDirection(int d) throws SQLException {}
    @Override public int getFetchDirection() throws SQLException { return FETCH_FORWARD; }
    @Override public void setFetchSize(int r) throws SQLException {}
    @Override public int getFetchSize() throws SQLException { return 0; }
    @Override public int getType() throws SQLException { return TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() throws SQLException { return CONCUR_READ_ONLY; }
    @Override public int getHoldability() throws SQLException { return CLOSE_CURSORS_AT_COMMIT; }
    @Override public boolean rowUpdated() throws SQLException { return false; }
    @Override public boolean rowInserted() throws SQLException { return false; }
    @Override public boolean rowDeleted() throws SQLException { return false; }
    @Override public Statement getStatement() throws SQLException { return null; }

    @Override public byte[] getBytes(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public byte[] getBytes(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public InputStream getAsciiStream(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public InputStream getAsciiStream(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public InputStream getUnicodeStream(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public InputStream getUnicodeStream(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public InputStream getBinaryStream(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public InputStream getBinaryStream(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Reader getCharacterStream(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Reader getCharacterStream(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public String getCursorName() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Ref getRef(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Ref getRef(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Blob getBlob(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Blob getBlob(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Clob getClob(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Clob getClob(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Array getArray(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Array getArray(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public URL getURL(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public URL getURL(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob getNClob(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob getNClob(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLXML getSQLXML(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public SQLXML getSQLXML(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public String getNString(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public String getNString(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Reader getNCharacterStream(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public Reader getNCharacterStream(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public RowId getRowId(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public RowId getRowId(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override public void insertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void deleteRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void refreshRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void cancelRowUpdates() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void moveToInsertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void moveToCurrentRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNull(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(int i, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(int i, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(int i, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(int i, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(int i, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(int i, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(int i, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(int i, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(int i, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(int i, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(int i, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(int i, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(int i, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String s, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String s, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String s, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String s, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String s, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String s, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int i, Reader x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int i, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String s, Reader x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String s, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String s, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int i, Object x, int s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int i, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNull(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(String s, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(String s, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(String s, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(String s, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(String s, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(String s, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(String s, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(String s, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(String s, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(String s, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(String s, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(String s, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(String s, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String s, Object x, int sc) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String s, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRef(int i, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRef(String s, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int i, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String s, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String s, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String s, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int i, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String s, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int i, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String s, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String s, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(int i, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(String s, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRowId(int i, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRowId(String s, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNString(int i, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNString(String s, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int i, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String s, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int i, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String s, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String s, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(int i, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(String s, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(String s, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateSQLXML(int i, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateSQLXML(String s, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override public <T> T unwrap(Class<T> iface) throws SQLException { if (iface.isInstance(this)) return iface.cast(this); throw new SQLException("Cannot unwrap"); }
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return iface.isInstance(this); }

    private void checkClosed() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
    }
}