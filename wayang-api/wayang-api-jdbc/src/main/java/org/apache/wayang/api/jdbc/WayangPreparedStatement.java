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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class WayangPreparedStatement extends WayangStatement implements PreparedStatement {

    private final String sqlTemplate;
    private final List<Object> parameters;

    public WayangPreparedStatement(final WayangConnection connection, final String sql) {
        super(connection);
        this.sqlTemplate = sql;
        this.parameters = new ArrayList<>();
    }

    private String buildSql() throws SQLException {
        final StringBuilder result = new StringBuilder();
        int paramIndex = 0;
        boolean inQuote = false;
        for (int i = 0; i < sqlTemplate.length(); i++) {
            final char c = sqlTemplate.charAt(i);
            if (c == '\'') {
                inQuote = !inQuote;
                result.append(c);
            } else if (c == '?' && !inQuote) {
                if (paramIndex >= parameters.size())
                    throw new SQLException("Missing parameter at position " + (paramIndex + 1));
                result.append(formatParameter(parameters.get(paramIndex)));
                paramIndex++;
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    private String formatParameter(final Object value) {
        if (value == null) return "NULL";
        if (value instanceof String) return "'" + ((String) value).replace("'", "''") + "'";
        if (value instanceof java.sql.Date) return "'" + value + "'";
        if (value instanceof java.sql.Timestamp) return "'" + value + "'";
        if (value instanceof java.sql.Time) return "'" + value + "'";
        return value.toString();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(buildSql());
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(buildSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("Wayang does not support INSERT/UPDATE/DELETE");
    }

    private void setParam(final int parameterIndex, final Object value) throws SQLException {
        while (parameters.size() < parameterIndex) parameters.add(null);
        parameters.set(parameterIndex - 1, value);
    }

    @Override public void setNull(int i, int t) throws SQLException { setParam(i, null); }
    @Override public void setNull(int i, int t, String n) throws SQLException { setParam(i, null); }
    @Override public void setBoolean(int i, boolean x) throws SQLException { setParam(i, x); }
    @Override public void setByte(int i, byte x) throws SQLException { setParam(i, x); }
    @Override public void setShort(int i, short x) throws SQLException { setParam(i, x); }
    @Override public void setInt(int i, int x) throws SQLException { setParam(i, x); }
    @Override public void setLong(int i, long x) throws SQLException { setParam(i, x); }
    @Override public void setFloat(int i, float x) throws SQLException { setParam(i, x); }
    @Override public void setDouble(int i, double x) throws SQLException { setParam(i, x); }
    @Override public void setBigDecimal(int i, BigDecimal x) throws SQLException { setParam(i, x); }
    @Override public void setString(int i, String x) throws SQLException { setParam(i, x); }
    @Override public void setBytes(int i, byte[] x) throws SQLException { setParam(i, x); }
    @Override public void setDate(int i, Date x) throws SQLException { setParam(i, x); }
    @Override public void setDate(int i, Date x, Calendar c) throws SQLException { setParam(i, x); }
    @Override public void setTime(int i, Time x) throws SQLException { setParam(i, x); }
    @Override public void setTime(int i, Time x, Calendar c) throws SQLException { setParam(i, x); }
    @Override public void setTimestamp(int i, Timestamp x) throws SQLException { setParam(i, x); }
    @Override public void setTimestamp(int i, Timestamp x, Calendar c) throws SQLException { setParam(i, x); }
    @Override public void setObject(int i, Object x) throws SQLException { setParam(i, x); }
    @Override public void setObject(int i, Object x, int t) throws SQLException { setParam(i, x); }
    @Override public void setObject(int i, Object x, int t, int s) throws SQLException { setParam(i, x); }
    @Override public void clearParameters() throws SQLException { parameters.clear(); }

    @Override public ResultSetMetaData getMetaData() throws SQLException { return null; }
    @Override public ParameterMetaData getParameterMetaData() throws SQLException { throw new SQLFeatureNotSupportedException(); }

    @Override public void setAsciiStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setAsciiStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setUnicodeStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int i, InputStream x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBinaryStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int i, Reader x, int l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int i, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setCharacterStream(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setRef(int i, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int i, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int i, InputStream x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setBlob(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int i, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int i, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setClob(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setArray(int i, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setURL(int i, URL x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setRowId(int i, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNString(int i, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNCharacterStream(int i, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNCharacterStream(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int i, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int i, Reader x, long l) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setNClob(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void setSQLXML(int i, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void addBatch() throws SQLException { throw new SQLFeatureNotSupportedException(); }
}
