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

import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.core.api.Configuration;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class WayangConnection implements Connection {

    private final String url;
    private final SqlContext sqlContext;
    private boolean closed = false;
    private final Properties properties;

    public WayangConnection(final String url, final String configPath, final Properties properties) throws SQLException {
        this.url = url;
        this.properties = properties != null ? properties : new Properties();
        try {
            final Configuration configuration;
            if (configPath != null && !configPath.isEmpty()) {
                configuration = new Configuration(configPath);
            } else {
                configuration = new Configuration();
            }
            this.sqlContext = new SqlContext(configuration);
        } catch (Exception e) {
            throw new SQLException("Failed to create WayangConnection: " + e.getMessage(), e);
        }
    }

    /**
     * Package-private constructor for testing.
     * Accepts a pre-built SqlContext directly, bypassing config file loading.
     */
    WayangConnection(final String url, final SqlContext sqlContext, final Properties properties) {
        this.url = url;
        this.properties = properties != null ? properties : new Properties();
        this.sqlContext = sqlContext;
    }

    public SqlContext getSqlContext() {
        return sqlContext;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new WayangStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        checkClosed();
        return new WayangPreparedStatement(this, sql);
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public boolean isValid(final int timeout) throws SQLException {
        return !closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new WayangDatabaseMetaData(this);
    }

    @Override
    public String getCatalog() throws SQLException {
        return "wayang";
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {}

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(final int level) throws SQLException {}

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {}

    @Override
    public void commit() throws SQLException {}

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Wayang does not support transactions");
    }

    @Override
    public void rollback(final Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Wayang does not support transactions");
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency,
            final int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType,
            final int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int resultSetType,
            final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(final String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Wayang does not support stored procedures");
    }

    @Override
    public CallableStatement prepareCall(final String sql, final int resultSetType,
            final int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Wayang does not support stored procedures");
    }

    @Override
    public CallableStatement prepareCall(final String sql, final int resultSetType,
            final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Wayang does not support stored procedures");
    }

    @Override
    public String nativeSQL(final String sql) throws SQLException {
        return sql;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {}

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeMap not supported");
    }

    @Override
    public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTypeMap not supported");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setHoldability(final int holdability) throws SQLException {}

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Wayang does not support savepoints");
    }

    @Override
    public Savepoint setSavepoint(final String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Wayang does not support savepoints");
    }

    @Override
    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Wayang does not support savepoints");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {}

    @Override
    public String getSchema() throws SQLException {
        return "wayang";
    }

    @Override
    public void setSchema(final String schema) throws SQLException {}

    @Override
    public void abort(final Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(final Executor executor, final int milliseconds) throws SQLException {}

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML not supported");
    }

    @Override
    public java.sql.Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf not supported");
    }

    @Override
    public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct not supported");
    }

    @Override
    public void setClientInfo(final String name, final String value) throws SQLClientInfoException {}

    @Override
    public void setClientInfo(final Properties properties) throws SQLClientInfoException {}

    @Override
    public String getClientInfo(final String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
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

    private void checkClosed() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
    }
}