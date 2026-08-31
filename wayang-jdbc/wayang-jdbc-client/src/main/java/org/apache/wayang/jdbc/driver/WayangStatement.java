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

package org.apache.wayang.jdbc.driver;

import org.apache.wayang.jdbc.protocol.message.QueryResultResponse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.UUID;

class WayangStatement implements Statement {

    private final WayangConnection connection;

    private final WayangJdbcClient client;

    private final String statementId;

    private int fetchSize;

    private int maxRows;

    private boolean poolable;

    private boolean closeOnCompletion;

    private volatile boolean closed;

    private ResultSet currentResultSet;

    WayangStatement(
            final WayangConnection connection,
            final WayangJdbcClient client
    ) {
        if (connection == null) {
            throw new IllegalArgumentException("Connection must not be null.");
        }
        if (client == null) {
            throw new IllegalArgumentException("JDBC client must not be null.");
        }
        this.connection = connection;
        this.client = client;
        this.statementId = UUID.randomUUID().toString();
    }

    String getStatementId() {
        return this.statementId;
    }

    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {
        this.ensureOpen();
        this.closeCurrentResultSet();
        this.ensureOpen();

        final QueryResultResponse response = this.client.executeQuery(this.statementId, sql, this.fetchSize);
        try {
            this.currentResultSet = WayangResultSet.create(
                    this,
                    this.client,
                    response,
                    this.fetchSize,
                    this.maxRows
            );
        } catch (SQLException e) {
            this.closeResponseCursor(response, e);
            throw e;
        }
        return this.currentResultSet;
    }

    @Override
    public boolean execute(final String sql) throws SQLException {
        this.executeQuery(sql);
        return true;
    }

    @Override
    public void close() throws SQLException {
        if (this.closed) {
            return;
        }
        this.closed = true;
        SQLException failure = null;
        try {
            this.closeCurrentResultSet();
        } catch (SQLException e) {
            failure = e;
        } finally {
            this.connection.statementClosed(this);
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        this.ensureOpen();
        return 0;
    }

    @Override
    public void setMaxFieldSize(final int max) throws SQLException {
        this.ensureOpen();
        if (max < 0) {
            throw new SQLException("Maximum field size must not be negative.");
        }
        if (max > 0) {
            throw this.unsupported("Maximum field size limiting is not supported.");
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        this.ensureOpen();
        return this.maxRows;
    }

    @Override
    public void setMaxRows(final int max) throws SQLException {
        this.ensureOpen();
        if (max < 0) {
            throw new SQLException("Maximum rows must not be negative.");
        }
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(final boolean enable) throws SQLException {
        this.ensureOpen();
        if (enable) {
            throw this.unsupported("JDBC escape processing is not supported.");
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        this.ensureOpen();
        return 0;
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        this.ensureOpen();
        if (seconds < 0) {
            throw new SQLException("Query timeout must not be negative.");
        }
        if (seconds > 0) {
            throw this.unsupported("Query timeouts are not supported.");
        }
    }

    @Override
    public void cancel() throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Query cancellation is not supported.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        this.ensureOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.ensureOpen();
    }

    @Override
    public void setCursorName(final String name) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Named cursors are not supported.");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        this.ensureOpen();
        return this.currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        this.ensureOpen();
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        this.ensureOpen();
        this.closeCurrentResultSet();
        return false;
    }

    @Override
    public boolean getMoreResults(final int current) throws SQLException {
        this.ensureOpen();
        if (current == Statement.CLOSE_CURRENT_RESULT || current == Statement.CLOSE_ALL_RESULTS) {
            this.closeCurrentResultSet();
        } else if (current != Statement.KEEP_CURRENT_RESULT) {
            throw new SQLException("Unsupported getMoreResults option: " + current);
        }
        return false;
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        this.ensureOpen();
        if (direction != ResultSet.FETCH_FORWARD
                && direction != ResultSet.FETCH_REVERSE
                && direction != ResultSet.FETCH_UNKNOWN) {
            throw new SQLException("Invalid fetch direction: " + direction);
        }
        if (direction != ResultSet.FETCH_FORWARD) {
            throw this.unsupported("Wayang JDBC supports only forward fetch direction.");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        this.ensureOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        this.ensureOpen();
        if (rows < 0) {
            throw new SQLException("Fetch size must not be negative.");
        }
        if (this.maxRows > 0 && rows > this.maxRows) {
            throw new SQLException("Fetch size must not exceed the maximum row limit.");
        }
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        this.ensureOpen();
        return this.fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        this.ensureOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        this.ensureOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        this.ensureOpen();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Connection getConnection() throws SQLException {
        this.ensureOpen();
        return this.connection;
    }

    @Override
    public boolean isClosed() {
        return this.closed || this.connection.isClosed();
    }

    @Override
    public void setPoolable(final boolean poolable) throws SQLException {
        this.ensureOpen();
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        this.ensureOpen();
        return this.poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.ensureOpen();
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        this.ensureOpen();
        return this.closeOnCompletion;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Generated keys are not supported.");
    }

    @Override
    public int executeUpdate(final String sql) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Wayang JDBC is read-only and does not support updates.");
    }

    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Wayang JDBC is read-only and does not support updates.");
    }

    @Override
    public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Wayang JDBC is read-only and does not support updates.");
    }

    @Override
    public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Wayang JDBC is read-only and does not support updates.");
    }

    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
        this.ensureOpen();
        if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
            return this.execute(sql);
        }
        if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS) {
            throw this.unsupported("Generated keys are not supported.");
        }
        throw new SQLException("Invalid auto-generated keys option: " + autoGeneratedKeys);
    }

    @Override
    public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Generated keys are not supported.");
    }

    @Override
    public boolean execute(final String sql, final String[] columnNames) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Generated keys are not supported.");
    }

    @Override
    public void addBatch(final String sql) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Batch execution is not supported.");
    }

    @Override
    public void clearBatch() throws SQLException {
        this.ensureOpen();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Batch execution is not supported.");
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        this.ensureOpen();
        return -1L;
    }

    @Override
    public void setLargeMaxRows(final long max) throws SQLException {
        this.ensureOpen();
        if (max < 0L || max > Integer.MAX_VALUE) {
            throw new SQLException("Large maximum rows must be between 0 and " + Integer.MAX_VALUE + ".");
        }
        this.maxRows = (int) max;
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        this.ensureOpen();
        return this.maxRows;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Batch execution is not supported.");
    }

    @Override
    public long executeLargeUpdate(final String sql) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Wayang JDBC is read-only and does not support updates.");
    }

    @Override
    public long executeLargeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Wayang JDBC is read-only and does not support updates.");
    }

    @Override
    public long executeLargeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Wayang JDBC is read-only and does not support updates.");
    }

    @Override
    public long executeLargeUpdate(final String sql, final String[] columnNames) throws SQLException {
        this.ensureOpen();
        throw this.unsupported("Wayang JDBC is read-only and does not support updates.");
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        this.ensureOpen();
        if (iface == null) {
            throw new SQLException("Wrapper interface must not be null.");
        }
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Statement does not wrap " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        this.ensureOpen();
        if (iface == null) {
            throw new SQLException("Wrapper interface must not be null.");
        }
        return iface.isInstance(this);
    }

    void resultSetClosed(final ResultSet resultSet) throws SQLException {
        if (this.currentResultSet == resultSet) {
            this.currentResultSet = null;
            if (this.closeOnCompletion && !this.closed) {
                this.close();
            }
        }
    }

    private void closeCurrentResultSet() throws SQLException {
        final ResultSet resultSet = this.currentResultSet;
        if (resultSet != null) {
            resultSet.close();
        }
        this.currentResultSet = null;
    }

    private void closeResponseCursor(
            final QueryResultResponse response,
            final SQLException failure
    ) {
        if (response == null
                || response.getCursorId() == null
                || response.getCursorId().isBlank()
                || this.client.isClosed()) {
            return;
        }
        try {
            this.client.closeCursor(this.statementId, response.getCursorId());
        } catch (SQLException closeException) {
            failure.addSuppressed(closeException);
        }
    }

    private void ensureOpen() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Wayang JDBC statement is closed.", "07000");
        }
    }

    private SQLFeatureNotSupportedException unsupported(final String message) {
        return new SQLFeatureNotSupportedException(message, "0A000");
    }
}
