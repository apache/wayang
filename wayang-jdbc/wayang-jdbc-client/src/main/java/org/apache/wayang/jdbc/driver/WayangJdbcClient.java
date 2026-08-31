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

import org.apache.wayang.jdbc.protocol.MessageEnvelope;
import org.apache.wayang.jdbc.protocol.MessageType;
import org.apache.wayang.jdbc.protocol.io.ProtocolMessageCodec;
import org.apache.wayang.jdbc.protocol.message.CancelQueryRequest;
import org.apache.wayang.jdbc.protocol.message.CancelQueryResponse;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionRequest;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionResponse;
import org.apache.wayang.jdbc.protocol.message.CloseCursorRequest;
import org.apache.wayang.jdbc.protocol.message.CloseCursorResponse;
import org.apache.wayang.jdbc.protocol.message.ErrorCode;
import org.apache.wayang.jdbc.protocol.message.ErrorResponse;
import org.apache.wayang.jdbc.protocol.message.ExecuteQueryRequest;
import org.apache.wayang.jdbc.protocol.message.FetchRequest;
import org.apache.wayang.jdbc.protocol.message.FetchResponse;
import org.apache.wayang.jdbc.protocol.message.GetColumnsRequest;
import org.apache.wayang.jdbc.protocol.message.GetSchemasRequest;
import org.apache.wayang.jdbc.protocol.message.GetTablesRequest;
import org.apache.wayang.jdbc.protocol.message.MetadataResultResponse;
import org.apache.wayang.jdbc.protocol.message.MetadataType;
import org.apache.wayang.jdbc.protocol.message.OpenConnectionRequest;
import org.apache.wayang.jdbc.protocol.message.OpenConnectionResponse;
import org.apache.wayang.jdbc.protocol.message.PingRequest;
import org.apache.wayang.jdbc.protocol.message.PingResponse;
import org.apache.wayang.jdbc.protocol.message.QueryResultResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

class WayangJdbcClient implements AutoCloseable {

    private static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 10_000;

    private final Socket socket;

    private final ProtocolMessageCodec codec;

    private final String connectionId;

    private final String serverVersion;

    private volatile boolean closed;

    WayangJdbcClient(final WayangJdbcUrl jdbcUrl) throws SQLException {
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("JDBC URL must not be null.");
        }

        this.codec = new ProtocolMessageCodec();
        this.socket = this.openSocket(jdbcUrl);
        final String openedConnectionId;
        final String openedServerVersion;
        try {
            final Properties properties = jdbcUrl.getProperties();
            final OpenConnectionRequest request = new OpenConnectionRequest(
                    properties.getProperty("user"),
                    jdbcUrl.getDatabase(),
                    this.toStringMap(properties)
            );

            final OpenConnectionResponse response = this.sendAndReceive(
                    MessageType.OPEN_CONNECTION,
                    request,
                    MessageType.OPEN_CONNECTION_OK,
                    OpenConnectionResponse.class
            );
            if (response.getConnectionId() == null || response.getConnectionId().isBlank()) {
                throw this.protocolFailure(
                        "Wayang JDBC server returned an invalid connection id."
                );
            }
            openedConnectionId = response.getConnectionId();
            openedServerVersion = response.getServerVersion();
            this.socket.setSoTimeout(0);
        } catch (SQLException e) {
            this.markBroken();
            throw e;
        } catch (SocketException e) {
            throw this.communicationFailure(
                    "Could not reset the Wayang JDBC socket after the initial handshake.",
                    e
            );
        }
        this.connectionId = openedConnectionId;
        this.serverVersion = openedServerVersion;
    }

    String getConnectionId() {
        return this.connectionId;
    }

    String getServerVersion() {
        return this.serverVersion;
    }

    boolean isClosed() {
        return this.closed || this.socket.isClosed();
    }

    synchronized boolean ping(final int timeoutSeconds) throws SQLException {
        if (timeoutSeconds < 0) {
            throw new SQLException("Ping timeout must not be negative.", "HY092");
        }
        this.ensureOpen();

        final boolean hasTemporaryTimeout = timeoutSeconds > 0;
        final int previousTimeout;
        try {
            previousTimeout = this.socket.getSoTimeout();
            if (hasTemporaryTimeout) {
                this.socket.setSoTimeout(this.secondsToMilliseconds(timeoutSeconds));
            }
        } catch (SocketException e) {
            throw this.communicationFailure(
                    "Could not configure the Wayang JDBC ping timeout.",
                    e
            );
        }

        SQLException failure = null;
        try {
            final PingResponse response = this.sendAndReceive(
                    MessageType.PING,
                    new PingRequest(this.connectionId),
                    MessageType.PING_OK,
                    PingResponse.class
            );
            this.validateIdentifier(response.getConnectionId(), this.connectionId, "connection");
        } catch (SQLException e) {
            failure = e;
        }

        if (hasTemporaryTimeout && !this.socket.isClosed()) {
            try {
                this.socket.setSoTimeout(previousTimeout);
            } catch (SocketException e) {
                final SQLException resetFailure = this.communicationFailure(
                        "Could not restore the Wayang JDBC socket timeout after ping.",
                        e
                );
                if (failure == null) {
                    failure = resetFailure;
                } else {
                    failure.addSuppressed(resetFailure);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
        return true;
    }

    synchronized QueryResultResponse executeQuery(
            final String statementId,
            final String sql,
            final int fetchSize
    ) throws SQLException {
        this.requireText(statementId, "Statement id");
        this.requireText(sql, "SQL query");

        final QueryResultResponse response = this.sendAndReceive(
                MessageType.EXECUTE_QUERY,
                new ExecuteQueryRequest(this.connectionId, statementId, sql, fetchSize),
                MessageType.QUERY_RESULT,
                QueryResultResponse.class
        );
        this.validateQueryResponse(response, statementId);
        return response;
    }

    synchronized FetchResponse fetch(
            final String cursorId,
            final int fetchSize
    ) throws SQLException {
        this.requireText(cursorId, "Cursor id");

        final FetchResponse response = this.sendAndReceive(
                MessageType.FETCH,
                new FetchRequest(this.connectionId, cursorId, fetchSize),
                MessageType.FETCH_RESULT,
                FetchResponse.class
        );
        this.validateFetchResponse(response, cursorId);
        return response;
    }

    synchronized void closeCursor(
            final String statementId,
            final String cursorId
    ) throws SQLException {
        this.requireText(statementId, "Statement id");
        this.requireText(cursorId, "Cursor id");

        final CloseCursorResponse response = this.sendAndReceive(
                MessageType.CLOSE_CURSOR,
                new CloseCursorRequest(this.connectionId, statementId, cursorId),
                MessageType.CLOSE_CURSOR_OK,
                CloseCursorResponse.class
        );
        this.validateIdentifier(response.getConnectionId(), this.connectionId, "connection");
        this.validateIdentifier(response.getStatementId(), statementId, "statement");
        this.validateIdentifier(response.getCursorId(), cursorId, "cursor");
    }

    synchronized CancelQueryResponse cancelQuery(
            final String statementId,
            final String cursorId
    ) throws SQLException {
        this.requireText(statementId, "Statement id");

        final CancelQueryResponse response = this.sendAndReceive(
                MessageType.CANCEL_QUERY,
                new CancelQueryRequest(this.connectionId, statementId, cursorId),
                MessageType.CANCEL_QUERY_OK,
                CancelQueryResponse.class
        );
        this.validateIdentifier(response.getConnectionId(), this.connectionId, "connection");
        this.validateIdentifier(response.getStatementId(), statementId, "statement");
        this.validateIdentifier(response.getCursorId(), cursorId, "cursor");
        return response;
    }

    synchronized MetadataResultResponse getSchemas(
            final String catalog,
            final String schemaPattern
    ) throws SQLException {
        final MetadataResultResponse response = this.sendAndReceive(
                MessageType.GET_SCHEMAS,
                new GetSchemasRequest(this.connectionId, catalog, schemaPattern),
                MessageType.METADATA_RESULT,
                MetadataResultResponse.class
        );
        this.validateMetadataResponse(response, MetadataType.SCHEMAS, 2);
        return response;
    }

    synchronized MetadataResultResponse getTables(
            final String catalog,
            final String schemaPattern,
            final String tableNamePattern,
            final String[] tableTypes
    ) throws SQLException {
        final MetadataResultResponse response = this.sendAndReceive(
                MessageType.GET_TABLES,
                new GetTablesRequest(
                        this.connectionId,
                        catalog,
                        schemaPattern,
                        tableNamePattern,
                        this.toList(tableTypes)
                ),
                MessageType.METADATA_RESULT,
                MetadataResultResponse.class
        );
        this.validateMetadataResponse(response, MetadataType.TABLES, 10);
        return response;
    }

    synchronized MetadataResultResponse getColumns(
            final String catalog,
            final String schemaPattern,
            final String tableNamePattern,
            final String columnNamePattern
    ) throws SQLException {
        final MetadataResultResponse response = this.sendAndReceive(
                MessageType.GET_COLUMNS,
                new GetColumnsRequest(
                        this.connectionId,
                        catalog,
                        schemaPattern,
                        tableNamePattern,
                        columnNamePattern
                ),
                MessageType.METADATA_RESULT,
                MetadataResultResponse.class
        );
        this.validateMetadataResponse(response, MetadataType.COLUMNS, 24);
        return response;
    }

    @Override
    public synchronized void close() throws SQLException {
        if (this.closed) {
            return;
        }

        SQLException failure = null;
        try {
            final CloseConnectionResponse response = this.sendAndReceive(
                    MessageType.CLOSE_CONNECTION,
                    new CloseConnectionRequest(this.connectionId),
                    MessageType.CLOSE_CONNECTION_OK,
                    CloseConnectionResponse.class
            );
            this.validateIdentifier(
                    response.getConnectionId(),
                    this.connectionId,
                    "connection"
            );
        } catch (SQLException e) {
            failure = e;
        } finally {
            this.closed = true;
            try {
                this.socket.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = new SQLException("Could not close Wayang JDBC socket.", "08006", e);
                }
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    private Socket openSocket(final WayangJdbcUrl jdbcUrl) throws SQLException {
        final int connectTimeout = this.connectTimeout(jdbcUrl.getProperties());
        final Socket openedSocket = new Socket();
        try {
            openedSocket.connect(
                    new InetSocketAddress(jdbcUrl.getHost(), jdbcUrl.getPort()),
                    connectTimeout
            );
            openedSocket.setSoTimeout(connectTimeout);
            return openedSocket;
        } catch (IOException e) {
            try {
                openedSocket.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw new SQLException("Could not connect to Wayang JDBC server.", "08001", e);
        }
    }

    private int connectTimeout(final Properties properties) throws SQLException {
        final String configuredValue = properties.getProperty("connectTimeout");
        if (configuredValue != null && !configuredValue.isBlank()) {
            final long configuredTimeout;
            try {
                configuredTimeout = Long.parseLong(configuredValue.trim());
            } catch (NumberFormatException e) {
                throw new SQLException(
                        "Wayang JDBC connectTimeout must be an integer number of milliseconds.",
                        "HY092",
                        e
                );
            }
            if (configuredTimeout < 0 || configuredTimeout > Integer.MAX_VALUE) {
                throw new SQLException(
                        "Wayang JDBC connectTimeout must be between 0 and "
                                + Integer.MAX_VALUE + " milliseconds.",
                        "HY092"
                );
            }
            if (configuredTimeout > 0) {
                return (int) configuredTimeout;
            }
        }

        final int loginTimeoutSeconds = DriverManager.getLoginTimeout();
        if (loginTimeoutSeconds > 0) {
            return this.secondsToMilliseconds(loginTimeoutSeconds);
        }
        return DEFAULT_CONNECT_TIMEOUT_MILLIS;
    }

    private int secondsToMilliseconds(final int seconds) {
        return (int) Math.min(
                Integer.MAX_VALUE,
                Math.multiplyExact((long) seconds, 1_000L)
        );
    }

    private synchronized <T> T sendAndReceive(
            final MessageType requestType,
            final Object requestPayload,
            final MessageType expectedResponseType,
            final Class<T> responseClass
    ) throws SQLException {
        this.ensureOpen();

        final String requestId = UUID.randomUUID().toString();
        try {
            this.codec.write(
                    this.socket.getOutputStream(),
                    this.codec.toEnvelope(requestId, requestType, requestPayload)
            );

            final MessageEnvelope response = this.codec.read(this.socket.getInputStream());
            if (response == null) {
                throw this.communicationFailure(
                        "Wayang JDBC server closed the connection.",
                        null
                );
            }

            if (!requestId.equals(response.getRequestId())) {
                throw this.protocolFailure(
                        "Wayang JDBC server returned a mismatched request id."
                );
            }

            if (response.getType() == MessageType.ERROR) {
                final ErrorResponse errorResponse =
                        this.codec.payloadAs(response, ErrorResponse.class);
                if (errorResponse == null) {
                    throw this.protocolFailure(
                            "Wayang JDBC server returned an empty error response."
                    );
                }
                // A well-formed server error describes a request failure and
                // does not make the protocol connection unusable.
                throw this.toSqlException(errorResponse);
            }

            if (response.getType() != expectedResponseType) {
                throw this.protocolFailure(
                        "Wayang JDBC server returned unexpected response type: "
                                + response.getType()
                );
            }

            final T responsePayload = this.codec.payloadAs(response, responseClass);
            if (responsePayload == null) {
                throw this.protocolFailure(
                        "Wayang JDBC server returned an empty "
                                + expectedResponseType + " response."
                );
            }
            return responsePayload;
        } catch (IOException e) {
            throw this.communicationFailure(
                    "Could not communicate with Wayang JDBC server.",
                    e
            );
        } catch (RuntimeException e) {
            throw this.protocolFailure(
                    "Could not process the Wayang JDBC server response.",
                    e
            );
        }
    }

    private SQLException toSqlException(final ErrorResponse response) {
        String message = response.getMessage();

        if (message == null || message.isBlank()) {
            message = "Wayang JDBC server returned an error.";
        }

        if (response.getDetail() != null && !response.getDetail().isBlank()) {
            message = message + " " + response.getDetail();
        }

        if (response.getErrorCode() == ErrorCode.UNSUPPORTED_OPERATION) {
            return new SQLFeatureNotSupportedException(
                    message,
                    response.getSqlState(),
                    response.getVendorCode()
            );
        }

        return new SQLException(
                message,
                response.getSqlState(),
                response.getVendorCode()
        );
    }

    private void validateQueryResponse(
            final QueryResultResponse response,
            final String expectedStatementId
    ) throws SQLException {
        this.validateIdentifier(response.getConnectionId(), this.connectionId, "connection");
        this.validateIdentifier(response.getStatementId(), expectedStatementId, "statement");
        if (response.getColumns() == null || response.getRows() == null) {
            throw this.protocolFailure(
                    "Wayang JDBC server returned null query columns or rows."
            );
        }
        for (int index = 0; index < response.getColumns().size(); index++) {
            if (response.getColumns().get(index) == null) {
                throw this.protocolFailure(
                        "Wayang JDBC server returned a null query column at index "
                                + index + "."
                );
            }
        }
        this.validateRowWidths(
                response.getRows(),
                response.getColumns().size(),
                "query"
        );

        final boolean hasCursor = response.getCursorId() != null
                && !response.getCursorId().isBlank();
        if (response.isHasMoreRows() != hasCursor) {
            throw this.protocolFailure(
                    "Wayang JDBC server returned inconsistent query cursor state."
            );
        }
        if (response.isHasMoreRows() && response.getRows().isEmpty()) {
            throw this.protocolFailure(
                    "Wayang JDBC server reported more query rows after an empty batch."
            );
        }
    }

    private void validateFetchResponse(
            final FetchResponse response,
            final String expectedCursorId
    ) throws SQLException {
        this.validateIdentifier(response.getConnectionId(), this.connectionId, "connection");
        this.validateIdentifier(response.getCursorId(), expectedCursorId, "cursor");
        if (response.getRows() == null) {
            throw this.protocolFailure(
                    "Wayang JDBC server returned null fetch rows."
            );
        }
        for (int index = 0; index < response.getRows().size(); index++) {
            if (response.getRows().get(index) == null) {
                throw this.protocolFailure(
                        "Wayang JDBC server returned a null fetch row at index "
                                + index + "."
                );
            }
        }
        if (response.isHasMoreRows() && response.getRows().isEmpty()) {
            throw this.protocolFailure(
                    "Wayang JDBC server reported more fetched rows after an empty batch."
            );
        }
    }

    private void validateMetadataResponse(
            final MetadataResultResponse response,
            final MetadataType expectedType,
            final int expectedColumnCount
    ) throws SQLException {
        this.validateIdentifier(response.getConnectionId(), this.connectionId, "connection");
        if (response.getMetadataType() != expectedType) {
            throw this.protocolFailure(
                    "Wayang JDBC server returned metadata type "
                            + response.getMetadataType() + " for " + expectedType + "."
            );
        }
        if (response.getColumns() == null || response.getRows() == null) {
            throw this.protocolFailure(
                    "Wayang JDBC server returned null metadata columns or rows."
            );
        }
        if (response.getColumns().size() != expectedColumnCount) {
            throw this.protocolFailure(
                    "Wayang JDBC server returned " + response.getColumns().size()
                            + " columns for " + expectedType + " metadata; expected "
                            + expectedColumnCount + "."
            );
        }
        for (int index = 0; index < response.getColumns().size(); index++) {
            if (response.getColumns().get(index) == null
                    || response.getColumns().get(index).getColumnName() == null
                    || response.getColumns().get(index).getColumnName().isBlank()) {
                throw this.protocolFailure(
                        "Wayang JDBC server returned an invalid metadata column at index "
                                + index + "."
                );
            }
        }
        this.validateRowWidths(response.getRows(), expectedColumnCount, "metadata");
    }

    private void validateRowWidths(
            final List<List<Object>> rows,
            final int expectedColumnCount,
            final String resultName
    ) throws SQLException {
        for (int index = 0; index < rows.size(); index++) {
            final List<Object> row = rows.get(index);
            if (row == null || row.size() != expectedColumnCount) {
                throw this.protocolFailure(
                        "Wayang JDBC server returned an invalid " + resultName
                                + " row at index " + index + "."
                );
            }
        }
    }

    private void validateIdentifier(
            final String actual,
            final String expected,
            final String identifierName
    ) throws SQLException {
        if (!Objects.equals(actual, expected)) {
            throw this.protocolFailure(
                    "Wayang JDBC server returned a mismatched "
                            + identifierName + " id."
            );
        }
    }

    private SQLException protocolFailure(final String message) {
        return this.protocolFailure(message, null);
    }

    private SQLException protocolFailure(
            final String message,
            final Throwable cause
    ) {
        this.markBroken();
        return new SQLException(message, "08S01", cause);
    }

    private SQLException communicationFailure(
            final String message,
            final Throwable cause
    ) {
        this.markBroken();
        return new SQLException(message, "08006", cause);
    }

    private synchronized void markBroken() {
        this.closed = true;
        try {
            this.socket.close();
        } catch (IOException ignored) {
            // The original communication/protocol failure is more useful.
        }
    }

    private void ensureOpen() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("Wayang JDBC connection is closed.", "08003");
        }
    }

    private void requireText(
            final String value,
            final String fieldName
    ) throws SQLException {
        if (value == null || value.isBlank()) {
            throw new SQLException(
                    fieldName + " must not be blank.", "HY000"
            );
        }
    }

    private Map<String, String> toStringMap(final Properties properties) {
        final Map<String, String> values = new LinkedHashMap<>();
        for (String name : properties.stringPropertyNames()) {
            values.put(name, properties.getProperty(name));
        }
        return values;
    }

    private List<String> toList(final String[] values) {
        if (values == null) {
            return null;
        }
        return new ArrayList<>(Arrays.asList(values));
    }
}
