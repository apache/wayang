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

package org.apache.wayang.jdbc.server;

import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.wayang.api.sql.context.SqlQueryResult;
import org.apache.wayang.jdbc.protocol.MessageEnvelope;
import org.apache.wayang.jdbc.protocol.MessageType;
import org.apache.wayang.jdbc.protocol.ProtocolException;
import org.apache.wayang.jdbc.protocol.io.ProtocolMessageCodec;
import org.apache.wayang.jdbc.protocol.message.CancelQueryRequest;
import org.apache.wayang.jdbc.protocol.message.CancelQueryResponse;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionRequest;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionResponse;
import org.apache.wayang.jdbc.protocol.message.CloseCursorRequest;
import org.apache.wayang.jdbc.protocol.message.CloseCursorResponse;
import org.apache.wayang.jdbc.protocol.message.ColumnInfo;
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dispatches protocol messages to server-side JDBC gateway actions.
 */
public class JdbcRequestDispatcher {

    private static final int DEFAULT_FETCH_SIZE = 100;

    private static final int MAX_FETCH_SIZE = 10_000;

    private static final String SERVER_VERSION = "1.1.2-SNAPSHOT";

    private static final String LOCAL_CLIENT_ID = "local-dispatcher";

    private static final Set<String> QUERY_STATEMENT_KEYWORDS = Set.of(
            "SELECT",
            "TABLE",
            "VALUES"
    );

    private static final Set<String> UNSUPPORTED_STATEMENT_KEYWORDS = Set.of(
            "ALTER",
            "ANALYZE",
            "BEGIN",
            "CALL",
            "COMMENT",
            "COMMIT",
            "COPY",
            "CREATE",
            "DELETE",
            "DROP",
            "END",
            "GRANT",
            "INSERT",
            "LOAD",
            "LOCK",
            "MERGE",
            "RENAME",
            "REPLACE",
            "REVOKE",
            "ROLLBACK",
            "SAVEPOINT",
            "SET",
            "START",
            "TRUNCATE",
            "UNLOCK",
            "UPDATE",
            "UPSERT",
            "USE",
            "VACUUM"
    );

    private final ProtocolMessageCodec codec;

    private final SqlQueryExecutor queryExecutor;

    private final SqlMetadataProvider metadataProvider;

    private final JdbcServerSessionManager sessionManager;

    private final CursorStore cursorStore;

    private final int defaultFetchSize;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public JdbcRequestDispatcher(final SqlQueryExecutor queryExecutor) {
        this(queryExecutor, DEFAULT_FETCH_SIZE);
    }

    public JdbcRequestDispatcher(final SqlQueryExecutor queryExecutor, final int defaultFetchSize) {
        this(queryExecutor, createDefaultMetadataProvider(queryExecutor), defaultFetchSize);
    }

    JdbcRequestDispatcher(
            final SqlQueryExecutor queryExecutor,
            final SqlMetadataProvider metadataProvider
    ) {
        this(queryExecutor, metadataProvider, DEFAULT_FETCH_SIZE);
    }

    public JdbcRequestDispatcher(
            final SqlQueryExecutor queryExecutor,
            final SqlMetadataProvider metadataProvider,
            final int defaultFetchSize
    ) {
        if (queryExecutor == null) {
            throw new IllegalArgumentException("Query executor must not be null.");
        }
        if (metadataProvider == null) {
            throw new IllegalArgumentException("Metadata provider must not be null.");
        }
        if (defaultFetchSize <= 0) {
            throw new IllegalArgumentException("Default fetch size must be positive.");
        }
        this.codec = new ProtocolMessageCodec();
        this.queryExecutor = queryExecutor;
        this.metadataProvider = metadataProvider;
        this.sessionManager = new JdbcServerSessionManager();
        this.cursorStore = new CursorStore();
        this.defaultFetchSize = defaultFetchSize;
    }

    private static SqlMetadataProvider createDefaultMetadataProvider(final SqlQueryExecutor queryExecutor) {
        if (queryExecutor instanceof WayangSqlQueryExecutor) {
            return new DefaultSqlMetadataProvider(
                    ((WayangSqlQueryExecutor) queryExecutor).getSqlContext()
            );
        }
        return new DefaultSqlMetadataProvider();
    }

    public MessageEnvelope dispatch(final MessageEnvelope request) {
        return this.dispatch(request, LOCAL_CLIENT_ID);
    }

    MessageEnvelope dispatch(final MessageEnvelope request, final String clientId) {
        try {
            if (this.closed.get()) {
                throw new DispatcherClosedException("The Wayang JDBC server is closed.");
            }
            this.requireText(clientId, "Client id");
            switch (request.getType()) {
                case OPEN_CONNECTION:
                    return this.openConnection(request, clientId);
                case PING:
                    return this.ping(request, clientId);
                case EXECUTE_QUERY:
                    return this.executeQuery(request, clientId);
                case FETCH:
                    return this.fetch(request, clientId);
                case CLOSE_CURSOR:
                    return this.closeCursor(request, clientId);
                case CANCEL_QUERY:
                    return this.cancelQuery(request, clientId);
                case CLOSE_CONNECTION:
                    return this.closeConnection(request, clientId);
                case GET_SCHEMAS:
                    return this.getSchemas(request, clientId);
                case GET_TABLES:
                    return this.getTables(request, clientId);
                case GET_COLUMNS:
                    return this.getColumns(request, clientId);
                default:
                    return this.error(
                            request.getRequestId(),
                            ErrorCode.INVALID_REQUEST,
                            "HY000",
                            1000,
                            "Unsupported request message type: " + request.getType(),
                            null,
                            null
                    );
            }
        } catch (UnsupportedSqlOperationException e) {
            return this.error(
                    request.getRequestId(),
                    ErrorCode.UNSUPPORTED_OPERATION,
                    "0A000",
                    1003,
                    e.getMessage(),
                    null,
                    e.getClass().getName()
            );
        } catch (JdbcServerSessionManager.CapacityException
                 | CursorStore.CapacityException
                 | DispatcherClosedException e) {
            return this.error(
                    request.getRequestId(),
                    ErrorCode.CONNECTION_ERROR,
                    "08004",
                    1004,
                    e.getMessage(),
                    null,
                    e.getClass().getName()
            );
        } catch (ProtocolException | IllegalArgumentException e) {
            return this.error(
                    request.getRequestId(),
                    ErrorCode.INVALID_REQUEST,
                    "HY000",
                    1000,
                    e.getMessage(),
                    null,
                    e.getClass().getName()
            );
        } catch (SqlParseException e) {
            return this.error(
                    request.getRequestId(),
                    ErrorCode.QUERY_PARSE_ERROR,
                    "42000",
                    1002,
                    "Could not parse SQL query.",
                    e.getMessage(),
                    e.getClass().getName()
            );
        } catch (MetadataOperationException e) {
            return this.error(
                    request.getRequestId(),
                    ErrorCode.METADATA_ERROR,
                    "HY000",
                    1005,
                    e.getMessage(),
                    e.getCause() == null ? null : e.getCause().getMessage(),
                    e.getCause() == null ? e.getClass().getName() : e.getCause().getClass().getName()
            );
        } catch (Exception e) {
            return this.error(
                    request.getRequestId(),
                    ErrorCode.QUERY_EXECUTION_ERROR,
                    "HY000",
                    1001,
                    "Could not execute SQL query.",
                    e.getMessage(),
                    e.getClass().getName()
            );
        }
    }

    void closeClient(final String clientId) {
        for (final String connectionId : this.sessionManager.closeClient(clientId)) {
            this.cursorStore.closeConnection(connectionId);
        }
    }

    void close() {
        if (this.closed.compareAndSet(false, true)) {
            this.cursorStore.clear();
            this.sessionManager.clear();
        }
    }

    private MessageEnvelope openConnection(
            final MessageEnvelope request,
            final String clientId
    ) throws ProtocolException {
        final OpenConnectionRequest openRequest = this.codec.payloadAs(request, OpenConnectionRequest.class);
        final String connectionId = this.sessionManager.openConnection(
                clientId,
                openRequest.getUsername(),
                openRequest.getDatabase(),
                openRequest.getProperties()
        );
        final OpenConnectionResponse response = new OpenConnectionResponse(
                connectionId,
                SERVER_VERSION,
                new LinkedHashMap<>()
        );
        return this.codec.toEnvelope(request.getRequestId(), MessageType.OPEN_CONNECTION_OK, response);
    }

    private MessageEnvelope executeQuery(
            final MessageEnvelope request,
            final String clientId
    ) throws Exception {
        final ExecuteQueryRequest executeRequest = this.codec.payloadAs(request, ExecuteQueryRequest.class);
        this.requireOpenConnection(executeRequest.getConnectionId(), clientId);
        this.requireText(executeRequest.getStatementId(), "Statement id");
        this.requireText(executeRequest.getSql(), "SQL query");
        this.requireReadOnlyQuery(executeRequest.getSql());

        this.cursorStore.closeStatement(
                executeRequest.getConnectionId(),
                executeRequest.getStatementId()
        );
        final SqlQueryResult result = this.queryExecutor.execute(executeRequest.getSql());
        if (this.closed.get()) {
            throw new DispatcherClosedException("The Wayang JDBC server is closed.");
        }
        this.requireOpenConnection(executeRequest.getConnectionId(), clientId);
        if (result == null) {
            throw new IllegalStateException("The SQL query executor returned no result.");
        }
        final List<ColumnInfo> columns = SqlQueryResultAdapter.toColumnInfo(result);
        final List<List<Object>> rows = SqlQueryResultAdapter.toRows(result);
        this.validateTabularResult(columns, rows, "Query result");
        final int fetchSize = this.normalizeFetchSize(executeRequest.getFetchSize());
        final int batchEnd = Math.min(fetchSize, rows.size());
        final List<List<Object>> firstBatch = List.copyOf(rows.subList(0, batchEnd));
        final boolean hasMoreRows = batchEnd < rows.size();
        if (!hasMoreRows) {
            return this.queryResultEnvelope(
                    request.getRequestId(),
                    executeRequest,
                    columns,
                    firstBatch,
                    false,
                    null
            );
        }

        return this.cursorStore.openCursor(
                executeRequest.getConnectionId(),
                executeRequest.getStatementId(),
                rows,
                batchEnd,
                cursorId -> this.queryResultEnvelope(
                        request.getRequestId(),
                        executeRequest,
                        columns,
                        firstBatch,
                        true,
                        cursorId
                )
        );
    }

    private MessageEnvelope ping(
            final MessageEnvelope request,
            final String clientId
    ) throws ProtocolException {
        final PingRequest pingRequest = this.codec.payloadAs(request, PingRequest.class);
        this.requireOpenConnection(pingRequest.getConnectionId(), clientId);
        return this.codec.toEnvelope(
                request.getRequestId(),
                MessageType.PING_OK,
                new PingResponse(pingRequest.getConnectionId())
        );
    }

    private MessageEnvelope getSchemas(
            final MessageEnvelope request,
            final String clientId
    ) throws MetadataOperationException, ProtocolException {
        final GetSchemasRequest metadataRequest = this.codec.payloadAs(request, GetSchemasRequest.class);
        final JdbcServerSession session = this.requireOpenSession(metadataRequest.getConnectionId(), clientId);
        try {
            final MetadataResultResponse response = this.metadataProvider.getSchemas(session, metadataRequest);
            this.validateMetadataResponse(response, session.getConnectionId(), MetadataType.SCHEMAS);
            return this.codec.toEnvelope(request.getRequestId(), MessageType.METADATA_RESULT, response);
        } catch (SQLException | IllegalStateException e) {
            throw new MetadataOperationException("Could not retrieve JDBC schema metadata.", e);
        }
    }

    private MessageEnvelope getTables(
            final MessageEnvelope request,
            final String clientId
    ) throws MetadataOperationException, ProtocolException {
        final GetTablesRequest metadataRequest = this.codec.payloadAs(request, GetTablesRequest.class);
        final JdbcServerSession session = this.requireOpenSession(metadataRequest.getConnectionId(), clientId);
        try {
            final MetadataResultResponse response = this.metadataProvider.getTables(session, metadataRequest);
            this.validateMetadataResponse(response, session.getConnectionId(), MetadataType.TABLES);
            return this.codec.toEnvelope(request.getRequestId(), MessageType.METADATA_RESULT, response);
        } catch (SQLException | IllegalStateException e) {
            throw new MetadataOperationException("Could not retrieve JDBC table metadata.", e);
        }
    }

    private MessageEnvelope getColumns(
            final MessageEnvelope request,
            final String clientId
    ) throws MetadataOperationException, ProtocolException {
        final GetColumnsRequest metadataRequest = this.codec.payloadAs(request, GetColumnsRequest.class);
        final JdbcServerSession session = this.requireOpenSession(metadataRequest.getConnectionId(), clientId);
        try {
            final MetadataResultResponse response = this.metadataProvider.getColumns(session, metadataRequest);
            this.validateMetadataResponse(response, session.getConnectionId(), MetadataType.COLUMNS);
            return this.codec.toEnvelope(request.getRequestId(), MessageType.METADATA_RESULT, response);
        } catch (SQLException | IllegalStateException e) {
            throw new MetadataOperationException("Could not retrieve JDBC column metadata.", e);
        }
    }

    private MessageEnvelope fetch(
            final MessageEnvelope request,
            final String clientId
    ) throws ProtocolException {
        final FetchRequest fetchRequest = this.codec.payloadAs(request, FetchRequest.class);
        this.requireOpenConnection(fetchRequest.getConnectionId(), clientId);
        this.requireText(fetchRequest.getCursorId(), "Cursor id");

        final MessageEnvelope response = this.cursorStore.fetch(
                fetchRequest.getConnectionId(),
                fetchRequest.getCursorId(),
                this.normalizeFetchSize(fetchRequest.getFetchSize()),
                batch -> this.codec.toEnvelope(
                        request.getRequestId(),
                        MessageType.FETCH_RESULT,
                        new FetchResponse(
                                fetchRequest.getConnectionId(),
                                fetchRequest.getCursorId(),
                                batch.getRows(),
                                batch.hasMoreRows()
                        )
                )
        );
        if (response == null) {
            throw new IllegalArgumentException("Unknown cursor id: " + fetchRequest.getCursorId());
        }
        return response;
    }

    private MessageEnvelope queryResultEnvelope(
            final String requestId,
            final ExecuteQueryRequest executeRequest,
            final List<ColumnInfo> columns,
            final List<List<Object>> rows,
            final boolean hasMoreRows,
            final String cursorId
    ) {
        final QueryResultResponse response = new QueryResultResponse(
                executeRequest.getConnectionId(),
                executeRequest.getStatementId(),
                columns,
                rows,
                hasMoreRows,
                cursorId
        );
        return this.codec.toEnvelope(requestId, MessageType.QUERY_RESULT, response);
    }

    private MessageEnvelope closeCursor(
            final MessageEnvelope request,
            final String clientId
    ) throws ProtocolException {
        final CloseCursorRequest closeRequest = this.codec.payloadAs(request, CloseCursorRequest.class);
        this.requireOpenConnection(closeRequest.getConnectionId(), clientId);
        this.requireText(closeRequest.getStatementId(), "Statement id");
        this.requireText(closeRequest.getCursorId(), "Cursor id");
        final boolean closed = this.cursorStore.closeCursor(
                closeRequest.getConnectionId(),
                closeRequest.getStatementId(),
                closeRequest.getCursorId()
        );
        if (!closed) {
            throw new IllegalArgumentException(
                    "Unknown cursor for statement: " + closeRequest.getCursorId()
            );
        }

        final CloseCursorResponse response = new CloseCursorResponse(
                closeRequest.getConnectionId(),
                closeRequest.getStatementId(),
                closeRequest.getCursorId()
        );
        return this.codec.toEnvelope(request.getRequestId(), MessageType.CLOSE_CURSOR_OK, response);
    }

    private MessageEnvelope cancelQuery(
            final MessageEnvelope request,
            final String clientId
    ) throws ProtocolException {
        final CancelQueryRequest cancelRequest = this.codec.payloadAs(request, CancelQueryRequest.class);
        this.requireOpenConnection(cancelRequest.getConnectionId(), clientId);
        this.requireText(cancelRequest.getStatementId(), "Statement id");
        if (cancelRequest.getCursorId() != null) {
            this.requireText(cancelRequest.getCursorId(), "Cursor id");
        }
        final boolean cancelled = cancelRequest.getCursorId() != null
                && this.cursorStore.cancelCursor(
                        cancelRequest.getConnectionId(),
                        cancelRequest.getStatementId(),
                        cancelRequest.getCursorId()
                );

        final CancelQueryResponse response = new CancelQueryResponse(
                cancelRequest.getConnectionId(),
                cancelRequest.getStatementId(),
                cancelRequest.getCursorId(),
                cancelled
        );
        return this.codec.toEnvelope(request.getRequestId(), MessageType.CANCEL_QUERY_OK, response);
    }

    private MessageEnvelope closeConnection(
            final MessageEnvelope request,
            final String clientId
    ) throws ProtocolException {
        final CloseConnectionRequest closeRequest = this.codec.payloadAs(request, CloseConnectionRequest.class);
        this.requireOpenConnection(closeRequest.getConnectionId(), clientId);
        this.cursorStore.closeConnection(closeRequest.getConnectionId());
        this.sessionManager.closeConnection(closeRequest.getConnectionId());

        final CloseConnectionResponse response = new CloseConnectionResponse(closeRequest.getConnectionId());
        return this.codec.toEnvelope(request.getRequestId(), MessageType.CLOSE_CONNECTION_OK, response);
    }

    private void requireOpenConnection(final String connectionId, final String clientId) {
        this.requireOpenSession(connectionId, clientId);
    }

    private JdbcServerSession requireOpenSession(final String connectionId, final String clientId) {
        this.requireText(connectionId, "Connection id");
        final JdbcServerSession session = this.sessionManager.getSession(connectionId, clientId);
        if (session == null) {
            throw new IllegalArgumentException("Unknown connection id: " + connectionId);
        }
        return session;
    }

    private void requireText(final String value, final String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank.");
        }
    }

    private int normalizeFetchSize(final int fetchSize) {
        return Math.min(fetchSize > 0 ? fetchSize : this.defaultFetchSize, MAX_FETCH_SIZE);
    }

    private void requireReadOnlyQuery(
            final String sql
    ) throws SqlParseException, UnsupportedSqlOperationException {
        final SqlNode sqlNode;
        try {
            sqlNode = SqlParser.create(sql).parseStmt();
        } catch (SqlParseException e) {
            final String statementKeyword = this.findTopLevelStatementKeyword(sql);
            if (statementKeyword != null && UNSUPPORTED_STATEMENT_KEYWORDS.contains(statementKeyword)) {
                throw new UnsupportedSqlOperationException(
                        "Wayang JDBC is read-only; SQL statement kind "
                                + statementKeyword
                                + " is not supported."
                );
            }
            throw e;
        }

        if (!this.isReadOnlyQuery(sqlNode)) {
            throw new UnsupportedSqlOperationException(
                    "Wayang JDBC is read-only; SQL statement kind "
                            + sqlNode.getKind()
                            + " is not supported."
            );
        }
    }

    private boolean isReadOnlyQuery(final SqlNode sqlNode) {
        if (sqlNode instanceof SqlWith) {
            return this.isReadOnlyQuery(((SqlWith) sqlNode).body);
        }
        if (sqlNode instanceof SqlExplain) {
            return this.isReadOnlyQuery(((SqlExplain) sqlNode).getExplicandum());
        }
        return sqlNode.getKind().belongsTo(SqlKind.QUERY);
    }

    private String findTopLevelStatementKeyword(final String sql) {
        final List<String> keywords = this.topLevelWords(sql);
        if (keywords.isEmpty()) {
            return null;
        }

        final String firstKeyword = keywords.get(0);
        if (!"WITH".equals(firstKeyword) && !"EXPLAIN".equals(firstKeyword)) {
            return firstKeyword;
        }

        for (int index = 1; index < keywords.size(); index++) {
            final String keyword = keywords.get(index);
            if (QUERY_STATEMENT_KEYWORDS.contains(keyword)
                    || UNSUPPORTED_STATEMENT_KEYWORDS.contains(keyword)) {
                return keyword;
            }
        }
        return firstKeyword;
    }

    private List<String> topLevelWords(final String sql) {
        final List<String> words = new ArrayList<>();
        int parenthesisDepth = 0;
        int index = 0;
        while (index < sql.length()) {
            final char character = sql.charAt(index);
            if (character == '-' && index + 1 < sql.length() && sql.charAt(index + 1) == '-') {
                index = this.skipLineComment(sql, index + 2);
            } else if (character == '/' && index + 1 < sql.length() && sql.charAt(index + 1) == '*') {
                index = this.skipBlockComment(sql, index + 2);
            } else if (character == '\'' || character == '"' || character == '`') {
                index = this.skipQuotedText(sql, index + 1, character);
            } else if (character == '[') {
                index = this.skipQuotedText(sql, index + 1, ']');
            } else if (character == '(') {
                parenthesisDepth++;
                index++;
            } else if (character == ')') {
                parenthesisDepth = Math.max(0, parenthesisDepth - 1);
                index++;
            } else if (parenthesisDepth == 0 && Character.isLetter(character)) {
                final int wordStart = index;
                index++;
                while (index < sql.length()) {
                    final char wordCharacter = sql.charAt(index);
                    if (!Character.isLetterOrDigit(wordCharacter) && wordCharacter != '_') {
                        break;
                    }
                    index++;
                }
                words.add(sql.substring(wordStart, index).toUpperCase(Locale.ROOT));
            } else {
                index++;
            }
        }
        return words;
    }

    private int skipLineComment(final String sql, final int startIndex) {
        int index = startIndex;
        while (index < sql.length() && sql.charAt(index) != '\n' && sql.charAt(index) != '\r') {
            index++;
        }
        return index;
    }

    private int skipBlockComment(final String sql, final int startIndex) {
        int index = startIndex;
        while (index + 1 < sql.length()) {
            if (sql.charAt(index) == '*' && sql.charAt(index + 1) == '/') {
                return index + 2;
            }
            index++;
        }
        return sql.length();
    }

    private int skipQuotedText(final String sql, final int startIndex, final char quote) {
        int index = startIndex;
        while (index < sql.length()) {
            if (sql.charAt(index) == quote) {
                if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                    index += 2;
                    continue;
                }
                return index + 1;
            }
            index++;
        }
        return sql.length();
    }

    private void validateMetadataResponse(
            final MetadataResultResponse response,
            final String connectionId,
            final MetadataType metadataType
    ) {
        if (response == null) {
            throw new IllegalStateException("The metadata provider returned no result.");
        }
        if (!Objects.equals(connectionId, response.getConnectionId())) {
            throw new IllegalStateException("The metadata provider returned a mismatched connection id.");
        }
        if (response.getMetadataType() != metadataType) {
            throw new IllegalStateException("The metadata provider returned a mismatched metadata type.");
        }
        this.validateTabularResult(response.getColumns(), response.getRows(), "Metadata result");
    }

    private void validateTabularResult(
            final List<ColumnInfo> columns,
            final List<List<Object>> rows,
            final String resultName
    ) {
        if (columns == null || rows == null) {
            throw new IllegalStateException(resultName + " columns and rows must not be null.");
        }
        for (final ColumnInfo column : columns) {
            if (column == null) {
                throw new IllegalStateException(resultName + " must not contain a null column.");
            }
        }
        for (final List<Object> row : rows) {
            if (row == null || row.size() != columns.size()) {
                throw new IllegalStateException(
                        resultName + " row width does not match its column count."
                );
            }
        }
    }

    private MessageEnvelope error(
            final String requestId,
            final ErrorCode errorCode,
            final String sqlState,
            final int vendorCode,
            final String message,
            final String detail,
            final String exceptionClass
    ) {
        final ErrorResponse response = new ErrorResponse(
                errorCode,
                sqlState,
                vendorCode,
                message,
                detail,
                exceptionClass
        );
        return this.codec.toEnvelope(requestId, MessageType.ERROR, response);
    }

    private static class UnsupportedSqlOperationException extends Exception {

        UnsupportedSqlOperationException(final String message) {
            super(message);
        }
    }

    private static class DispatcherClosedException extends IllegalStateException {

        DispatcherClosedException(final String message) {
            super(message);
        }
    }

    private static class MetadataOperationException extends Exception {

        MetadataOperationException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
