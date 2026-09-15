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

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.wayang.api.sql.context.SqlColumn;
import org.apache.wayang.api.sql.context.SqlQueryResult;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.jdbc.protocol.MessageEnvelope;
import org.apache.wayang.jdbc.protocol.MessageType;
import org.apache.wayang.jdbc.protocol.io.ProtocolMessageCodec;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionRequest;
import org.apache.wayang.jdbc.protocol.message.ErrorCode;
import org.apache.wayang.jdbc.protocol.message.ErrorResponse;
import org.apache.wayang.jdbc.protocol.message.ExecuteQueryRequest;
import org.apache.wayang.jdbc.protocol.message.FetchRequest;
import org.apache.wayang.jdbc.protocol.message.FetchResponse;
import org.apache.wayang.jdbc.protocol.message.OpenConnectionRequest;
import org.apache.wayang.jdbc.protocol.message.OpenConnectionResponse;
import org.apache.wayang.jdbc.protocol.message.QueryResultResponse;

import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcRequestDispatcherTest {

    private final ProtocolMessageCodec codec = new ProtocolMessageCodec();

    @Test
    void rejectsWriteSqlBeforeExecution() throws Exception {
        final RecordingQueryExecutor executor = new RecordingQueryExecutor();
        final JdbcRequestDispatcher dispatcher = new JdbcRequestDispatcher(executor, new DefaultSqlMetadataProvider());
        final String connectionId = this.openConnection(dispatcher, "client-a");

        final MessageEnvelope response = dispatcher.dispatch(
                this.request(
                        "execute-1",
                        MessageType.EXECUTE_QUERY,
                        new ExecuteQueryRequest(connectionId, "statement-1", "INSERT INTO t VALUES (1)", 1)
                ),
                "client-a"
        );
        final ErrorResponse error = this.payloadAs(response, ErrorResponse.class);

        assertEquals(MessageType.ERROR, response.getType());
        assertEquals(ErrorCode.UNSUPPORTED_OPERATION, error.getErrorCode());
        assertEquals("0A000", error.getSqlState());
        assertEquals(Collections.emptyList(), executor.getExecutedSql());
    }

    @Test
    void pagesAndClosesCursorAfterFinalFetch() throws Exception {
        final JdbcRequestDispatcher dispatcher = new JdbcRequestDispatcher(
                new RecordingQueryExecutor(),
                new DefaultSqlMetadataProvider()
        );
        final String connectionId = this.openConnection(dispatcher, "client-a");

        final MessageEnvelope executeResponse = dispatcher.dispatch(
                this.request(
                        "execute-1",
                        MessageType.EXECUTE_QUERY,
                        new ExecuteQueryRequest(connectionId, "statement-1", "SELECT * FROM t", 1)
                ),
                "client-a"
        );
        final QueryResultResponse firstBatch = this.payloadAs(executeResponse, QueryResultResponse.class);
        assertEquals(MessageType.QUERY_RESULT, executeResponse.getType());
        assertEquals(Collections.singletonList(Collections.singletonList(1)), firstBatch.getRows());
        assertTrue(firstBatch.isHasMoreRows());
        assertNotNull(firstBatch.getCursorId());

        final MessageEnvelope fetchResponse = dispatcher.dispatch(
                this.request(
                        "fetch-1",
                        MessageType.FETCH,
                        new FetchRequest(connectionId, firstBatch.getCursorId(), 5)
                ),
                "client-a"
        );
        final FetchResponse finalBatch = this.payloadAs(fetchResponse, FetchResponse.class);

        assertEquals(MessageType.FETCH_RESULT, fetchResponse.getType());
        assertEquals(
                Arrays.asList(Collections.singletonList(2), Collections.singletonList(3)),
                finalBatch.getRows()
        );
        assertFalse(finalBatch.isHasMoreRows());

        final MessageEnvelope staleFetchResponse = dispatcher.dispatch(
                this.request(
                        "fetch-2",
                        MessageType.FETCH,
                        new FetchRequest(connectionId, firstBatch.getCursorId(), 1)
                ),
                "client-a"
        );
        final ErrorResponse staleFetchError = this.payloadAs(staleFetchResponse, ErrorResponse.class);
        assertEquals(MessageType.ERROR, staleFetchResponse.getType());
        assertEquals(ErrorCode.INVALID_REQUEST, staleFetchError.getErrorCode());
    }

    @Test
    void enforcesClientOwnership() throws Exception {
        final JdbcRequestDispatcher dispatcher = new JdbcRequestDispatcher(
                new RecordingQueryExecutor(),
                new DefaultSqlMetadataProvider()
        );
        final String connectionId = this.openConnection(dispatcher, "client-a");

        final MessageEnvelope response = dispatcher.dispatch(
                this.request(
                        "execute-1",
                        MessageType.EXECUTE_QUERY,
                        new ExecuteQueryRequest(connectionId, "statement-1", "SELECT * FROM t", 1)
                ),
                "client-b"
        );
        final ErrorResponse error = this.payloadAs(response, ErrorResponse.class);

        assertEquals(MessageType.ERROR, response.getType());
        assertEquals(ErrorCode.INVALID_REQUEST, error.getErrorCode());
    }

    @Test
    void closeConnectionReleasesCursors() throws Exception {
        final JdbcRequestDispatcher dispatcher = new JdbcRequestDispatcher(
                new RecordingQueryExecutor(),
                new DefaultSqlMetadataProvider()
        );
        final String connectionId = this.openConnection(dispatcher, "client-a");
        final QueryResultResponse firstBatch = this.payloadAs(
                dispatcher.dispatch(
                        this.request(
                                "execute-1",
                                MessageType.EXECUTE_QUERY,
                                new ExecuteQueryRequest(connectionId, "statement-1", "SELECT * FROM t", 1)
                        ),
                        "client-a"
                ),
                QueryResultResponse.class
        );

        final MessageEnvelope closeResponse = dispatcher.dispatch(
                this.request(
                        "close-1",
                        MessageType.CLOSE_CONNECTION,
                        new CloseConnectionRequest(connectionId)
                ),
                "client-a"
        );
        assertEquals(MessageType.CLOSE_CONNECTION_OK, closeResponse.getType());

        final MessageEnvelope fetchResponse = dispatcher.dispatch(
                this.request(
                        "fetch-1",
                        MessageType.FETCH,
                        new FetchRequest(connectionId, firstBatch.getCursorId(), 1)
                ),
                "client-a"
        );
        final ErrorResponse error = this.payloadAs(fetchResponse, ErrorResponse.class);
        assertEquals(MessageType.ERROR, fetchResponse.getType());
        assertEquals(ErrorCode.INVALID_REQUEST, error.getErrorCode());
    }

    private String openConnection(
            final JdbcRequestDispatcher dispatcher,
            final String clientId
    ) throws Exception {
        final MessageEnvelope response = dispatcher.dispatch(
                this.request(
                        "open-1",
                        MessageType.OPEN_CONNECTION,
                        new OpenConnectionRequest("user", "analytics", Collections.emptyMap())
                ),
                clientId
        );
        final OpenConnectionResponse payload = this.payloadAs(response, OpenConnectionResponse.class);
        assertEquals(MessageType.OPEN_CONNECTION_OK, response.getType());
        return payload.getConnectionId();
    }

    private MessageEnvelope request(
            final String requestId,
            final MessageType messageType,
            final Object payload
    ) {
        return this.codec.toEnvelope(requestId, messageType, payload);
    }

    private <T> T payloadAs(
            final MessageEnvelope envelope,
            final Class<T> payloadClass
    ) throws Exception {
        return this.codec.payloadAs(envelope, payloadClass);
    }

    private static class RecordingQueryExecutor implements SqlQueryExecutor {

        private final List<String> executedSql = new ArrayList<>();

        @Override
        public SqlQueryResult execute(final String sql) throws SqlParseException {
            this.executedSql.add(sql);
            return new SqlQueryResult(
                    Collections.singletonList(new SqlColumn("VALUE", "VALUE", "INTEGER", Types.INTEGER, 10, 0, false)),
                    Arrays.asList(new Record(1), new Record(2), new Record(3))
            );
        }

        List<String> getExecutedSql() {
            return List.copyOf(this.executedSql);
        }
    }
}
