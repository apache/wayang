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
import org.apache.wayang.jdbc.protocol.message.CloseConnectionRequest;
import org.apache.wayang.jdbc.protocol.message.CloseConnectionResponse;
import org.apache.wayang.jdbc.protocol.message.ColumnInfo;
import org.apache.wayang.jdbc.protocol.message.ErrorCode;
import org.apache.wayang.jdbc.protocol.message.ErrorResponse;
import org.apache.wayang.jdbc.protocol.message.ExecuteQueryRequest;
import org.apache.wayang.jdbc.protocol.message.OpenConnectionResponse;
import org.apache.wayang.jdbc.protocol.message.PingResponse;
import org.apache.wayang.jdbc.protocol.message.QueryResultResponse;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WayangJdbcClientProtocolTest {

    @Test
    void mismatchedResponseRequestIdMarksClientClosed() throws Exception {
        try (ScriptedServer server = new ScriptedServer(exchange -> {
            if (exchange.request.getType() == MessageType.OPEN_CONNECTION) {
                exchange.respond(MessageType.OPEN_CONNECTION_OK, openConnectionResponse());
            } else if (exchange.request.getType() == MessageType.EXECUTE_QUERY) {
                final ExecuteQueryRequest request = exchange.payloadAs(ExecuteQueryRequest.class);
                exchange.respond(
                        "different-request-id",
                        MessageType.QUERY_RESULT,
                        queryResultResponse(request.getConnectionId(), request.getStatementId())
                );
            }
        });
             WayangJdbcClient client = new WayangJdbcClient(jdbcUrl(server))) {

            final SQLException exception = assertThrows(
                    SQLException.class,
                    () -> client.executeQuery("statement-1", "SELECT 1", 1)
            );

            assertEquals("08S01", exception.getSQLState());
            assertTrue(client.isClosed());
        }
    }

    @Test
    void unsupportedServerErrorBecomesFeatureNotSupportedAndKeepsSocketUsable() throws Exception {
        try (ScriptedServer server = new ScriptedServer(exchange -> {
            if (exchange.request.getType() == MessageType.OPEN_CONNECTION) {
                exchange.respond(MessageType.OPEN_CONNECTION_OK, openConnectionResponse());
            } else if (exchange.request.getType() == MessageType.EXECUTE_QUERY) {
                exchange.respond(
                        MessageType.ERROR,
                        new ErrorResponse(
                                ErrorCode.UNSUPPORTED_OPERATION,
                                "0A000",
                                1003,
                                "Wayang JDBC is read-only.",
                                null,
                                null
                        )
                );
            } else if (exchange.request.getType() == MessageType.PING) {
                exchange.respond(MessageType.PING_OK, new PingResponse("connection-1"));
            } else if (exchange.request.getType() == MessageType.CLOSE_CONNECTION) {
                final CloseConnectionRequest request = exchange.payloadAs(CloseConnectionRequest.class);
                exchange.respond(
                        MessageType.CLOSE_CONNECTION_OK,
                        new CloseConnectionResponse(request.getConnectionId())
                );
            }
        });
             WayangJdbcClient client = new WayangJdbcClient(jdbcUrl(server))) {

            final SQLFeatureNotSupportedException exception = assertThrows(
                    SQLFeatureNotSupportedException.class,
                    () -> client.executeQuery("statement-1", "INSERT INTO t VALUES (1)", 1)
            );

            assertEquals("0A000", exception.getSQLState());
            assertFalse(client.isClosed());
            assertTrue(client.ping(1));
        }
    }

    @Test
    void serverEofMarksClientClosed() throws Exception {
        try (ScriptedServer server = new ScriptedServer(exchange -> {
            if (exchange.request.getType() == MessageType.OPEN_CONNECTION) {
                exchange.respond(MessageType.OPEN_CONNECTION_OK, openConnectionResponse());
            } else if (exchange.request.getType() == MessageType.PING) {
                exchange.closeSocket();
            }
        });
             WayangJdbcClient client = new WayangJdbcClient(jdbcUrl(server))) {

            final SQLException exception = assertThrows(SQLException.class, () -> client.ping(1));

            assertEquals("08006", exception.getSQLState());
            assertTrue(client.isClosed());
        }
    }

    private static WayangJdbcUrl jdbcUrl(final ScriptedServer server) throws SQLException {
        final Properties properties = new Properties();
        properties.setProperty("connectTimeout", "2000");
        return WayangJdbcUrl.parse("jdbc:wayang://127.0.0.1:" + server.getPort() + "/analytics", properties);
    }

    private static OpenConnectionResponse openConnectionResponse() {
        return new OpenConnectionResponse(
                "connection-1",
                "test-server",
                Collections.emptyMap()
        );
    }

    private static QueryResultResponse queryResultResponse(
            final String connectionId,
            final String statementId
    ) {
        return new QueryResultResponse(
                connectionId,
                statementId,
                List.of(new ColumnInfo(
                        "VALUE",
                        "VALUE",
                        null,
                        null,
                        "INTEGER",
                        Types.INTEGER,
                        ResultSetMetaData.columnNoNulls,
                        10,
                        0
                )),
                List.of(Collections.singletonList(1)),
                false,
                null
        );
    }

    private interface RequestHandler {

        void handle(Exchange exchange) throws Exception;
    }

    private static class ScriptedServer implements AutoCloseable {

        private final ProtocolMessageCodec codec = new ProtocolMessageCodec();

        private final ServerSocket serverSocket;

        private final RequestHandler handler;

        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private final Thread thread;

        ScriptedServer(final RequestHandler handler) throws IOException {
            this.handler = handler;
            this.serverSocket = new ServerSocket(0);
            this.thread = new Thread(this::run, "wayang-jdbc-client-protocol-test");
            this.thread.start();
        }

        int getPort() {
            return this.serverSocket.getLocalPort();
        }

        private void run() {
            try (Socket socket = this.serverSocket.accept()) {
                while (!socket.isClosed()) {
                    final MessageEnvelope request = this.codec.read(socket.getInputStream());
                    if (request == null) {
                        return;
                    }
                    this.handler.handle(new Exchange(this.codec, socket, request));
                }
            } catch (SocketException e) {
                if (!this.serverSocket.isClosed()) {
                    this.failure.compareAndSet(null, e);
                }
            } catch (Throwable t) {
                this.failure.compareAndSet(null, t);
            }
        }

        @Override
        public void close() throws Exception {
            this.serverSocket.close();
            this.thread.join(5000L);
            if (this.thread.isAlive()) {
                throw new AssertionError("Scripted JDBC test server did not stop.");
            }
            final Throwable serverFailure = this.failure.get();
            if (serverFailure != null) {
                throw new AssertionError("Scripted JDBC test server failed.", serverFailure);
            }
        }
    }

    private static class Exchange {

        private final ProtocolMessageCodec codec;

        private final Socket socket;

        private final MessageEnvelope request;

        Exchange(
                final ProtocolMessageCodec codec,
                final Socket socket,
                final MessageEnvelope request
        ) {
            this.codec = codec;
            this.socket = socket;
            this.request = request;
        }

        <T> T payloadAs(final Class<T> payloadClass) throws Exception {
            return this.codec.payloadAs(this.request, payloadClass);
        }

        void respond(
                final MessageType responseType,
                final Object payload
        ) throws IOException {
            this.respond(this.request.getRequestId(), responseType, payload);
        }

        void respond(
                final String requestId,
                final MessageType responseType,
                final Object payload
        ) throws IOException {
            this.codec.write(
                    this.socket.getOutputStream(),
                    this.codec.toEnvelope(requestId, responseType, payload)
            );
        }

        void closeSocket() throws IOException {
            this.socket.close();
        }
    }
}
