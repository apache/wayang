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

import org.apache.wayang.jdbc.protocol.MessageEnvelope;
import org.apache.wayang.jdbc.protocol.io.ProtocolMessageCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.UUID;

/**
 * Handles one TCP client connection.
 */
class JdbcClientHandler implements Runnable {

    private final Socket socket;

    private final JdbcRequestDispatcher dispatcher;

    private final ProtocolMessageCodec codec;

    private final String clientId;

    private final Runnable closeCallback;

    JdbcClientHandler(final Socket socket, final JdbcRequestDispatcher dispatcher) {
        this(socket, dispatcher, () -> {
        });
    }

    JdbcClientHandler(
            final Socket socket,
            final JdbcRequestDispatcher dispatcher,
            final Runnable closeCallback
    ) {
        if (socket == null) {
            throw new IllegalArgumentException("Client socket must not be null.");
        }
        if (dispatcher == null) {
            throw new IllegalArgumentException("Request dispatcher must not be null.");
        }
        if (closeCallback == null) {
            throw new IllegalArgumentException("Close callback must not be null.");
        }
        this.socket = socket;
        this.dispatcher = dispatcher;
        this.codec = new ProtocolMessageCodec();
        this.clientId = UUID.randomUUID().toString();
        this.closeCallback = closeCallback;
    }

    @Override
    public void run() {
        try (Socket clientSocket = this.socket;
             InputStream inputStream = clientSocket.getInputStream();
             OutputStream outputStream = clientSocket.getOutputStream()) {
            while (!clientSocket.isClosed()) {
                final MessageEnvelope request = this.codec.read(inputStream);
                if (request == null) {
                    return;
                }
                final MessageEnvelope response = this.dispatcher.dispatch(request, this.clientId);
                this.codec.write(outputStream, response);
            }
        } catch (IOException ignored) {
            // The driver will surface connection failures through JDBC exceptions.
        } finally {
            this.dispatcher.closeClient(this.clientId);
            this.closeCallback.run();
        }
    }
}
