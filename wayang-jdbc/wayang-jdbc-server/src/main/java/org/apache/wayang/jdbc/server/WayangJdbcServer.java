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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.jdbc.protocol.ProtocolConstants;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TCP server for the external Wayang JDBC protocol.
 */
public class WayangJdbcServer implements AutoCloseable {

    private static final int ACCEPT_BACKLOG = 50;

    private static final int MAX_CLIENT_CONNECTIONS = 64;

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final String host;

    private final int port;

    private final JdbcRequestDispatcher dispatcher;

    private final ExecutorService clientExecutor;

    private final Set<Socket> clientSockets = ConcurrentHashMap.newKeySet();

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final CountDownLatch termination = new CountDownLatch(1);

    private volatile ServerSocket serverSocket;

    private volatile Thread acceptThread;

    public WayangJdbcServer(
            final String host,
            final int port,
            final SqlQueryExecutor queryExecutor
    ) {
        this(host, port, new JdbcRequestDispatcher(queryExecutor));
    }

    WayangJdbcServer(
            final String host,
            final int port,
            final SqlQueryExecutor queryExecutor,
            final SqlMetadataProvider metadataProvider
    ) {
        this(host, port, new JdbcRequestDispatcher(queryExecutor, metadataProvider));
    }

    private WayangJdbcServer(
            final String host,
            final int port,
            final JdbcRequestDispatcher dispatcher
    ) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Server host must not be blank.");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Server port must be between 0 and 65535.");
        }
        this.host = host;
        this.port = port;
        this.dispatcher = dispatcher;
        final AtomicInteger threadId = new AtomicInteger();
        this.clientExecutor = new ThreadPoolExecutor(
                0,
                MAX_CLIENT_CONNECTIONS,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    final Thread thread = new Thread(
                            runnable,
                            "wayang-jdbc-client-" + threadId.incrementAndGet()
                    );
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public synchronized void start() throws IOException {
        if (this.closed.get()) {
            throw new IllegalStateException("Wayang JDBC server is closed.");
        }
        if (this.running.get()) {
            throw new IllegalStateException("Wayang JDBC server is already running.");
        }

        final ServerSocket newServerSocket = new ServerSocket();
        try {
            newServerSocket.setReuseAddress(true);
            newServerSocket.bind(
                    new InetSocketAddress(InetAddress.getByName(this.host), this.port),
                    ACCEPT_BACKLOG
            );
            this.serverSocket = newServerSocket;
            this.running.set(true);
            this.acceptThread = new Thread(this::acceptLoop, "wayang-jdbc-server-accept");
            this.acceptThread.setDaemon(true);
            this.acceptThread.start();
        } catch (IOException | RuntimeException e) {
            this.running.set(false);
            this.serverSocket = null;
            try {
                newServerSocket.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    public int getPort() {
        if (this.serverSocket == null) {
            return this.port;
        }
        return this.serverSocket.getLocalPort();
    }

    public void awaitTermination() throws InterruptedException {
        this.termination.await();
    }

    @Override
    public synchronized void close() throws IOException {
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }

        this.running.set(false);
        IOException failure = null;
        final ServerSocket currentServerSocket = this.serverSocket;
        if (currentServerSocket != null) {
            try {
                currentServerSocket.close();
            } catch (IOException e) {
                failure = e;
            }
        }
        for (final Socket clientSocket : this.clientSockets) {
            try {
                clientSocket.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        this.clientSockets.clear();
        this.clientExecutor.shutdownNow();

        final Thread currentAcceptThread = this.acceptThread;
        if (currentAcceptThread != null && currentAcceptThread != Thread.currentThread()) {
            try {
                currentAcceptThread.join(TimeUnit.SECONDS.toMillis(SHUTDOWN_TIMEOUT_SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            this.clientExecutor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.dispatcher.close();
        this.termination.countDown();

        if (failure != null) {
            throw failure;
        }
    }

    public static void main(final String[] args) throws IOException, SQLException, InterruptedException {
        final String host = args.length > 0 ? args[0] : "127.0.0.1";
        final int port = args.length > 1 ? Integer.parseInt(args[1]) : ProtocolConstants.DEFAULT_PORT;
        final Configuration configuration = args.length > 2 ? new Configuration(args[2]) : new Configuration();

        final WayangJdbcServer server = new WayangJdbcServer(
                host,
                port,
                new WayangSqlQueryExecutor(configuration)
        );
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (IOException ignored) {
            }
        }));
        server.start();
        server.awaitTermination();
    }

    private void acceptLoop() {
        while (this.running.get()) {
            try {
                final Socket socket = this.serverSocket.accept();
                try {
                    socket.setKeepAlive(true);
                    socket.setTcpNoDelay(true);
                } catch (IOException e) {
                    this.closeClientSocket(socket);
                    continue;
                }
                this.clientSockets.add(socket);
                if (!this.running.get()) {
                    this.closeClientSocket(socket);
                    return;
                }
                try {
                    this.clientExecutor.submit(new JdbcClientHandler(
                            socket,
                            this.dispatcher,
                            () -> this.clientSockets.remove(socket)
                    ));
                } catch (RejectedExecutionException e) {
                    this.closeClientSocket(socket);
                }
            } catch (IOException | RuntimeException e) {
                if (this.running.get()) {
                    try {
                        this.close();
                    } catch (IOException closeException) {
                        e.addSuppressed(closeException);
                    }
                }
                return;
            }
        }
    }

    private void closeClientSocket(final Socket socket) {
        this.clientSockets.remove(socket);
        try {
            socket.close();
        } catch (IOException ignored) {
            // The socket is already unusable; no additional recovery is possible.
        }
    }
}
