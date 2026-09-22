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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks logical JDBC connections managed by the server.
 */
class JdbcServerSessionManager {

    private static final int DEFAULT_MAX_SESSIONS = 1024;

    private static final int DEFAULT_MAX_SESSIONS_PER_CLIENT = 16;

    private final Map<String, JdbcServerSession> sessions = new ConcurrentHashMap<>();

    private final int maxSessions;

    private final int maxSessionsPerClient;

    JdbcServerSessionManager() {
        this(DEFAULT_MAX_SESSIONS, DEFAULT_MAX_SESSIONS_PER_CLIENT);
    }

    JdbcServerSessionManager(final int maxSessions, final int maxSessionsPerClient) {
        if (maxSessions <= 0) {
            throw new IllegalArgumentException("Maximum session count must be positive.");
        }
        if (maxSessionsPerClient <= 0 || maxSessionsPerClient > maxSessions) {
            throw new IllegalArgumentException(
                    "Maximum sessions per client must be positive and no greater than the total maximum."
            );
        }
        this.maxSessions = maxSessions;
        this.maxSessionsPerClient = maxSessionsPerClient;
    }

    String openConnection() {
        return this.openConnection(null, null, null, null);
    }

    String openConnection(
            final String username,
            final String database,
            final Map<String, String> properties
    ) {
        return this.openConnection(null, username, database, properties);
    }

    synchronized String openConnection(
            final String clientId,
            final String username,
            final String database,
            final Map<String, String> properties
    ) {
        if (this.sessions.size() >= this.maxSessions) {
            throw new CapacityException("The JDBC server has reached its logical connection limit.");
        }

        final long clientSessionCount = this.sessions.values().stream()
                .filter(session -> Objects.equals(clientId, session.getClientId()))
                .count();
        if (clientSessionCount >= this.maxSessionsPerClient) {
            throw new CapacityException("This JDBC client has reached its logical connection limit.");
        }

        final String connectionId = UUID.randomUUID().toString();
        this.sessions.put(
                connectionId,
                new JdbcServerSession(connectionId, clientId, username, database, properties)
        );
        return connectionId;
    }

    boolean isOpen(final String connectionId) {
        return connectionId != null && this.sessions.containsKey(connectionId);
    }

    JdbcServerSession getSession(final String connectionId) {
        return connectionId == null ? null : this.sessions.get(connectionId);
    }

    JdbcServerSession getSession(final String connectionId, final String clientId) {
        final JdbcServerSession session = this.getSession(connectionId);
        if (session == null || !Objects.equals(clientId, session.getClientId())) {
            return null;
        }
        return session;
    }

    synchronized void closeConnection(final String connectionId) {
        if (connectionId != null) {
            this.sessions.remove(connectionId);
        }
    }

    synchronized List<String> closeClient(final String clientId) {
        final List<String> connectionIds = new ArrayList<>();
        this.sessions.entrySet().removeIf(entry -> {
            if (Objects.equals(clientId, entry.getValue().getClientId())) {
                connectionIds.add(entry.getKey());
                return true;
            }
            return false;
        });
        return connectionIds;
    }

    synchronized void clear() {
        this.sessions.clear();
    }

    static class CapacityException extends IllegalStateException {

        CapacityException(final String message) {
            super(message);
        }
    }
}
