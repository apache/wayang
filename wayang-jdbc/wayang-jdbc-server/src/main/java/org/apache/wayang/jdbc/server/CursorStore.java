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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

/**
 * Stores in-memory row cursors for query results that need fetch paging.
 */
class CursorStore {

    private static final int DEFAULT_MAX_CURSORS = 1024;

    private static final int DEFAULT_MAX_CURSORS_PER_CONNECTION = 64;

    private final Map<String, Cursor> cursors = new HashMap<>();

    private final int maxCursors;

    private final int maxCursorsPerConnection;

    CursorStore() {
        this(DEFAULT_MAX_CURSORS, DEFAULT_MAX_CURSORS_PER_CONNECTION);
    }

    CursorStore(final int maxCursors, final int maxCursorsPerConnection) {
        if (maxCursors <= 0) {
            throw new IllegalArgumentException("Maximum cursor count must be positive.");
        }
        if (maxCursorsPerConnection <= 0 || maxCursorsPerConnection > maxCursors) {
            throw new IllegalArgumentException(
                    "Maximum cursors per connection must be positive and no greater than the total maximum."
            );
        }
        this.maxCursors = maxCursors;
        this.maxCursorsPerConnection = maxCursorsPerConnection;
    }

    synchronized String openCursor(
            final String connectionId,
            final String statementId,
            final List<List<Object>> rows,
            final int position
    ) {
        return this.openCursor(
                connectionId,
                statementId,
                rows,
                position,
                cursorId -> cursorId
        );
    }

    /**
     * Opens a cursor only after {@code responseFactory} has successfully
     * created the response that exposes its id.
     */
    synchronized <T> T openCursor(
            final String connectionId,
            final String statementId,
            final List<List<Object>> rows,
            final int position,
            final Function<String, T> responseFactory
    ) {
        if (connectionId == null || statementId == null || rows == null) {
            throw new IllegalArgumentException("Cursor connection, statement, and rows must not be null.");
        }
        if (responseFactory == null) {
            throw new IllegalArgumentException("Cursor response factory must not be null.");
        }
        if (position < 0 || position >= rows.size()) {
            throw new IllegalArgumentException("Cursor position must reference an unread row.");
        }

        this.closeStatement(connectionId, statementId);
        if (this.cursors.size() >= this.maxCursors) {
            throw new CapacityException("The JDBC server has reached its open cursor limit.");
        }

        final long connectionCursorCount = this.cursors.values().stream()
                .filter(cursor -> cursor.connectionId.equals(connectionId))
                .count();
        if (connectionCursorCount >= this.maxCursorsPerConnection) {
            throw new CapacityException("This JDBC connection has reached its open cursor limit.");
        }

        final String cursorId = UUID.randomUUID().toString();
        final T response = responseFactory.apply(cursorId);
        this.cursors.put(cursorId, new Cursor(connectionId, statementId, rows, position));
        return response;
    }

    synchronized FetchBatch fetch(final String connectionId, final String cursorId, final int fetchSize) {
        return this.fetch(connectionId, cursorId, fetchSize, batch -> batch);
    }

    /**
     * Advances a cursor only after {@code responseFactory} has successfully
     * created a response for the selected batch.
     */
    synchronized <T> T fetch(
            final String connectionId,
            final String cursorId,
            final int fetchSize,
            final Function<FetchBatch, T> responseFactory
    ) {
        if (fetchSize <= 0) {
            throw new IllegalArgumentException("Fetch size must be positive.");
        }
        if (responseFactory == null) {
            throw new IllegalArgumentException("Fetch response factory must not be null.");
        }
        final Cursor cursor = this.cursors.get(cursorId);
        if (cursor == null || !cursor.connectionId.equals(connectionId)) {
            return null;
        }

        final FetchBatch batch = cursor.preview(fetchSize);
        final T response = responseFactory.apply(batch);
        cursor.advance(batch.getRows().size());
        if (!batch.hasMoreRows()) {
            this.cursors.remove(cursorId);
        }
        return response;
    }

    synchronized boolean closeCursor(
            final String connectionId,
            final String statementId,
            final String cursorId
    ) {
        final Cursor cursor = this.cursors.get(cursorId);
        if (cursor == null
                || !cursor.connectionId.equals(connectionId)
                || !cursor.statementId.equals(statementId)) {
            return false;
        }
        return this.cursors.remove(cursorId) != null;
    }

    synchronized boolean cancelCursor(
            final String connectionId,
            final String statementId,
            final String cursorId
    ) {
        return this.closeCursor(connectionId, statementId, cursorId);
    }

    synchronized void closeStatement(final String connectionId, final String statementId) {
        this.cursors.entrySet().removeIf(entry ->
                entry.getValue().connectionId.equals(connectionId)
                        && entry.getValue().statementId.equals(statementId)
        );
    }

    synchronized void closeConnection(final String connectionId) {
        this.cursors.entrySet().removeIf(entry ->
                Objects.equals(entry.getValue().connectionId, connectionId)
        );
    }

    synchronized void clear() {
        this.cursors.clear();
    }

    static class FetchBatch {

        private final List<List<Object>> rows;

        private final boolean hasMoreRows;

        FetchBatch(final List<List<Object>> rows, final boolean hasMoreRows) {
            this.rows = rows;
            this.hasMoreRows = hasMoreRows;
        }

        List<List<Object>> getRows() {
            return this.rows;
        }

        boolean hasMoreRows() {
            return this.hasMoreRows;
        }
    }

    private static class Cursor {

        private final String connectionId;

        private final String statementId;

        private final List<List<Object>> rows;

        private int position;

        Cursor(
                final String connectionId,
                final String statementId,
                final List<List<Object>> rows,
                final int position
        ) {
            this.connectionId = connectionId;
            this.statementId = statementId;
            this.rows = rows;
            this.position = position;
        }

        FetchBatch preview(final int fetchSize) {
            final int end = Math.min(this.position + fetchSize, this.rows.size());
            final List<List<Object>> batch = new ArrayList<>(this.rows.subList(this.position, end));
            return new FetchBatch(batch, end < this.rows.size());
        }

        void advance(final int rowCount) {
            this.position += rowCount;
        }
    }

    static class CapacityException extends IllegalStateException {

        CapacityException(final String message) {
            super(message);
        }
    }
}
