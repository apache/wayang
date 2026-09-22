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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorStoreTest {

    @Test
    void fetchAdvancesOnlyAfterResponseFactorySucceeds() {
        final CursorStore store = new CursorStore();
        final String cursorId = store.openCursor("connection-1", "statement-1", rows(), 0);

        assertThrows(
                IllegalStateException.class,
                () -> store.fetch("connection-1", cursorId, 2, batch -> {
                    throw new IllegalStateException("Could not encode response.");
                })
        );

        final CursorStore.FetchBatch firstBatch = store.fetch("connection-1", cursorId, 2);
        assertEquals(Arrays.asList(Collections.singletonList(1), Collections.singletonList(2)), firstBatch.getRows());
        assertTrue(firstBatch.hasMoreRows());

        final CursorStore.FetchBatch secondBatch = store.fetch("connection-1", cursorId, 2);
        assertEquals(Collections.singletonList(Collections.singletonList(3)), secondBatch.getRows());
        assertFalse(secondBatch.hasMoreRows());
        assertNull(store.fetch("connection-1", cursorId, 1));
    }

    @Test
    void openCursorAddsOnlyAfterResponseFactorySucceeds() {
        final CursorStore store = new CursorStore();
        final AtomicReference<String> exposedCursorId = new AtomicReference<>();

        assertThrows(
                IllegalStateException.class,
                () -> store.openCursor("connection-1", "statement-1", rows(), 1, cursorId -> {
                    exposedCursorId.set(cursorId);
                    throw new IllegalStateException("Could not build response.");
                })
        );

        assertNull(store.fetch("connection-1", exposedCursorId.get(), 1));
    }

    @Test
    void openingCursorForStatementReplacesPreviousCursor() {
        final CursorStore store = new CursorStore();
        final String firstCursorId = store.openCursor("connection-1", "statement-1", rows(), 0);
        final String secondCursorId = store.openCursor("connection-1", "statement-1", rows(), 1);

        assertNull(store.fetch("connection-1", firstCursorId, 1));
        assertEquals(
                Collections.singletonList(Collections.singletonList(2)),
                store.fetch("connection-1", secondCursorId, 1).getRows()
        );
    }

    @Test
    void closeConnectionRemovesConnectionCursorsOnly() {
        final CursorStore store = new CursorStore();
        final String firstCursorId = store.openCursor("connection-1", "statement-1", rows(), 0);
        final String secondCursorId = store.openCursor("connection-2", "statement-1", rows(), 0);

        store.closeConnection("connection-1");

        assertNull(store.fetch("connection-1", firstCursorId, 1));
        assertEquals(
                Collections.singletonList(Collections.singletonList(1)),
                store.fetch("connection-2", secondCursorId, 1).getRows()
        );
    }

    @Test
    void enforcesCursorCapacity() {
        final CursorStore store = new CursorStore(1, 1);
        store.openCursor("connection-1", "statement-1", rows(), 0);

        assertThrows(
                CursorStore.CapacityException.class,
                () -> store.openCursor("connection-2", "statement-1", rows(), 0)
        );
    }

    private static List<List<Object>> rows() {
        return Arrays.asList(
                Collections.singletonList(1),
                Collections.singletonList(2),
                Collections.singletonList(3)
        );
    }
}
