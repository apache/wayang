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

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcServerSessionManagerTest {

    @Test
    void enforcesClientOwnershipAndCapacity() {
        final JdbcServerSessionManager manager = new JdbcServerSessionManager(2, 1);

        final String firstConnectionId = manager.openConnection(
                "client-a",
                "user-a",
                "analytics",
                Collections.singletonMap("property", "value")
        );

        final JdbcServerSession session = manager.getSession(firstConnectionId, "client-a");
        assertNotNull(session);
        assertEquals("client-a", session.getClientId());
        assertEquals("user-a", session.getUsername());
        assertEquals("analytics", session.getDatabase());
        assertEquals("value", session.getProperties().get("property"));
        assertNull(manager.getSession(firstConnectionId, "client-b"));

        assertThrows(
                JdbcServerSessionManager.CapacityException.class,
                () -> manager.openConnection("client-a", null, null, null)
        );

        final String secondConnectionId = manager.openConnection("client-b", null, null, null);
        assertTrue(manager.isOpen(secondConnectionId));

        assertThrows(
                JdbcServerSessionManager.CapacityException.class,
                () -> manager.openConnection("client-c", null, null, null)
        );
    }

    @Test
    void closeClientRemovesOnlyOwnedSessions() {
        final JdbcServerSessionManager manager = new JdbcServerSessionManager(3, 2);
        final String firstConnectionId = manager.openConnection("client-a", null, null, null);
        final String secondConnectionId = manager.openConnection("client-a", null, null, null);
        final String thirdConnectionId = manager.openConnection("client-b", null, null, null);

        final List<String> closedConnectionIds = manager.closeClient("client-a");

        assertEquals(2, closedConnectionIds.size());
        assertTrue(closedConnectionIds.contains(firstConnectionId));
        assertTrue(closedConnectionIds.contains(secondConnectionId));
        assertFalse(manager.isOpen(firstConnectionId));
        assertFalse(manager.isOpen(secondConnectionId));
        assertTrue(manager.isOpen(thirdConnectionId));
    }
}
