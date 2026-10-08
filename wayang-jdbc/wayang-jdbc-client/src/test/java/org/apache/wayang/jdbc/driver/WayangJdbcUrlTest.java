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

import org.apache.wayang.jdbc.protocol.ProtocolConstants;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WayangJdbcUrlTest {

    @Test
    void parsesHostPortDatabaseAndProperties() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty("user", "property-user");
        properties.setProperty("connectTimeout", "1000");

        final WayangJdbcUrl url = WayangJdbcUrl.parse(
                "jdbc:wayang://localhost:9998/sales%20data?user=url-user&password=s3cret",
                properties
        );

        assertEquals("localhost", url.getHost());
        assertEquals(9998, url.getPort());
        assertEquals("sales data", url.getDatabase());
        assertEquals("url-user", url.getProperties().getProperty("user"));
        assertEquals("s3cret", url.getProperties().getProperty("password"));
        assertEquals("1000", url.getProperties().getProperty("connectTimeout"));
    }

    @Test
    void usesDefaultPortAndOptionalDatabase() throws Exception {
        final WayangJdbcUrl url = WayangJdbcUrl.parse("jdbc:wayang://127.0.0.1", null);

        assertEquals("127.0.0.1", url.getHost());
        assertEquals(ProtocolConstants.DEFAULT_PORT, url.getPort());
        assertNull(url.getDatabase());
    }

    @Test
    void acceptsOnlyWayangJdbcUrls() {
        assertTrue(WayangJdbcUrl.accepts("jdbc:wayang://localhost:9999/db"));
        assertFalse(WayangJdbcUrl.accepts("jdbc:postgresql://localhost/db"));
        assertFalse(WayangJdbcUrl.accepts(null));
    }

    @Test
    void rejectsInvalidUrls() {
        assertThrows(SQLException.class, () -> WayangJdbcUrl.parse("jdbc:wayang://", null));
        assertThrows(SQLException.class, () -> WayangJdbcUrl.parse("jdbc:wayang://localhost:0/db", null));
        assertThrows(SQLException.class, () -> WayangJdbcUrl.parse("jdbc:wayang://localhost/db/extra", null));
        assertThrows(SQLException.class, () -> WayangJdbcUrl.parse("jdbc:wayang://localhost/db?=value", null));
        assertThrows(SQLException.class, () -> WayangJdbcUrl.parse("jdbc:wayang://localhost/db%2Fextra", null));
        assertThrows(SQLException.class, () -> WayangJdbcUrl.parse("jdbc:wayang://user@localhost/db", null));
    }
}
