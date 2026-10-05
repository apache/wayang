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

package org.apache.wayang.api.jdbc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Unit tests for the Wayang JDBC Driver.
 * Tests that the driver registers correctly and handles URLs properly.
 */
public class WayangDriverTest {

    @Test
    public void testDriverRegistered() throws Exception {
        // Force class loading which triggers static registration
        Class.forName("org.apache.wayang.api.jdbc.WayangDriver");

        // Check driver is registered with DriverManager
        Driver driver = DriverManager.getDriver("jdbc:wayang:test");
        assertNotNull(driver, "WayangDriver should be registered");
        assertInstanceOf(WayangDriver.class, driver);
    }

    @Test
    public void testAcceptsWayangUrl() throws Exception {
        final WayangDriver driver = new WayangDriver();
        assertTrue(driver.acceptsURL("jdbc:wayang:/path/to/config.properties"));
        assertTrue(driver.acceptsURL("jdbc:wayang:"));
    }

    @Test
    public void testRejectsNonWayangUrl() throws Exception {
        final WayangDriver driver = new WayangDriver();
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost/db"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost/db"));
        assertFalse(driver.acceptsURL(null));
    }

    @Test
    public void testDriverVersion() {
        final WayangDriver driver = new WayangDriver();
        assertEquals(1, driver.getMajorVersion());
        assertEquals(0, driver.getMinorVersion());
    }

    @Test
    public void testConnectReturnsNullForNonWayangUrl() throws Exception {
        final WayangDriver driver = new WayangDriver();
        // Should return null for non-Wayang URLs (JDBC spec requirement)
        assertNull(driver.connect("jdbc:mysql://localhost/db", new Properties()));
    }

    @Test
    public void testUrlPrefix() {
        assertEquals("jdbc:wayang:", WayangDriver.URL_PREFIX);
    }
}