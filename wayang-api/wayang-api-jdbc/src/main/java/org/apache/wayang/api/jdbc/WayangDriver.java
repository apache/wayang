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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver for Apache Wayang.
 *
 * Allows external tools to connect to Wayang using standard JDBC.
 *
 * Connection URL format:
 *   jdbc:wayang:<config-path>
 *
 * Example:
 *   jdbc:wayang:/path/to/wayang.properties
 *
 * Usage:
 *   Connection conn = DriverManager.getConnection("jdbc:wayang:/path/to/config.properties");
 *   Statement stmt = conn.createStatement();
 *   ResultSet rs = stmt.executeQuery("SELECT * FROM myTable");
 */
public class WayangDriver implements Driver {

    /** The prefix all Wayang JDBC URLs must start with */
    public static final String URL_PREFIX = "jdbc:wayang:";

    /** JDBC driver version */
    public static final int MAJOR_VERSION = 1;
    public static final int MINOR_VERSION = 0;

    // Auto-register this driver with the DriverManager when the class is loaded
    static {
        try {
            DriverManager.registerDriver(new WayangDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register WayangDriver", e);
        }
    }

    /**
     * Attempts to connect to Wayang using the given URL.
     *
     * @param url  Must start with "jdbc:wayang:"
     * @param info Optional properties (unused for now)
     * @return a WayangConnection, or null if the URL is not for this driver
     */
    @Override
    public Connection connect(final String url, final Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            // Returning null tells DriverManager this driver can't handle this URL
            return null;
        }

        // Extract the config path from the URL
        // e.g. "jdbc:wayang:/path/to/config.properties" -> "/path/to/config.properties"
        final String configPath = url.substring(URL_PREFIX.length());

        return new WayangConnection(url, configPath, info);
    }

    /**
     * Returns true if this driver can handle the given URL.
     * Only accepts URLs starting with "jdbc:wayang:"
     */
    @Override
    public boolean acceptsURL(final String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    /**
     * JDBC-compliant means it fully implements the JDBC spec.
     * We return false since this is an incomplete/custom implementation.
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("WayangDriver does not use java.util.logging");
    }
}