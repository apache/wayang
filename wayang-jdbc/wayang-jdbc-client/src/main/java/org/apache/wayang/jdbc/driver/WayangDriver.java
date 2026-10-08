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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class WayangDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new WayangDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(final String url, final Properties info) throws SQLException {
        if (!this.acceptsURL(url)) {
            return null;
        }

        final WayangJdbcUrl jdbcUrl = WayangJdbcUrl.parse(url, info);
        final WayangJdbcClient client = new WayangJdbcClient(jdbcUrl);
        return new WayangConnection(client, jdbcUrl.getDatabase(), url, jdbcUrl.getProperties());
    }

    @Override
    public boolean acceptsURL(final String url) {
        return WayangJdbcUrl.accepts(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(
            final String url,
            final Properties info
    ) throws SQLException {
        final Properties properties = new Properties();
        if (info != null) {
            properties.putAll(info);
        }
        if (url != null && this.acceptsURL(url)) {
            properties.putAll(WayangJdbcUrl.parse(url, info).getProperties());
        }

        return new DriverPropertyInfo[]{
                this.property(
                        "user",
                        properties.getProperty("user"),
                        "Optional user name reported to the Wayang JDBC server."
                ),
                this.property(
                        "password",
                        properties.getProperty("password"),
                        "Optional password passed to the Wayang JDBC server."
                ),
                this.property(
                        "connectTimeout",
                        properties.getProperty("connectTimeout"),
                        "TCP connect and initial server handshake timeout in milliseconds; "
                                + "zero uses DriverManager.getLoginTimeout or the driver default."
                )
        };
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Wayang JDBC does not use java.util.logging.");
    }

    private DriverPropertyInfo property(
            final String name,
            final String value,
            final String description
    ) {
        final DriverPropertyInfo property = new DriverPropertyInfo(name, value);
        property.description = description;
        property.required = false;
        return property;
    }
}
