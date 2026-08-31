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

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public final class WayangJdbcUrl {

    public static final String PREFIX = "jdbc:wayang://";

    private final String host;

    private final int port;

    private final String database;

    private final Properties properties;

    private WayangJdbcUrl(
            final String host,
            final int port,
            final String database,
            final Properties properties
    ) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.properties = properties;
    }

    public static boolean accepts(final String url) {
        return url != null && url.startsWith(PREFIX);
    }

    public static WayangJdbcUrl parse(final String url, final Properties inputProperties) throws SQLException {
        if (!accepts(url)) {
            throw new SQLException("Invalid Wayang JDBC URL: " + url);
        }

        final String uriValue = "wayang://" + url.substring(PREFIX.length());

        final URI uri;
        try {
            uri = new URI(uriValue);
        } catch (URISyntaxException e) {
            throw new SQLException("Could not parse Wayang JDBC URL: " + url, e);
        }

        final String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new SQLException("Wayang JDBC URL must contain a host.");
        }
        if (uri.getRawUserInfo() != null) {
            throw new SQLException("Wayang JDBC URLs do not support user information.");
        }
        if (uri.getRawFragment() != null) {
            throw new SQLException("Wayang JDBC URLs do not support fragments.");
        }

        final int port = uri.getPort() == -1 ? ProtocolConstants.DEFAULT_PORT : uri.getPort();
        if (port < 1 || port > 65_535) {
            throw new SQLException("Wayang JDBC URL port must be between 1 and 65535.");
        }
        final String database = parseDatabase(uri.getRawPath());

        final Properties properties = new Properties();
        if (inputProperties != null) {
            properties.putAll(inputProperties);
        }
        // JDBC URL query properties deliberately take precedence over the
        // Properties supplied to Driver.connect.
        parseQueryProperties(uri.getRawQuery()).forEach(properties::setProperty);

        return new WayangJdbcUrl(host, port, database, properties);
    }

    private static String parseDatabase(final String rawPath) throws SQLException {
        if (rawPath == null || rawPath.isEmpty() || "/".equals(rawPath)) {
            return null;
        }
        if (!rawPath.startsWith("/")) {
            throw new SQLException("Wayang JDBC URL database path must start with '/'.");
        }

        final String rawDatabase = rawPath.substring(1);
        if (rawDatabase.isEmpty() || rawDatabase.indexOf('/') >= 0) {
            throw new SQLException("Wayang JDBC URL supports at most one database path segment.");
        }

        final String database = percentDecode(rawDatabase, "database");
        if (database.isBlank() || database.indexOf('/') >= 0) {
            throw new SQLException("Wayang JDBC URL database must be a non-blank path segment.");
        }
        return database;
    }

    private static Map<String, String> parseQueryProperties(final String query) throws SQLException {
        final Map<String, String> properties = new LinkedHashMap<>();
        if (query == null || query.isBlank()) {
            return properties;
        }

        final String[] pairs = query.split("&", -1);
        for (String pair : pairs) {
            final int separator = pair.indexOf('=');
            if (separator <= 0) {
                throw new SQLException("Invalid Wayang JDBC URL query property: " + pair);
            }
            final String name = percentDecode(pair.substring(0, separator), "query property name");
            final String value = percentDecode(pair.substring(separator + 1), "query property value");
            if (name.isBlank()) {
                throw new SQLException("Wayang JDBC URL query property names must not be blank.");
            }
            properties.put(name, value);
        }

        return properties;
    }

    private static String percentDecode(
            final String value,
            final String fieldName
    ) throws SQLException {
        final StringBuilder decoded = new StringBuilder(value.length());
        int index = 0;
        while (index < value.length()) {
            if (value.charAt(index) != '%') {
                decoded.append(value.charAt(index));
                index++;
                continue;
            }

            final ByteArrayOutputStream escapedBytes = new ByteArrayOutputStream();
            while (index < value.length() && value.charAt(index) == '%') {
                if (index + 2 >= value.length()) {
                    throw invalidPercentEncoding(fieldName);
                }
                final int high = Character.digit(value.charAt(index + 1), 16);
                final int low = Character.digit(value.charAt(index + 2), 16);
                if (high < 0 || low < 0) {
                    throw invalidPercentEncoding(fieldName);
                }
                escapedBytes.write((high << 4) + low);
                index += 3;
            }

            try {
                decoded.append(StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(escapedBytes.toByteArray())));
            } catch (CharacterCodingException e) {
                throw new SQLException(
                        "Wayang JDBC URL " + fieldName + " is not valid UTF-8.",
                        e
                );
            }
        }
        return decoded.toString();
    }

    private static SQLException invalidPercentEncoding(final String fieldName) {
        return new SQLException(
                "Wayang JDBC URL " + fieldName + " contains invalid percent encoding."
        );
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public String getDatabase() {
        return this.database;
    }

    public Properties getProperties() {
        final Properties copy = new Properties();
        copy.putAll(this.properties);
        return copy;
    }
}
