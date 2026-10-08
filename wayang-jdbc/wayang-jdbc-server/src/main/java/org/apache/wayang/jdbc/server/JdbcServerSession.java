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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Server-side state for one logical JDBC connection.
 */
public final class JdbcServerSession {

    private final String connectionId;

    private final String clientId;

    private final String username;

    private final String database;

    private final Map<String, String> properties;

    JdbcServerSession(
            final String connectionId,
            final String clientId,
            final String username,
            final String database,
            final Map<String, String> properties
    ) {
        this.connectionId = connectionId;
        this.clientId = clientId;
        this.username = username;
        this.database = database;
        this.properties = properties == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(properties);
    }

    public String getConnectionId() {
        return this.connectionId;
    }

    String getClientId() {
        return this.clientId;
    }

    public String getUsername() {
        return this.username;
    }

    public String getDatabase() {
        return this.database;
    }

    public Map<String, String> getProperties() {
        return new LinkedHashMap<>(this.properties);
    }
}
