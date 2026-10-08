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

package org.apache.wayang.jdbc.protocol.message;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Opens a logical JDBC connection on the Wayang JDBC server.
 */
public class OpenConnectionRequest {

    private String username;

    private String database;

    private Map<String, String> properties = new LinkedHashMap<>();

    public OpenConnectionRequest() {
    }

    public OpenConnectionRequest(
            final String username,
            final String database,
            final Map<String, String> properties
    ) {
        this.username = username;
        this.database = database;
        this.properties = properties;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(final String username) {
        this.username = username;
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(final String database) {
        this.database = database;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public void setProperties(final Map<String, String> properties) {
        this.properties = properties;
    }
}
