/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.clickhouse.platform;

import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

/**
 * {@link Platform} implementation for ClickHouse.
 */
public class ClickHousePlatform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "ClickHouse";

    private static final String CONFIG_NAME = "clickhouse";

    private static ClickHousePlatform instance = null;

    public static ClickHousePlatform getInstance() {
        if (instance == null) {
            instance = new ClickHousePlatform();
        }
        return instance;
    }

    protected ClickHousePlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public String getJdbcDriverClassName() {
        // Prefer the non-deprecated Driver; ClickHouseDriver remains only as a
        // compatibility alias in clickhouse-jdbc 0.9.x.
        return com.clickhouse.jdbc.Driver.class.getName();
    }

}
