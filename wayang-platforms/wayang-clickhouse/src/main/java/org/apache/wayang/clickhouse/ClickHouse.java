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

package org.apache.wayang.clickhouse;

import org.apache.wayang.clickhouse.platform.ClickHousePlatform;
import org.apache.wayang.clickhouse.plugin.ClickHouseConversionsPlugin;
import org.apache.wayang.clickhouse.plugin.ClickHousePlugin;

/**
 * Register for relevant components of this module.
 */
public class ClickHouse {

    private static final ClickHousePlugin PLUGIN = new ClickHousePlugin();

    private static final ClickHouseConversionsPlugin CONVERSIONS_PLUGIN = new ClickHouseConversionsPlugin();

    /**
     * Retrieve the {@link ClickHousePlugin}.
     *
     * @return the {@link ClickHousePlugin}
     */
    public static ClickHousePlugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link ClickHouseConversionsPlugin}.
     *
     * @return the {@link ClickHouseConversionsPlugin}
     */
    public static ClickHouseConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }

    /**
     * Retrieve the {@link ClickHousePlatform}.
     *
     * @return the {@link ClickHousePlatform}
     */
    public static ClickHousePlatform platform() {
        return ClickHousePlatform.getInstance();
    }

}
