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

package org.apache.wayang.duckdb;

import org.apache.wayang.duckdb.platform.DuckDBPlatform;
import org.apache.wayang.duckdb.plugin.DuckDBConversionsPlugin;
import org.apache.wayang.duckdb.plugin.DuckDBPlugin;

/**
 * Entry point that exposes the relevant components of the DuckDB platform.
 *
 * <p>Typical usage:
 * <pre>{@code
 *   new WayangContext(config)
 *       .withPlugin(Java.basicPlugin())
 *       .withPlugin(DuckDB.plugin());
 * }</pre>
 */
public class DuckDB {

    private static final DuckDBPlugin PLUGIN = new DuckDBPlugin();

    private static final DuckDBConversionsPlugin CONVERSIONS_PLUGIN = new DuckDBConversionsPlugin();

    /**
     * @return the {@link DuckDBPlugin} (operator mappings + channel conversions)
     */
    public static DuckDBPlugin plugin() {
        return PLUGIN;
    }

    /**
     * @return the {@link DuckDBConversionsPlugin} (channel conversions only)
     */
    public static DuckDBConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }

    /**
     * @return the {@link DuckDBPlatform}
     */
    public static DuckDBPlatform platform() {
        return DuckDBPlatform.getInstance();
    }

}
