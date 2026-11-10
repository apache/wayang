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

package org.apache.wayang.clickhouse.operators;

import org.apache.wayang.clickhouse.ClickHouse;
import org.apache.wayang.clickhouse.mapping.FilterMapping;
import org.apache.wayang.clickhouse.mapping.ProjectionMapping;
import org.apache.wayang.clickhouse.platform.ClickHousePlatform;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.java.platform.JavaPlatform;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke tests for ClickHouse plugin registration.
 */
class ClickHousePluginTest {

    @Test
    void pluginExposesPlatformsMappingsAndConversions() {
        assertTrue(ClickHouse.plugin().getRequiredPlatforms().contains(ClickHousePlatform.getInstance()));
        assertTrue(ClickHouse.plugin().getRequiredPlatforms().contains(JavaPlatform.getInstance()));
        assertEquals(2, ClickHouse.plugin().getChannelConversions().size());

        Collection<Mapping> mappings = ClickHouse.plugin().getMappings();
        assertEquals(2, mappings.size());
        assertTrue(mappings.stream().anyMatch(FilterMapping.class::isInstance));
        assertTrue(mappings.stream().anyMatch(ProjectionMapping.class::isInstance));
    }

    @Test
    void conversionsPluginHasNoMappings() {
        assertTrue(ClickHouse.conversionPlugin().getMappings().isEmpty());
        assertEquals(2, ClickHouse.conversionPlugin().getChannelConversions().size());
    }
}
