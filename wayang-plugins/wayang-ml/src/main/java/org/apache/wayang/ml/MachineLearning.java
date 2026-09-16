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

package org.apache.wayang.ml;

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.platform.SparkPlatform;

/**
 * Provides {@link Plugin}s that enable usage of the xxxx.
 */
public class MachineLearning {

    private static final String DEFAULT_CONFIG_FILE = "wayang-ml-defaults.properties";

    /**
     * Enables use with the {@link JavaPlatform} and {@link SparkPlatform}.
     */
    private static final Plugin PLUGIN = new Plugin() {

        @Override
        public Collection<Platform> getRequiredPlatforms() {
            return Collections.emptyList();
        }

        @Override
        public Collection<Mapping> getMappings() {
            return Collections.emptyList();
        }

        @Override
        public Collection<ChannelConversion> getChannelConversions() {
            return Collections.emptyList();
        }

        @Override
        public void setProperties(final Configuration configuration) {
            final InputStream is = ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE);
            if (is != null) {
                configuration.load(is);
            }
        }
    };

    /**
     * Retrieve a {@link Plugin} to use xxx on the
     *
     * @return the {@link Plugin}
     */
    public static Plugin plugin() {
        return PLUGIN;
    }
}
