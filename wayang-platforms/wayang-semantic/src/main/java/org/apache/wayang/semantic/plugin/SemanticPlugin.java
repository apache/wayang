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

package org.apache.wayang.semantic.plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;

public class SemanticPlugin implements Plugin {
    private final List<Mapping> mappings;

    private SemanticPlugin(final List<Mapping> mappings) {
        this.mappings = mappings;
    }

    public SemanticPlugin() {
        this.mappings = List.of();
    }

    @Override
    public Collection<Mapping> getMappings() {
        return mappings;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        final Set<Platform> platforms = new LinkedHashSet<>();
        // The surrounding pipeline (sources, sinks, plain transformations) always runs on Java.
        platforms.add(JavaPlatform.getInstance());
        for (final Mapping mapping : this.mappings) {
            for (final PlanTransformation transformation : mapping.getTransformations()) {
                platforms.addAll(transformation.getTargetPlatforms());
            }
        }
        return platforms;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return Java.basicPlugin().getChannelConversions();
    }

    @Override
    public void setProperties(final Configuration configuration) {
    }

    /**
     * Registers a {@link Mapping} that rewrites a semantic operator (e.g. a
     * {@link org.apache.wayang.basic.operators.SemanticFilterOperator}) into a physical operator for one
     * specific model implementation.
     */
    public SemanticPlugin withMapping(final Mapping mapping) {
        final List<Mapping> nextMappings = new ArrayList<>(this.mappings);
        nextMappings.add(mapping);
        return new SemanticPlugin(nextMappings);
    }
}
