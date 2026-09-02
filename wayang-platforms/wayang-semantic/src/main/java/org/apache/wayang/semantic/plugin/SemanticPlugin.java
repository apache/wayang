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

import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.basic.operators.SemanticFilterOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.semantic.mappings.JavaFilterMapping;
import org.apache.wayang.semantic.udf.SemanticAlgorithm;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
        // TODO: maybe we should find another way to handle this? but do Java for now.
        return Collections.singleton(JavaPlatform.getInstance());
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return Java.basicPlugin().getChannelConversions();
    }

    @Override
    public void setProperties(final Configuration configuration) {
    }

    public SemanticPlugin withOperatorMapping(final Class<?> operatorClass, final SemanticAlgorithm<?, ?> model) {
        final List<Mapping> nextMappings = new ArrayList<>(this.mappings);

        if (operatorClass.equals(SemanticFilterOperator.class)) {
            nextMappings.add(new JavaFilterMapping((SemanticAlgorithm<Object, Boolean>) model));
        }

        return new SemanticPlugin(nextMappings);
    }
}