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

package org.apache.wayang.spatial.mapping;

import org.apache.wayang.basic.operators.GeoJsonFileSource;
import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.postgres.platform.PostgresPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link Mapping}s for the {@link SpatialFilterOperator}, {@link SpatialJoinOperator}, and {@link GeoJsonFileSource}.
 */
public class Mappings {

    /**
     * {@link Mapping}s towards the {@link JavaPlatform}.
     */
    public static Collection<Mapping> javaMappings = Arrays.asList(
            new org.apache.wayang.spatial.mapping.java.SpatialFilterMapping(),
            new org.apache.wayang.spatial.mapping.java.SpatialJoinMapping(),
            new org.apache.wayang.spatial.mapping.java.GeoJsonFileSourceMapping(),
            new org.apache.wayang.spatial.mapping.java.GeoJsonFileSinkMapping()
    );

    /**
     * {@link Mapping}s towards the {@link SparkPlatform}.
     */
    public static Collection<Mapping> sparkMappings = Arrays.asList(
            new org.apache.wayang.spatial.mapping.spark.SpatialFilterMapping(),
            new org.apache.wayang.spatial.mapping.spark.SpatialJoinMapping()
    );

    /**
     * {@link Mapping}s towards the {@link PostgresPlatform}.
     */
    public static Collection<Mapping> postgresMappings = Arrays.asList(
            new org.apache.wayang.spatial.mapping.postgres.SpatialFilterMapping(),
            new org.apache.wayang.spatial.mapping.postgres.SpatialJoinMapping()
    );

}
