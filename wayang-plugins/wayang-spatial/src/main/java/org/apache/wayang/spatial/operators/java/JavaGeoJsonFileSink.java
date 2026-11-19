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

package org.apache.wayang.spatial.operators.java;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.GeoJsonFileSink;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.spatial.data.WayangGeometry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Java execution operator that writes {@link Record}s as features of a GeoJSON FeatureCollection.
 * Each Record holds the feature's geometry as a {@link WayangGeometry} and its properties as a
 * {@link Map}, which may be {@code null}, as emitted by {@link JavaGeoJsonFileSource}.
 */
public class JavaGeoJsonFileSink extends GeoJsonFileSink implements JavaExecutionOperator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public JavaGeoJsonFileSink(String outputUrl) {
        super(outputUrl);
    }

    public JavaGeoJsonFileSink(GeoJsonFileSink that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == 1;
        assert outputs.length == 0;

        JavaChannelInstance input = (JavaChannelInstance) inputs[0];
        final FileSystem fs = FileSystems.requireFileSystem(this.getOutputUrl());

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(fs.create(this.getOutputUrl()), StandardCharsets.UTF_8))) {
            writer.write("{\"type\":\"FeatureCollection\",\"features\":[");

            boolean[] first = {true};
            input.<Record>provideStream().forEach(record -> {
                try {
                    if (!first[0]) writer.write(",");
                    writer.write(MAPPER.writeValueAsString(toFeature(record)));
                    first[0] = false;
                } catch (IOException e) {
                    throw new WayangException("Failed to write GeoJSON feature.", e);
                }
            });

            writer.write("]}");
        } catch (IOException e) {
            throw new WayangException("Failed to write GeoJSON file: " + this.getOutputUrl(), e);
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    /**
     * Converts a {@link Record} into a GeoJSON Feature.
     */
    private static ObjectNode toFeature(Record record) throws IOException {
        if (record.size() <= PROPERTIES_FIELD_INDEX) {
            throw new WayangException(String.format(
                    "Expected a GeoJSON record with at least %d fields, got %s.",
                    PROPERTIES_FIELD_INDEX + 1, record));
        }
        final Object geometryField = record.getField(GEOMETRY_FIELD_INDEX);
        final Object propertiesField = record.getField(PROPERTIES_FIELD_INDEX);
        if (!(geometryField instanceof WayangGeometry)) {
            throw new WayangException(String.format(
                    "Expected a %s in field %d of %s, got %s.",
                    WayangGeometry.class.getSimpleName(), GEOMETRY_FIELD_INDEX, record,
                    geometryField == null ? "null" : geometryField.getClass().getName()));
        }
        if (propertiesField != null && !(propertiesField instanceof Map)) {
            throw new WayangException(String.format(
                    "Expected a %s or null in field %d of %s, got %s.",
                    Map.class.getSimpleName(), PROPERTIES_FIELD_INDEX, record,
                    propertiesField.getClass().getName()));
        }

        final JsonNode geometry = MAPPER.readTree(((WayangGeometry) geometryField).getGeoJSON());
        if (geometry instanceof ObjectNode) {
            // JTS adds a crs member, which RFC 7946 no longer allows.
            ((ObjectNode) geometry).remove("crs");
        }

        final ObjectNode feature = MAPPER.createObjectNode();
        feature.put("type", "Feature");
        feature.set("geometry", geometry);
        if (propertiesField == null) {
            // GeoJSON allows null properties.
            feature.putNull("properties");
        } else {
            feature.set("properties", MAPPER.valueToTree(propertiesField));
        }
        return feature;
    }

    @Override
    public JavaGeoJsonFileSink copy() {
        return new JavaGeoJsonFileSink(this.getOutputUrl());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.geojsonfilesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException();
    }
}
