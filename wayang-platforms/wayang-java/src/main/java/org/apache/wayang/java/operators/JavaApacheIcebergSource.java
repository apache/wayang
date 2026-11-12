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
package org.apache.wayang.java.operators;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics.ScanBuilder;
import org.apache.iceberg.expressions.Expression;
import org.apache.wayang.basic.operators.ApacheIcebergSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

/**
 * This is execution operator implements the {@link ApacheIcebergSource}.
 */
public class JavaApacheIcebergSource 
extends ApacheIcebergSource implements JavaExecutionOperator {

    /**
     * Creates a new Java Iceberg source instance.
     *
     * @param catalog          {@link org.apache.iceberg.catalog.Catalog} catalog
     *                         used to load the table
     * @param tableIdentifier  {@linkorg.apache.iceberg.catalog.TableIdentifier} identifier
     *                         of the target table
     * @param whereExpressions Collection of
     *                         {@link org.apache.iceberg.expressions.Expression}
     *                         filters; empty list for none
     * @param columns          collection of column names to project; empty list for
     *                         all columns
     */

    public JavaApacheIcebergSource(Catalog catalog, TableIdentifier tableIdentifier,
            Expression[] whereExpressions, String[] columns) {
        super(ApacheIcebergSource.create(catalog, tableIdentifier, whereExpressions, columns));
    }

    /**
     * Creates a {@link Stream} of {@link org.apache.wayang.basic.data.Record}
     * objects
     * from the given Iceberg {@link org.apache.iceberg.data.ScanBuilder}.
     *
     * @param scanBuilder the configured {@link org.apache.iceberg.data.ScanBuilder}
     *                    used to read table data
     * @return a sequential {@link Stream} of Wayang
     *         {@link org.apache.wayang.basic.data.Record} instances
     */
    private static Stream<org.apache.wayang.basic.data.Record> getStreamFromIcebergTable(ScanBuilder scanBuilder) {
        return StreamSupport.stream(scanBuilder.build().spliterator(), false)
                .map(record -> getWayangRecord(record));
    }

    /**
     * Converts an Iceberg {@link org.apache.iceberg.data.Record} to a Wayang
     * {@link org.apache.wayang.basic.data.Record}.
     *
     * @param icebergRecord the Iceberg record to convert
     * @return a new Wayang {@link org.apache.wayang.basic.data.Record} containing
     *         the same field values
     */
    private static org.apache.wayang.basic.data.Record getWayangRecord(org.apache.iceberg.data.Record icebergRecord) {
        Object[] values = new Object[icebergRecord.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = icebergRecord.get(i);
        }
        return new org.apache.wayang.basic.data.Record(values);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        String tableName = this.getIcebergTableName();

        try {

            ScanBuilder scanBuilder = this.getScanBuilder();

            ((StreamChannel.Instance) outputs[0]).accept(getStreamFromIcebergTable(scanBuilder));

            ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
            prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                    "wayang.java.apacheicebergsource.load.prepare", javaExecutor.getConfiguration()));
            ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
            mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                    "wayang.java.apacheicebergsource.load.main", javaExecutor.getConfiguration()));

            outputs[0].getLineage().addPredecessor(mainLineageNode);

            return prepareLineageNode.collectAndMark();

        } catch (Exception e) {
            throw new WayangException(String.format("Reading from Apache Iceberg Source table %s failed.", tableName),
                    e);

        }

    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaApacheIcebergSource(ApacheIcebergSource that) {
        super(that);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.apacheicebergsource.load.prepare",
                "wayang.java.apacheicebergsource.load.main");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
