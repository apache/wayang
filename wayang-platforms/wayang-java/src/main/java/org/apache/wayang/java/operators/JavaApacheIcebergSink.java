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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.data.Record;
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.operators.ApacheIcebergSink;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;

/**
 * {@link Operator} for the {@link JavaPlatform} that creates an Iceberg Table.
 * If the table does not exists it will create a new, otherwise it will append
 * to a table
 * {@code R} Is the input type of the incoming Data Stream. Must be a
 * {@link org.apache.wayang.basic.data.Record} (or subclass).
 */

public class JavaApacheIcebergSink extends ApacheIcebergSink
        implements JavaExecutionOperator {

    private final int defaultPartitionId = 1;
    private final int defaultTaskId = 1;

    /**
     * Creates a new sink for the Java Platform.
     *
     * @param catalog         Iceberg catalog used to resolve the target table; must
     *                        not be {@code null}
     * @param schema          Iceberg write schema; must be compatible with the
     *                        target table
     * @param tableIdentifier fully qualified identifier of the target table
     * 
     * @param outputFileFormat file format used for writing (e.g., Parquet, Avro)
     */
    public JavaApacheIcebergSink(
        Catalog catalog, 
        Schema schema, 
        TableIdentifier tableIdentifier, 
        FileFormat outputFileFormat) {
        super(catalog, schema, tableIdentifier, outputFileFormat);
    }


    /**
     * Copy constructor.
     *
     * @param that sink instance to copy
     */

    public JavaApacheIcebergSink(ApacheIcebergSink that) {
        super(that);
    }



    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        try {

            assert inputs.length == 1;
            assert outputs.length == 2;

            FileFormat outputFileFormat = getOutputFileFormat();

            JavaChannelInstance input = (JavaChannelInstance) inputs[0];

            if (!tableExists()) {
                catalog.createTable(tableIdentifier, schema);
            }

            Stream<org.apache.iceberg.data.Record> inputStream = input
                    .provideStream()
                    .peek(r -> {
                        if (!(r instanceof org.apache.wayang.basic.data.Record)) {
                           throw new WayangException("Expected Wayang Record but got " + r.getClass());
                        }})
                    .map(r ->  wayangRecordToIcebergRecord((org.apache.wayang.basic.data.Record) r));

            Table table = catalog.loadTable(tableIdentifier);
            OutputFileFactory outputFileFactory = OutputFileFactory
                    .builderFor(table, this.defaultPartitionId, this.defaultTaskId)
                    .format(outputFileFormat)
                    .build();

            EncryptedOutputFile outputFile = outputFileFactory.newOutputFile();

            FileAppenderFactory<org.apache.iceberg.data.Record> appenderFactory = new org.apache.iceberg.data.GenericAppenderFactory(
                    this.schema);

            try (DataWriter<org.apache.iceberg.data.Record> writer = appenderFactory.newDataWriter(outputFile,
                    outputFileFormat, /* Partition */null)) {

                inputStream.forEach(dataQuanta -> {
                    writer.write(dataQuanta);
                });

            }

        } catch (Exception e) {
            throw new WayangException("Coult not write stream to iceberg location.", e);
        }

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);

    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaApacheIcebergSink(this.catalog, this.schema, this.tableIdentifier, getOutputFileFormat());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.apacheicebergsink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException();
    }

    private org.apache.iceberg.data.Record wayangRecordToIcebergRecord(org.apache.wayang.basic.data.Record wayangRecord) {
        GenericRecord template = GenericRecord.create(this.schema);
        Record out = template.copy();

        int n = this.schema.columns().size();
        for (int i = 0; i < n; i++) {
            Object v = wayangRecord.getField(i);
            out.set(i, v);
        }
        return out;
    };

    private boolean tableExists() {
        return catalog.tableExists(tableIdentifier);
    }

}
