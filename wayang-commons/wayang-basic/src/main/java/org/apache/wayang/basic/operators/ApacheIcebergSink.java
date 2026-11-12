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
package org.apache.wayang.basic.operators;

import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.types.DataSetType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.FileFormat;
import org.apache.wayang.basic.data.Record;

/**
 * This {@link UnarySink} writes all incoming {@link Record} data quanta to an iceberg table.
 * Either if the table does not exists it will create new, otherwise append.
 */
public class ApacheIcebergSink extends UnarySink<org.apache.wayang.basic.data.Record> {

    protected final Catalog catalog;
    protected final Schema schema;
    protected final TableIdentifier tableIdentifier;
    protected final FileFormat outputFileFormat;

    /**
     *
     * @param catalog         Iceberg catalog used to resolve the target table; must
     *                        not be {@code null}
     * @param schema          Iceberg write schema; must be compatible with the
     *                        target table
     * @param tableIdentifier fully qualified identifier of the target table
     * 
     * @param outputFileFormat {@link FileFormat} the format of the output data files
     */
    public ApacheIcebergSink(Catalog catalog, Schema schema, TableIdentifier tableIdentifier, FileFormat outputFileFormat) {
        super(DataSetType.createDefault(Record.class));
        this.catalog = catalog;
        this.schema = schema;
        this.tableIdentifier = tableIdentifier;
        this.outputFileFormat = outputFileFormat;
    }

    /**
     * Creates a copied instance.
     *
     * @param that should be copied
     */
    public ApacheIcebergSink(ApacheIcebergSink that) {
        super(that);
        this.catalog = that.catalog;
        this.schema = that.schema;
        this.tableIdentifier = that.tableIdentifier;
        this.outputFileFormat = that.outputFileFormat;
    }

    public FileFormat getOutputFileFormat() {
        return this.outputFileFormat;
    }
}
