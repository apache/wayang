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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;

/**
 * {@link UnarySource} that provides the tuples from a database table.
 */
public class TableSource extends UnarySource<Record> {

    private final String tableName;

    /**
     * Columns this source reads. Empty when the caller did not name any columns.
     */
    private final String[] columnNames;

    public String getTableName() {
        return tableName;
    }

    /**
     * @return a copy of the column names, or an empty array when none were given
     */
    public String[] getColumnNames() {
        if (this.columnNames == null) {
            return new String[0];
        }
        return this.columnNames.clone();
    }

    /**
     * Creates a new instance.
     *
     * @param tableName   name of the table to be read
     * @param columnNames names of the columns to read; can be omitted. When they are plain SQL identifiers,
     *                    JDBC scans select these columns instead of {@code *}
     */
    public TableSource(String tableName, String... columnNames) {
        this(tableName, createOutputDataSetType(columnNames));
    }

    public TableSource(String tableName, DataSetType<Record> type) {
        super(type);
        this.tableName = tableName;
        this.columnNames = columnNamesFrom(type);
    }

    private static String[] columnNamesFrom(DataSetType<Record> type) {
        if (type == null) {
            return new String[0];
        }
        final DataUnitType<Record> dataUnitType = type.getDataUnitType();
        if (!(dataUnitType instanceof RecordType)) {
            return new String[0];
        }
        final String[] names = ((RecordType) dataUnitType).getFieldNames();
        if (names == null || names.length == 0) {
            return new String[0];
        }
        return names.clone();
    }

    /**
     * Constructs an appropriate output {@link DataSetType} for the given column names.
     *
     * @param columnNames the column names or an empty array if unknown
     * @return the output {@link DataSetType}, which will be based upon a {@link RecordType} unless no {@code columnNames}
     * is empty
     */
    private static DataSetType<Record> createOutputDataSetType(String[] columnNames) {
        return columnNames.length == 0 ?
                DataSetType.createDefault(Record.class) :
                DataSetType.createDefault(new RecordType(columnNames));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public TableSource(TableSource that) {
        super(that);
        this.tableName = that.getTableName();
        this.columnNames = that.getColumnNames();
    }

}
