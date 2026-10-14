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
import org.apache.wayang.basic.util.SqlIdentifierUtils;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.types.DataSetType;

import java.util.Properties;

/**
 * {@link UnarySink} that writes Records to a database table.
 */

public class TableSink<T> extends UnarySink<T> {
    private final String tableName;

    private String[] columnNames;

    private final Properties props;

    private String mode;

    /**
     * Creates a new instance.
     *
     * @param props       database connection properties
     * @param mode        write mode
     * @param tableName   name of the table to be written
     * @param columnNames names of the columns in the tables
     */
    public TableSink(Properties props, String mode, String tableName, String... columnNames) {
        this(props, mode, tableName, columnNames, (DataSetType<T>) DataSetType.createDefault(Record.class));
    }

    public TableSink(Properties props, String mode, String tableName, String[] columnNames, DataSetType<T> type) {
        super(type);
        SqlIdentifierUtils.validateTableName(tableName);
        SqlIdentifierUtils.validateColumnNames(columnNames);
        this.tableName = tableName;
        this.columnNames = columnNames == null ? null : java.util.Arrays.copyOf(columnNames, columnNames.length);
        this.props = new Properties();
        if (props != null) {
            this.props.putAll(props);
        }
        this.mode = mode;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public TableSink(TableSink<T> that) {
        super(that);
        this.tableName = that.getTableName();
        this.columnNames = that.getColumnNames();
        this.props = that.getProperties();
        this.mode = that.getMode();
    }

    public String getTableName() {
        return this.tableName;
    }

    protected void setColumnNames(String[] columnNames) {
        SqlIdentifierUtils.validateColumnNames(columnNames);
        this.columnNames = columnNames == null ? null : java.util.Arrays.copyOf(columnNames, columnNames.length);
    }

    public String[] getColumnNames() {
        return this.columnNames == null ? null : java.util.Arrays.copyOf(this.columnNames, this.columnNames.length);
    }

    public Properties getProperties() {
        Properties copy = new Properties();
        copy.putAll(this.props);
        return copy;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}
