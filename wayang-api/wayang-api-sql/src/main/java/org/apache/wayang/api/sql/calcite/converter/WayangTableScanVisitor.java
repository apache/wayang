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

package org.apache.wayang.api.sql.calcite.converter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.apache.wayang.api.sql.calcite.rel.WayangTableScan;
import org.apache.wayang.api.sql.calcite.utils.ModelParser;
import org.apache.wayang.java.operators.JavaCSVFileSource;
import org.apache.wayang.java.operators.CsvType;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSource;

import java.util.ArrayList;
import java.util.List;

//TODO: create tablesource with column types
//TODO: support other sources
public class WayangTableScanVisitor extends WayangRelNodeVisitor<WayangTableScan> {
    WayangTableScanVisitor(final WayangRelConverter wayangRelConverter) {
        super(wayangRelConverter);
    }

    @Override
    Operator visit(final WayangTableScan wayangRelNode) {
        final String tableName = wayangRelNode.getTableName();
        final List<String> columnNames = wayangRelNode.getColumnNames();

        // Get the source platform for this table
        final String tableSource = wayangRelNode.getTable().getQualifiedName().get(0);

        if (tableSource.equals("postgres")) {
            return new PostgresTableSource(tableName, columnNames.toArray(String[]::new));
        } else if (tableSource.equals("fs")) {
            final ModelParser modelParser;
            try {
                modelParser = this.wayangRelConverter.getConfiguration() == null
                        ? new ModelParser()
                        : new ModelParser(this.wayangRelConverter.getConfiguration());
            } catch (final Exception e) {
                e.printStackTrace();
                throw new IllegalArgumentException(
                        "Could not initialize calcite model parser from current Wayang configuration");
            }

            final List<RelDataTypeField> allFields = wayangRelNode.getRowType().getFieldList();
            final List<CsvType> fieldTypes = new ArrayList<>(wayangRelNode.getFields().length);
            for (int fieldIndex : wayangRelNode.getFields()) {
                fieldTypes.add(csvType(allFields.get(fieldIndex).getType()));
            }

            final String url = String.format("file:/%s/%s.csv", modelParser.getFsPath(), wayangRelNode.getTableName());

            final char separator = modelParser.getSchemaDelimiter(tableSource);

            return new JavaCSVFileSource<>(url, DataSetType.createDefault(Record.class), fieldTypes, separator);
        } else if (wayangRelNode.getTable().getQualifiedName().size() == 1) {
            // we assume that it is coming from a test environement or in memory db.

            return new JdbcTableSource(wayangRelNode.getTable().getQualifiedName().get(0), wayangRelNode.getColumnNames().toArray(String[]::new)) {

                @Override
                public JdbcPlatformTemplate getPlatform() {
                    throw new UnsupportedOperationException("Unimplemented method 'getPlatform'");
                }
            };
        } else
            throw new RuntimeException(
                    "Source not supported, got: " + tableSource + ", expected either postgres or filesystem (fs).");
    }

    private static CsvType csvType(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case BOOLEAN: return CsvType.BOOLEAN;
            case TINYINT: return CsvType.TINYINT;
            case SMALLINT: return CsvType.SMALLINT;
            case INTEGER: return CsvType.INTEGER;
            case BIGINT: return CsvType.BIGINT;
            case FLOAT: return CsvType.FLOAT;
            case DOUBLE: return CsvType.DOUBLE;
            case DECIMAL: return CsvType.decimal(type.getPrecision(), type.getScale());
            case DATE: return CsvType.DATE;
            case TIME: return CsvType.TIME;
            case TIMESTAMP: return CsvType.TIMESTAMP;
            default: return CsvType.STRING;
        }
    }
}
