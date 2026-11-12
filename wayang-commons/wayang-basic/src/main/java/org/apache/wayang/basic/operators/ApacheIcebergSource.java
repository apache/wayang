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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.IcebergGenerics.ScanBuilder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.commons.lang3.Validate;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;


/**
 * This source reads an Iceberg Table and outputs the lines as
 * {@link org.apache.wayang.basic.data.Record}
 * units.
 */
public class ApacheIcebergSource extends UnarySource<Record> {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final Catalog catalog;

    private final TableIdentifier tableIdentifier;

    private Collection<Expression> whereExpressions;
    private Collection<String> columns;

    private org.apache.iceberg.Table cachedTable = null;

    private static final Double defaultSelectivityValue = 0.10;

    /**
     * Creates a new Iceberg source instance.
     *
     * @param catalog          Iceberg catalog used to load the table
     * @param tableIdentifier  identifier of the target table
     * @param whereExpressions list of
     *                         {@link org.apache.iceberg.expressions.Expression}
     *                         filters; empty list for none
     * @param columns          collection of column names to project; empty list for
     *                         all columns
     * @return a new {@link ApacheIcebergSource} instance
     */
    public static ApacheIcebergSource create(Catalog catalog, TableIdentifier tableIdentifier,
            Expression[] whereExpressions,
            String[] columns) {

            List<Expression> whereList =
            (whereExpressions == null) ? Collections.emptyList() : Arrays.asList(whereExpressions);

            List<String> columnList =
                 (columns == null) ? Collections.emptyList() : Arrays.asList(columns);

        return new ApacheIcebergSource(catalog, tableIdentifier, whereList, columnList);
    }

    public ApacheIcebergSource(Catalog catalog, TableIdentifier tableIdentifier,
            Collection<Expression> whereExpressions, Collection<String> columns) {
        
        
        super(createOutputDataSetType(columns));
        this.catalog = catalog;
        this.tableIdentifier = tableIdentifier;

        this.whereExpressions = whereExpressions;
        this.columns = columns;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public ApacheIcebergSource(ApacheIcebergSource that) {
        super(that);
        this.catalog = that.getCatalog();
        this.columns = that.getColumns();
        this.tableIdentifier = that.getTableIdentifier();
        this.whereExpressions = that.getWhereExpressions();
    }

    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new ApacheIcebergSource.CardinalityEstimator());
    }

    protected class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext,
                CardinalityEstimate... inputEstimates) {
            Validate.isTrue(ApacheIcebergSource.this.getNumInputs() == inputEstimates.length);

            // see Job for StopWatch measurements
            final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                    "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities");

            // Query the job cache first to see if there is already an estimate.
            String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(),
                    ApacheIcebergSource.this.getIcebergTableName());

            CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey,
                    CardinalityEstimate.class);
            if (cardinalityEstimate != null)
                return cardinalityEstimate;

            // Otherwise calculate the cardinality.
            // First, inspect the size of the file and its line sizes.
            OptionalLong fileSize = getFileSize();
            if (fileSize.isEmpty()) {
                ApacheIcebergSource.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                        ApacheIcebergSource.this.getIcebergTableName());
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }
            if (fileSize.getAsLong() == 0L) {
                timeMeasurement.stop();
                return new CardinalityEstimate(0L, 0L, 1d);
            }

            OptionalLong numberRows = ApacheIcebergSource.this.ExtractNumberOfRows();

            if (numberRows.isEmpty()) {
                ApacheIcebergSource.this.logger
                        .warn("Could not determine the cardinality of {}... deliver fallback estimate.",
                                ApacheIcebergSource.this.getIcebergTableName());
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }

            long rowCount = numberRows.getAsLong();
            cardinalityEstimate = new CardinalityEstimate(rowCount, rowCount, 1d);

            // Cache the result, so that it will not be recalculated again.
            optimizationContext.putIntoJobCache(jobCacheKey, cardinalityEstimate);

            timeMeasurement.stop();
            return cardinalityEstimate;
        }
    }

    /**
     * Creates a {@link DataSetType} for the output records based on the given
     * column names.
     *
     * @param columnNames collection of column names to include; empty for default
     *                    record type
     * @return a {@link DataSetType} describing the output record structure
     */

    private static DataSetType<Record> createOutputDataSetType(Collection<String> columnNames) {
        if (columnNames == null) {
            columnNames = new ArrayList<String>();
        }
        String[] columnNamesAsArray = columnNames.toArray(new String[0]);
        return columnNamesAsArray.length == 0 ? DataSetType.createDefault(Record.class)
                : DataSetType.createDefault(new RecordType(columnNamesAsArray));
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public TableIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public Collection<Expression> getWhereExpressions() {
        return whereExpressions;
    }

    public Collection<String> getColumns() {
        return columns;
    }

    private void setCachedTable(Table table) {
        this.cachedTable = table;
    }

    /**
     * Returns the Iceberg table name.
     *
     * @return the table name from the
     *         {@link org.apache.iceberg.catalog.TableIdentifier}
     */
    public String getIcebergTableName() {
        return tableIdentifier.name();
    }

    /**
     * Loads and returns the Iceberg {@link org.apache.iceberg.Table}.
     * Uses a cached instance if available.
     *
     * @return the loaded Iceberg table
     */
    private Table getTable() {
        if (this.cachedTable != null) {
            return this.cachedTable;
        }

        Table table = this.catalog.loadTable(this.tableIdentifier);
        setCachedTable(table);
        return table;
    }

    /**
     * Builds a {@link ScanBuilder} for the current table.
     * Applies selected columns and filter expressions if provided.
     *
     * @return configured {@link ScanBuilder}
     */
    public ScanBuilder getScanBuilder() {

        ScanBuilder scanBuilder = IcebergGenerics.read(getTable());

        if (this.columns != null && this.columns.size() > 0) {
            scanBuilder = scanBuilder.select(columns);
        }

        if (this.whereExpressions != null && this.whereExpressions.size() > 0) {
            for (Expression whereExpression : this.whereExpressions) {
                scanBuilder = scanBuilder.where(whereExpression);
            }
        }

        return scanBuilder;
    }

    /**
     * Estimates the number of rows in the table.
     * Applies a selectivity adjustment if filter expressions are present.
     *
     * @return estimated number of rows, or {@link OptionalLong#empty()} if
     *         unavailable
     */
    private OptionalLong ExtractNumberOfRows() {
        try {
            long rowCount = 0L;

            Table table = getTable();

            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask fileScanTask : tasks) {
                    rowCount += fileScanTask.estimatedRowsCount();
                }
            }

            if (rowCount == 0) {
                return OptionalLong.empty();
            }

            if (this.whereExpressions != null && this.whereExpressions.size() > 0) {

                Double updatedRowCount = rowCount * Math.pow(defaultSelectivityValue, this.whereExpressions.size());
                return OptionalLong.of(updatedRowCount.longValue());
            }

            return OptionalLong.of(rowCount);

        } catch (Exception e) {
            this.logger.warn("Could not extract the number of rows. Returning empty. Got erro:  " + e);
            return OptionalLong.empty();
        }

    }

    /**
     * Calculates the total file size in bytes of all table files.
     *
     * @return total file size in bytes, or {@link OptionalLong#empty()} if
     *         unavailable
     */
    private OptionalLong getFileSize() {

        try {
            long fileSizeCount = 0L;
            try (CloseableIterable<FileScanTask> tasks = getTable().newScan().planFiles()) {
                for (FileScanTask t : tasks) {
                    fileSizeCount += t.file().fileSizeInBytes();
                }
            }
            return OptionalLong.of(fileSizeCount);

        } catch (Exception e) {
            this.logger.warn("Could not get file size. Returning empty. Got error:  " + e);
            return OptionalLong.empty();

        }

    }

}
