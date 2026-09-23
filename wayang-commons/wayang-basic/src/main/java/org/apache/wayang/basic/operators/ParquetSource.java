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

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.fs.FileSystems;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * This source reads a parquet file and outputs the lines as {@link Record} units.
 */
public class ParquetSource extends UnarySource<Record> {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final String inputUrl;

    private final String[] projection;

    private ParquetMetadata metadata;

    private MessageType schema;

    private boolean preferDatasetOutput = false;

    /**
     * Creates a new instance.
     *
     * @param inputUrl   name of the file to be read
     * @param projection names of the columns to filter; can be omitted but allows for an early projection
     */
    public static ParquetSource create(String inputUrl, String[] projection) {
        ParquetMetadata metadata = readMetadata(inputUrl);
        MessageType schema = metadata.getFileMetaData().getSchema();

        String[] columnNames = schema.getFields().stream()
                .map(Type::getName)
                .toArray(String[]::new);

        ParquetSource instance = new ParquetSource(inputUrl, projection, createOutputDataSetType(columnNames));

        instance.metadata = metadata;
        instance.schema = schema;

        return instance;
    }

    public ParquetSource(String inputUrl, String[] projection, String... fieldNames) {
        this(inputUrl, projection, createOutputDataSetType(fieldNames));
    }

    public ParquetSource(String inputUrl, String[] projection, DataSetType<Record> type) {
        super(type);
        this.inputUrl = inputUrl;
        this.projection = projection;
    }

    private static ParquetMetadata readMetadata(String inputUrl) {
        Path path = new Path(inputUrl);

        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new org.apache.hadoop.conf.Configuration()))) {
            return reader.getFooter();
        } catch (Exception e) {
            throw new WayangException("Could not read metadata.", e);
        }
    }

    public String getInputUrl() { return this.inputUrl; }

    public String[] getProjection() { return this.projection; }

    public ParquetMetadata getMetadata() { return this.metadata; }

    public MessageType getSchema() { return this.schema; }

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
    public ParquetSource(ParquetSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
        this.projection = that.getProjection();
        this.metadata = that.getMetadata();
        this.schema = that.getSchema();
        this.preferDatasetOutput = that.preferDatasetOutput;
    }

    public ParquetSource preferDatasetOutput(boolean preferDataset) {
        this.preferDatasetOutput = preferDataset;
        return this;
    }

    public boolean isDatasetOutputPreferred() {
        return this.preferDatasetOutput;
    }

    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new ParquetSource.CardinalityEstimator());
    }

    /**
     * Custom {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    protected class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(ParquetSource.this.getNumInputs() == inputEstimates.length);

            // see Job for StopWatch measurements
            final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                    "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
            );

            // Query the job cache first to see if there is already an estimate.
            String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(), ParquetSource.this.inputUrl);
            CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey, CardinalityEstimate.class);
            if (cardinalityEstimate != null) return cardinalityEstimate;

            if (ParquetSource.this.metadata == null) {
                ParquetSource.this.logger.warn("Could not inspect metadata of {}... deliver fallback estimate.",
                        ParquetSource.this.inputUrl);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }

            // Otherwise calculate the cardinality.
            // First, inspect the size of the file and its line sizes.
            OptionalLong fileSize;
            try {
                fileSize = FileSystems.getFileSize(ParquetSource.this.inputUrl);
            } catch (Exception e) {
                ParquetSource.this.logger.warn(
                        "Could not determine size of {} ({}: {})... deliver fallback estimate.",
                        ParquetSource.this.inputUrl,
                        e.getClass().getSimpleName(),
                        e.getMessage()
                );
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }
            if (fileSize.isEmpty()) {
                ParquetSource.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                        ParquetSource.this.inputUrl);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;

            } else if (fileSize.getAsLong() == 0L) {
                timeMeasurement.stop();
                return new CardinalityEstimate(0L, 0L, 1d);
            }

            OptionalLong numberRows = this.extractNumberRows();
            if (numberRows.isEmpty()) {
                ParquetSource.this.logger.warn("Could not determine the cardinality of {}... deliver fallback estimate.",
                        ParquetSource.this.inputUrl);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }

            // Create an exact cardinality estimate for the complete file.
            long rowCount = numberRows.getAsLong();
            cardinalityEstimate = new CardinalityEstimate(rowCount, rowCount, 1d);

            // Cache the result, so that it will not be recalculated again.
            optimizationContext.putIntoJobCache(jobCacheKey, cardinalityEstimate);

            timeMeasurement.stop();
            return cardinalityEstimate;
        }

        /**
         * Extract the number of rows in the file
         *
         * @return the number of rows in the file
         */
        private OptionalLong extractNumberRows() {
            long rowCount = ParquetSource.this.metadata.getBlocks().stream()
                    .mapToLong(BlockMetaData::getRowCount)
                    .sum();

            if (rowCount == 0) {
                ParquetSource.this.logger.warn("Could not find any row in {}.", ParquetSource.this.inputUrl);
                return OptionalLong.empty();
            }
            return OptionalLong.of(rowCount);
        }
    }

}
