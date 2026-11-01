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

package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * PostgreSQL implementation for the {@link TableSource}.
 */
public abstract class JdbcTableSource extends TableSource implements JdbcSourceOperator {

    /**
     * Creates a new instance.
     *
     * @see TableSource#TableSource(String, String...)
     */
    public JdbcTableSource(String tableName, String... columnNames) {
        super(tableName, columnNames);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcTableSource(JdbcTableSource that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return this.getTableName();
    }

    @Override
    public String getSourceName() {
        return this.getTableName();
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.tablesource.load", this.getPlatform().getPlatformId());
    }

    @Override
    public CardinalityEstimator getCardinalityEstimator(int outputIndex) {
        assert outputIndex == 0;
        return new CardinalityEstimator() {
            @Override
            public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
                // see Job for StopWatch measurements
                final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                        "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
                );

                // Establish a DB connection.
                try (Connection connection = JdbcTableSource.this.getPlatform()
                        .createDatabaseDescriptor(optimizationContext.getConfiguration())
                        .createJdbcConnection()) {

                    // Query the table cardinality.
                    // No trailing ';': strict parsers (Trino, BigQuery) reject it in executeQuery.
                    final String sql = String.format("SELECT count(*) FROM %s", JdbcTableSource.this.getTableName());
                    final ResultSet resultSet = connection.createStatement().executeQuery(sql);
                    if (!resultSet.next()) {
                        throw new SQLException("No query result for \"" + sql + "\".");
                    }
                    long cardinality = resultSet.getLong(1);
                    return new CardinalityEstimate(cardinality, cardinality, 1d);

                } catch (Exception e) {
                    LogManager.getLogger(this.getClass()).error(
                            "Could not estimate cardinality for {}.", JdbcTableSource.this, e
                    );

                    // If we could not load the cardinality, let's use a very conservative estimate.
                    return new CardinalityEstimate(10, 10000000, 0.9);
                } finally {
                    timeMeasurement.stop();
                }
            }
        };
    }
}
