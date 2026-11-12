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

package org.apache.wayang.genericjdbc.operators;

import org.apache.logging.log4j.LogManager;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.genericjdbc.platform.GenericJdbcPlatform;
import org.apache.wayang.jdbc.operators.JdbcTableSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class GenericJdbcTableSource extends JdbcTableSource implements GenericJdbcExecutionOperator {

    /**
     * Name of the JDBC configuration to use.
     */
    public String jdbcName;

    /**
     * Creates a new instance.
     *
     * @param tableName   the table to read from
     * @param columnNames the columns to read
     */
    public GenericJdbcTableSource(String tableName, String... columnNames) {
        super(tableName, columnNames);
        this.jdbcName = "genericjdbc";
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that the instance that should be copied
     */
    public GenericJdbcTableSource(GenericJdbcTableSource that) {
        super(that);
        this.jdbcName = that.jdbcName;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no input channels.");
    }

    @Override
    public CardinalityEstimator getCardinalityEstimator(int outputIndex) {
        assert outputIndex == 0;
        return new CardinalityEstimator() {
            @Override
            public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {

                final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                        "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
                );

                try (Connection connection = GenericJdbcPlatform.getInstance()
                        .createDatabaseDescriptor(optimizationContext.getConfiguration(), jdbcName)
                        .createJdbcConnection()) {

                    final String sql = String.format(
                            "SELECT count(*) FROM %s;", GenericJdbcTableSource.this.getTableName()
                    );

                    final ResultSet resultSet = connection.createStatement().executeQuery(sql);

                    if (!resultSet.next()) {
                        throw new SQLException("No query result for \"" + sql + "\".");
                    }
                    long cardinality = resultSet.getLong(1);
                    return new CardinalityEstimate(cardinality, cardinality, 1d);

                } catch (Exception e) {

                    LogManager.getLogger(this.getClass()).error(
                            "Could not estimate cardinality for {}.", GenericJdbcTableSource.this, e
                    );

                    return new CardinalityEstimate(10, 10000000, 0.9);

                } finally {
                    timeMeasurement.stop();
                }
            }
        };
    }
}