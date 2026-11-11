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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.platform.ExecutionResourceTemplate;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.json.WayangJsonObj;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.logging.log4j.LogManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This {@link Operator} converts {@link SqlQueryChannel}s to {@link StreamChannel}s.
 */
public class SqlToStreamOperator extends UnaryToUnaryOperator<Record, Record> implements JavaExecutionOperator, JsonSerializable {

    private final JdbcPlatformTemplate jdbcPlatform;

    /**
     * Creates a new instance.
     *
     * @param jdbcPlatform from which the SQL data comes
     */
    public SqlToStreamOperator(JdbcPlatformTemplate jdbcPlatform) {
        this(jdbcPlatform, DataSetType.createDefault(Record.class));
    }

    /**
     * Creates a new instance.
     *
     * @param jdbcPlatform from which the SQL data comes
     * @param dataSetType  type of the {@link Record}s being transformed; see {@link RecordType}
     */
    public SqlToStreamOperator(JdbcPlatformTemplate jdbcPlatform, DataSetType<Record> dataSetType) {
        super(dataSetType, dataSetType, false);
        this.jdbcPlatform = jdbcPlatform;
    }

    protected SqlToStreamOperator(SqlToStreamOperator that) {
        super(that);
        this.jdbcPlatform = that.jdbcPlatform;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor executor,
            OptimizationContext.OperatorContext operatorContext) {
        // Cast the inputs and outputs.
        final SqlQueryChannel.Instance input = (SqlQueryChannel.Instance) inputs[0];
        final StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        JdbcPlatformTemplate producerPlatform = (JdbcPlatformTemplate) input.getChannel().getProducer().getPlatform();
        final String sqlQuery = input.getSqlQuery();
        final Connection connection = producerPlatform
                .createDatabaseDescriptor(executor.getConfiguration())
                .createJdbcConnection();

        ResultSetIterator resultSetIterator = new ResultSetIterator(connection, sqlQuery);
        boolean success = false;
        try {
            Spliterator<Record> resultSetSpliterator = Spliterators.spliteratorUnknownSize(resultSetIterator, 0);
            Stream<Record> resultSetStream = StreamSupport.stream(resultSetSpliterator, false)
                    .onClose(resultSetIterator::close);

            // Terminal operations do not close streams, and StreamChannel disposal only drops its reference.
            // Register cleanup for job shutdown: live channels can keep the JavaExecutor referenced after
            // downstream failures, so executor disposal alone is insufficient. Closing on channel disposal
            // could instead interrupt a lazy successor that still needs the JDBC resources.
            CrossPlatformExecutor crossPlatformExecutor = executor.getCrossPlatformExecutor();
            if (crossPlatformExecutor != null) {
                crossPlatformExecutor.registerGlobal(new ExecutionResourceTemplate(null) {
                    @Override
                    protected void doDispose() {
                        resultSetIterator.close();
                    }
                });
            }
            output.accept(resultSetStream);

            ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
            queryLineageNode.add(LoadProfileEstimators.createFromSpecification(
                    String.format("wayang.%s.sqltostream.load.query", this.jdbcPlatform.getPlatformId()),
                            executor.getConfiguration()
                    ));
            queryLineageNode.addPredecessor(input.getLineage());
            ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
            outputLineageNode.add(LoadProfileEstimators.createFromSpecification(
                    String.format("wayang.%s.sqltostream.load.output", this.jdbcPlatform.getPlatformId()),
                    executor.getConfiguration()
            ));
            output.getLineage().addPredecessor(outputLineageNode);

            Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> result = queryLineageNode.collectAndMark();
            success = true;
            return result;
        } finally {
            if (!success) resultSetIterator.close();
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.jdbcPlatform.getSqlQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList(
                String.format("wayang.%s.sqltostream.load.query", this.jdbcPlatform.getPlatformId()),
                String.format("wayang.%s.sqltostream.load.output", this.jdbcPlatform.getPlatformId())
        );
    }

    /**
     * Exposes a {@link ResultSet} as an {@link Iterator}.
     */
    public static class ResultSetIterator implements Iterator<Record>, AutoCloseable {

        private Connection connection;

        private Statement statement;

        /**
         * Keeps around the {@link ResultSet} of the SQL query.
         */
        private ResultSet resultSet;

        /**
         * The next {@link Record} to be delivered via {@link #next()}.
         */
        private Record next;

        /**
         * Creates a new instance.
         *
         * @param connection the JDBC connection on which to execute a SQL query; ownership is transferred
         *                   to this iterator, including when construction fails
         * @param sqlQuery   the SQL query
         */
        ResultSetIterator(Connection connection, String sqlQuery) {
            this.connection = connection;
            boolean success = false;
            try {
                //connection.setAutoCommit(false);
                Statement st = connection.createStatement();
                this.statement = st;
                //st.setFetchSize(100000000);
                this.resultSet = st.executeQuery(sqlQuery);
                this.moveToNext();
                success = true;
            } catch (SQLException e) {
                throw new WayangException("Could not execute SQL.", e);
            } finally {
                if (!success) this.close();
            }
        }

        /**
         * Moves this instance to the next {@link Record}.
         */
        private void moveToNext() {
            boolean success = false;
            try {
                if (this.resultSet == null || !this.resultSet.next()) {
                    this.next = null;
                    this.close();
                } else {
                    final int recordWidth = this.resultSet.getMetaData().getColumnCount();
                    Object[] values = new Object[recordWidth];
                    for (int i = 0; i < recordWidth; i++) {
                        values[i] = this.resultSet.getObject(i + 1);
                    }
                    this.next = new Record(values);
                }
                success = true;
            } catch (SQLException e) {
                throw new WayangException("Exception while iterating the result set.", e);
            } finally {
                if (!success) this.close();
            }
        }

        @Override
        public boolean hasNext() {
            return this.next != null;
        }

        @Override
        public Record next() {
            Record curNext = this.next;
            this.moveToNext();
            return curNext;
        }

        @Override
        public void close() {
            AutoCloseable[] resources = {this.resultSet, this.statement, this.connection};
            this.resultSet = null;
            this.statement = null;
            this.connection = null;
            this.next = null;
            for (AutoCloseable resource : resources) {
                if (resource == null) continue;
                try {
                    resource.close();
                } catch (Exception e) {
                    LogManager.getLogger(this.getClass()).error("Could not close JDBC resource.", e);
                }
            }
        }
    }

    @Override
    public WayangJsonObj toJson() {
        return new WayangJsonObj().put("platform", this.jdbcPlatform.getClass().getCanonicalName());
    }

    @SuppressWarnings("unused")
    public static SqlToStreamOperator fromJson(WayangJsonObj wayangJsonObj) {
        final String platformClassName = wayangJsonObj.getString("platform");
        JdbcPlatformTemplate jdbcPlatform = ReflectionUtils.evaluate(platformClassName + ".getInstance()");
        return new SqlToStreamOperator(jdbcPlatform);
    }
}
