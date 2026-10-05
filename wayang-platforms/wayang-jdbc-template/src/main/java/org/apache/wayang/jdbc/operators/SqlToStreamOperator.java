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
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
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

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
public class SqlToStreamOperator<Type> extends UnaryToUnaryOperator<Type, Type> implements JavaExecutionOperator, JsonSerializable {

    private final JdbcPlatformTemplate jdbcPlatform;

    /**
     * Creates a new instance.
     *
     * @param jdbcPlatform from which the SQL data comes
     */
    @SuppressWarnings("unchecked")
    public SqlToStreamOperator(JdbcPlatformTemplate jdbcPlatform) {
        this(jdbcPlatform, (DataSetType<Type>) DataSetType.createDefault(Record.class));
    }

    /**
     * Creates a new instance.
     *
     * @param jdbcPlatform from which the SQL data comes
     * @param dataSetType  type of the data passed through the channel (e.g. {@link Record} or {@link Tuple2})
     */
    @SuppressWarnings("unchecked")
    public SqlToStreamOperator(JdbcPlatformTemplate jdbcPlatform, DataSetType<?> dataSetType) {
        super((DataSetType<Type>) dataSetType, (DataSetType<Type>) dataSetType, false);
        this.jdbcPlatform = jdbcPlatform;
    }

    @SuppressWarnings("unchecked")
    public void adaptType(DataSetType<?> newType) {
        if (newType != null) {
            this.inputSlots[0] = new org.apache.wayang.core.plan.wayangplan.InputSlot<>("in", this, (DataSetType<Type>) newType);
            this.outputSlots[0] = new org.apache.wayang.core.plan.wayangplan.OutputSlot<>("out", this, (DataSetType<Type>) newType);
        }
    }

    protected SqlToStreamOperator(SqlToStreamOperator<Type> that) {
        super(that);
        this.jdbcPlatform = that.jdbcPlatform;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor executor,
            OptimizationContext.OperatorContext operatorContext) {
        // Cast the inputs and outputs.
        final SqlQueryChannel.Instance input = (SqlQueryChannel.Instance) inputs[0];
        final StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        JdbcPlatformTemplate producerPlatform = (JdbcPlatformTemplate) input.getChannel().getProducer().getPlatform();
        final Connection connection = producerPlatform
                .createDatabaseDescriptor(executor.getConfiguration())
                .createJdbcConnection();

        boolean isTuple = false;
        if (this.getOutputType() != null && Tuple2.class.isAssignableFrom(this.getOutputType().getDataUnitType().getTypeClass())) {
            isTuple = true;
        } else if (input.getChannel() != null) {
            if (input.getChannel().getProducerSlot() != null &&
                    Tuple2.class.isAssignableFrom(input.getChannel().getProducerSlot().getType().getDataUnitType().getTypeClass())) {
                isTuple = true;
            } else if (input.getChannel().getProducer() != null &&
                    input.getChannel().getProducer().getOperator() instanceof JoinOperator) {
                isTuple = true;
            }
        }

        Iterator<?> resultSetIterator;
        if (isTuple) {
            int leftColumnCount = resolveLeftColumnCount(connection, input);
            resultSetIterator = new Tuple2ResultSetIterator(connection, input.getSqlQuery(), leftColumnCount);
        } else {
            resultSetIterator = new ResultSetIterator(connection, input.getSqlQuery());
        }

        Spliterator<?> resultSetSpliterator = Spliterators.spliteratorUnknownSize(resultSetIterator, 0);
        Stream<?> resultSetStream = StreamSupport.stream(resultSetSpliterator, false);

        output.accept((Stream) resultSetStream);

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

        return queryLineageNode.collectAndMark();
    }

    public static int resolveLeftColumnCount(Connection connection, SqlQueryChannel.Instance input) {
        try {
            if (input.getChannel() != null && input.getChannel().getProducer() != null) {
                Operator op = input.getChannel().getProducer().getOperator();
                if (op instanceof JoinOperator) {
                    JoinOperator<?, ?, ?> joinOp = (JoinOperator<?, ?, ?>) op;
                    DataSetType<?> in0Type = joinOp.getInput(0).getType();
                    if (in0Type != null && in0Type.getDataUnitType() instanceof RecordType) {
                        int count = ((RecordType) in0Type.getDataUnitType()).getFieldNames().length;
                        if (count > 0) return count;
                    }
                    if (joinOp.getKeyDescriptor0() != null && joinOp.getKeyDescriptor0().getSqlImplementation() != null) {
                        String leftTable = joinOp.getKeyDescriptor0().getSqlImplementation().getField0();
                        if (leftTable != null && !leftTable.isEmpty()) {
                            int count = getTableColumnCount(connection, leftTable);
                            if (count > 0) return count;
                        }
                    }
                }
                ExecutionTask producerTask = input.getChannel().getProducer();
                if (producerTask.getNumInputChannels() > 0 && producerTask.getInputChannel(0) != null) {
                    Channel leftInChannel = producerTask.getInputChannel(0);
                    if (leftInChannel.getProducerSlot() != null && leftInChannel.getProducerSlot().getType() != null) {
                        if (leftInChannel.getProducerSlot().getType().getDataUnitType() instanceof RecordType) {
                            int count = ((RecordType) leftInChannel.getProducerSlot().getType().getDataUnitType()).getFieldNames().length;
                            if (count > 0) return count;
                        }
                    }
                    if (leftInChannel.getProducer() != null && leftInChannel.getProducer().getOperator() != null) {
                        Operator leftProducerOp = leftInChannel.getProducer().getOperator();
                        try {
                            java.lang.reflect.Method getTableNameMethod = leftProducerOp.getClass().getMethod("getTableName");
                            String tableName = (String) getTableNameMethod.invoke(leftProducerOp);
                            if (tableName != null && !tableName.isEmpty()) {
                                int count = getTableColumnCount(connection, tableName);
                                if (count > 0) return count;
                            }
                        } catch (Exception ignored) {}
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return -1;
    }

    public static int getTableColumnCount(Connection connection, String tableName) {
        if (connection == null || tableName == null) return -1;
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            int count = 0;
            try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
                while (rs.next()) count++;
            }
            if (count > 0) return count;

            try (ResultSet rs = metaData.getColumns(null, null, tableName.toUpperCase(), null)) {
                while (rs.next()) count++;
            }
            if (count > 0) return count;

            try (ResultSet rs = metaData.getColumns(null, null, tableName.toLowerCase(), null)) {
                while (rs.next()) count++;
            }
            return count;
        } catch (SQLException e) {
            return -1;
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
         * @param connection the JDBC connection on which to execute a SQL query
         * @param sqlQuery   the SQL query
         */
        ResultSetIterator(Connection connection, String sqlQuery) {
            try {
                //connection.setAutoCommit(false);
                Statement st = connection.createStatement();
                //st.setFetchSize(100000000);
                this.resultSet = st.executeQuery(sqlQuery);
            } catch (SQLException e) {
                this.close();
                throw new WayangException("Could not execute SQL.", e);
            }
            this.moveToNext();
        }

        /**
         * Moves this instance to the next {@link Record}.
         */
        private void moveToNext() {
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
            } catch (SQLException e) {
                this.next = null;
                this.close();
                throw new WayangException("Exception while iterating the result set.", e);
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
            if (this.resultSet != null) {
                try {
                    Statement st = this.resultSet.getStatement();
                    this.resultSet.close();
                    if (st != null) {
                        st.close();
                    }
                } catch (Throwable t) {
                    LogManager.getLogger(this.getClass()).error("Could not close result set.", t);
                } finally {
                    this.resultSet = null;
                }
            }
        }
    }

    /**
     * Exposes a {@link ResultSet} as an {@link Iterator} of {@link Tuple2} containing two {@link Record}s.
     */
    public static class Tuple2ResultSetIterator implements Iterator<Tuple2<Record, Record>>, AutoCloseable {

        private ResultSet resultSet;
        private Tuple2<Record, Record> next;
        private int leftColumnCount;

        public Tuple2ResultSetIterator(Connection connection, String sqlQuery, int leftColumnCount) {
            this.leftColumnCount = leftColumnCount;
            try {
                Statement st = connection.createStatement();
                this.resultSet = st.executeQuery(sqlQuery);
            } catch (SQLException e) {
                this.close();
                throw new WayangException("Could not execute SQL.", e);
            }
            this.moveToNext();
        }

        private void moveToNext() {
            try {
                if (this.resultSet == null || !this.resultSet.next()) {
                    this.next = null;
                    this.close();
                } else {
                    ResultSetMetaData metaData = this.resultSet.getMetaData();
                    final int totalColumnCount = metaData.getColumnCount();
                    if (this.leftColumnCount <= 0 || this.leftColumnCount >= totalColumnCount) {
                        int detectedCount = 0;
                        String firstTable = null;
                        try {
                            firstTable = metaData.getTableName(1);
                        } catch (Exception ignored) {}
                        if (firstTable != null && !firstTable.isEmpty()) {
                            for (int i = 1; i <= totalColumnCount; i++) {
                                if (firstTable.equalsIgnoreCase(metaData.getTableName(i))) {
                                    detectedCount++;
                                } else {
                                    break;
                                }
                            }
                        }
                        if (detectedCount > 0 && detectedCount < totalColumnCount) {
                            this.leftColumnCount = detectedCount;
                        } else {
                            this.leftColumnCount = totalColumnCount / 2;
                        }
                    }

                    int rightColumnCount = totalColumnCount - this.leftColumnCount;
                    Object[] leftValues = new Object[this.leftColumnCount];
                    for (int i = 0; i < this.leftColumnCount; i++) {
                        leftValues[i] = this.resultSet.getObject(i + 1);
                    }
                    Object[] rightValues = new Object[rightColumnCount];
                    for (int i = 0; i < rightColumnCount; i++) {
                        rightValues[i] = this.resultSet.getObject(this.leftColumnCount + i + 1);
                    }
                    this.next = new Tuple2<>(new Record(leftValues), new Record(rightValues));
                }
            } catch (SQLException e) {
                this.next = null;
                this.close();
                throw new WayangException("Exception while iterating the result set.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return this.next != null;
        }

        @Override
        public Tuple2<Record, Record> next() {
            Tuple2<Record, Record> curNext = this.next;
            this.moveToNext();
            return curNext;
        }

        @Override
        public void close() {
            if (this.resultSet != null) {
                try {
                    Statement st = this.resultSet.getStatement();
                    this.resultSet.close();
                    if (st != null) {
                        st.close();
                    }
                } catch (Throwable t) {
                    LogManager.getLogger(this.getClass()).error("Could not close result set.", t);
                } finally {
                    this.resultSet = null;
                }
            }
        }
    }

    @Override
    public WayangJsonObj toJson() {
        return new WayangJsonObj().put("platform", this.jdbcPlatform.getClass().getCanonicalName());
    }

    @SuppressWarnings("rawtypes")
    public static SqlToStreamOperator fromJson(WayangJsonObj wayangJsonObj) {
        final String platformClassName = wayangJsonObj.getString("platform");
        JdbcPlatformTemplate jdbcPlatform = ReflectionUtils.evaluate(platformClassName + ".getInstance()");
        return new SqlToStreamOperator(jdbcPlatform);
    }
}
