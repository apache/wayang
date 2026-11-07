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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.JsonSerializable;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.json.WayangJsonObj;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.execution.DatabaseDescriptor;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SqlToRddOperator<Type> extends UnaryToUnaryOperator<Type, Type> implements SparkExecutionOperator, JsonSerializable {

    private final JdbcPlatformTemplate jdbcPlatform;

    @SuppressWarnings("unchecked")
    public SqlToRddOperator(JdbcPlatformTemplate jdbcPlatform) {
        this(jdbcPlatform, (DataSetType<Type>) DataSetType.createDefault(Record.class));
    }

    @SuppressWarnings("unchecked")
    public SqlToRddOperator(JdbcPlatformTemplate jdbcPlatform, DataSetType<?> dataSetType) {
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

    protected SqlToRddOperator(SqlToRddOperator<Type> that) {
        super(that);
        this.jdbcPlatform = that.jdbcPlatform;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.jdbcPlatform.getSqlQueryChannelDescriptor());
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor executor,
            OptimizationContext.OperatorContext operatorContext) {
        // Cast the inputs and outputs.
        final SqlQueryChannel.Instance input = (SqlQueryChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        JdbcPlatformTemplate producerPlatform = (JdbcPlatformTemplate) input.getChannel().getProducer().getPlatform();
        DatabaseDescriptor databaseDescriptor = producerPlatform.createDatabaseDescriptor(executor.getConfiguration());

        String sqlQuery = cleanQuery(input.getSqlQuery());
        String dbtable = isTableName(sqlQuery) ? sqlQuery : "(" + sqlQuery + ") as wayang_subquery";

        DataFrameReader reader = executor.ss.read()
                .format("jdbc")
                .option("url", databaseDescriptor.getJdbcUrl())
                .option("dbtable", dbtable)
                .option("driver", databaseDescriptor.getJdbcDriverClassName());

        if (databaseDescriptor.getUser() != null) {
            reader.option("user", databaseDescriptor.getUser());
        }
        if (databaseDescriptor.getPassword() != null) {
            reader.option("password", databaseDescriptor.getPassword());
        }

        // Apply optional partition properties if configured
        String partitionColumn = executor.getConfiguration().getStringProperty(
                String.format("wayang.%s.jdbc.partitionColumn", producerPlatform.getPlatformId()), null);
        if (partitionColumn != null) {
            String lowerBound = executor.getConfiguration().getStringProperty(
                    String.format("wayang.%s.jdbc.lowerBound", producerPlatform.getPlatformId()), null);
            String upperBound = executor.getConfiguration().getStringProperty(
                    String.format("wayang.%s.jdbc.upperBound", producerPlatform.getPlatformId()), null);
            if (lowerBound == null || upperBound == null) {
                throw new IllegalArgumentException(
                        "JDBC partitioning requires lowerBound and upperBound when partitionColumn is set.");
            }
            int numPartitions = executor.getConfiguration().getOptionalIntProperty(
                    String.format("wayang.%s.jdbc.numPartitions", producerPlatform.getPlatformId()))
                    .orElse(executor.getNumDefaultPartitions());
            reader.option("partitionColumn", partitionColumn)
                    .option("lowerBound", lowerBound)
                    .option("upperBound", upperBound)
                    .option("numPartitions", String.valueOf(numPartitions));
        }

        // Apply optional fetchsize if configured
        String fetchSize = executor.getConfiguration().getStringProperty(
                String.format("wayang.%s.jdbc.fetchsize", producerPlatform.getPlatformId()), null);
        if (fetchSize != null) {
            reader.option("fetchsize", fetchSize);
        }

        Dataset<Row> df = reader.load();

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

        if (isTuple) {
            int leftColumnCount = -1;
            try (Connection conn = databaseDescriptor.createJdbcConnection()) {
                leftColumnCount = SqlToStreamOperator.resolveLeftColumnCount(conn, input);
            } catch (Exception ignored) {}
            final int finalLeftColumnCount = leftColumnCount;
            JavaRDD<Tuple2<Record, Record>> resultSetRDD = df.toJavaRDD().map(row -> rowToTuple2(row, finalLeftColumnCount));
            output.accept((JavaRDD) resultSetRDD, executor);
        } else {
            JavaRDD<Record> resultSetRDD = df.toJavaRDD().map(SqlToRddOperator::rowToRecord);
            output.accept((JavaRDD) resultSetRDD, executor);
        }

        ExecutionLineageNode queryLineageNode = new ExecutionLineageNode(operatorContext);
        queryLineageNode.add(LoadProfileEstimators.createFromSpecification(
                String.format("wayang.%s.sqltordd.load.query", this.jdbcPlatform.getPlatformId()),
                executor.getConfiguration()
        ));
        queryLineageNode.addPredecessor(input.getLineage());
        ExecutionLineageNode outputLineageNode = new ExecutionLineageNode(operatorContext);
        outputLineageNode.add(LoadProfileEstimators.createFromSpecification(
                String.format("wayang.%s.sqltordd.load.output", this.jdbcPlatform.getPlatformId()),
                executor.getConfiguration()
        ));
        output.getLineage().addPredecessor(outputLineageNode);

        return queryLineageNode.collectAndMark();
    }

    public static Record rowToRecord(Row row) {
        int length = row.size();
        Object[] fields = new Object[length];
        for (int i = 0; i < length; i++) {
            fields[i] = row.get(i);
        }
        return new Record(fields);
    }

    public static Tuple2<Record, Record> rowToTuple2(Row row, int leftColumnCount) {
        int total = row.size();
        if (leftColumnCount <= 0 || leftColumnCount >= total) {
            leftColumnCount = total / 2;
        }
        Object[] leftFields = new Object[leftColumnCount];
        for (int i = 0; i < leftColumnCount; i++) {
            leftFields[i] = row.get(i);
        }
        int rightColumnCount = total - leftColumnCount;
        Object[] rightFields = new Object[rightColumnCount];
        for (int i = 0; i < rightColumnCount; i++) {
            rightFields[i] = row.get(leftColumnCount + i);
        }
        return new Tuple2<>(new Record(leftFields), new Record(rightFields));
    }

    private static String cleanQuery(String query) {
        if (query == null) {
            return "";
        }
        String trimmed = query.trim();
        while (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        }
        return trimmed;
    }

    private static boolean isTableName(String query) {
        return !query.contains(" ") && !query.contains("\t") && !query.contains("\n") && !query.contains("(");
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList(
                String.format("wayang.%s.sqltordd.load.query", this.jdbcPlatform.getPlatformId()),
                String.format("wayang.%s.sqltordd.load.output", this.jdbcPlatform.getPlatformId())
        );
    }

    @Override
    public WayangJsonObj toJson() {
        return new WayangJsonObj().put("platform", this.jdbcPlatform.getClass().getCanonicalName());
    }

    @SuppressWarnings("rawtypes")
    public static SqlToRddOperator fromJson(WayangJsonObj wayangJsonObj) {
        final String platformClassName = wayangJsonObj.getString("platform");
        JdbcPlatformTemplate jdbcPlatform = ReflectionUtils.evaluate(platformClassName + ".getInstance()");
        return new SqlToRddOperator(jdbcPlatform);
    }
}
