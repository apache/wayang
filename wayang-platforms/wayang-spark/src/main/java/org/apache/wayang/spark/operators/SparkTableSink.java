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
package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.util.DatabaseProduct;
import org.apache.wayang.basic.util.SqlIdentifierUtils;
import org.apache.wayang.basic.util.SqlTypeUtils;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class SparkTableSink<T> extends TableSink<T> implements SparkExecutionOperator {

    private SaveMode mode;

    public SparkTableSink(Properties props, String mode, String tableName, String... columnNames) {
        super(props, mode, tableName, columnNames);
        this.setMode(mode);
    }

    public SparkTableSink(Properties props, String mode, String tableName, String[] columnNames, DataSetType<T> type) {
        super(props, mode, tableName, columnNames, type);
        this.setMode(mode);
    }

    public SparkTableSink(TableSink<T> that) {
        super(that);
        this.setMode(that.getMode());
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 1;
        assert outputs.length == 0;

        JavaRDD<T> recordRDD = ((RddChannel.Instance) inputs[0]).provideRdd();
        Class<T> typeClass = (Class<T>) this.getType().getDataUnitType().getTypeClass();
        SparkSession sparkSession = SparkSession.builder().sparkContext(sparkExecutor.sc.sc()).getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        Dataset<Row> df;
        if (typeClass == Record.class) {
            List<T> sample = recordRDD.take(1);
            if (sample.isEmpty()) {
                return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
            }
            Record first = (Record) sample.get(0);

            List<SqlTypeUtils.SchemaField> schemaFields = SqlTypeUtils.getSchema(first,
                    SqlTypeUtils.detectProduct(this.getProperties().getProperty("url")),
                    this.getColumnNames());

            JavaRDD<Row> rowRDD = recordRDD.map(rec -> RowFactory.create(((Record) rec).getValues()));

            StructField[] fields = new StructField[schemaFields.size()];
            for (int i = 0; i < schemaFields.size(); i++) {
                SqlTypeUtils.SchemaField sf = schemaFields.get(i);
                org.apache.spark.sql.types.DataType sparkType = getSparkDataType(sf.getJavaClass());
                fields[i] = new StructField(sf.getName(), sparkType, true, Metadata.empty());
            }

            // We skip updating column names in the operator to avoid mutating shared state.
            // Inferred names are used locally for df creation.

            df = sqlContext.createDataFrame(rowRDD, new StructType(fields));
        } else {
            df = sqlContext.createDataFrame(recordRDD, typeClass);
            // For POJOs, we currently do not support custom columnNames to avoid
            // ambiguous or misleading mappings. Fail fast if they are provided.
            String[] columnNames = this.getColumnNames();
            if (columnNames != null && columnNames.length > 0) {
                throw new WayangException(
                        "columnNames are not supported for POJO inputs in SparkTableSink. " +
                                "Either omit columnNames or use Record inputs if you need custom column mapping.");
            }
        }

        Properties writeProps = new Properties();
        writeProps.putAll(this.getProperties());
        if (!writeProps.containsKey("batchSize")) {
            writeProps.setProperty("batchSize", "250000");
        }

        DatabaseProduct product = SqlTypeUtils.detectProduct(this.getProperties().getProperty("url"));
        String targetTable = SqlIdentifierUtils.escapeAndQuoteIdentifier(this.getTableName(), product);

        df.write()
                .mode(this.mode)
                .jdbc(this.getProperties().getProperty("url"), targetTable, writeProps);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    // Package-private (not private) so SparkTableSinkTest can unit-test this
    // Java-class -> Spark DataType mapping directly; it uses no instance state.
    org.apache.spark.sql.types.DataType getSparkDataType(Class<?> cls) {
        if (cls == Integer.class || cls == int.class)
            return DataTypes.IntegerType;
        if (cls == Long.class || cls == long.class)
            return DataTypes.LongType;
        if (cls == Short.class || cls == short.class)
            return DataTypes.ShortType;
        if (cls == Double.class || cls == double.class)
            return DataTypes.DoubleType;
        if (cls == Float.class || cls == float.class)
            return DataTypes.FloatType;
        if (cls == java.math.BigDecimal.class)
            return DataTypes.createDecimalType(38, 18);
        if (cls == Boolean.class || cls == boolean.class)
            return DataTypes.BooleanType;
        if (cls == java.sql.Date.class || cls == java.time.LocalDate.class)
            return DataTypes.DateType;
        if (cls == java.sql.Timestamp.class || cls == java.time.LocalDateTime.class)
            return DataTypes.TimestampType;
        return DataTypes.StringType;
    }

    public void setMode(String mode) {
        if (mode == null) {
            throw new WayangException("Unspecified write mode for SparkTableSink.");
        } else if (mode.equals("append")) {
            this.mode = SaveMode.Append;
        } else if (mode.equals("overwrite")) {
            this.mode = SaveMode.Overwrite;
        } else if (mode.equals("errorIfExists")) {
            this.mode = SaveMode.ErrorIfExists;
        } else if (mode.equals("ignore")) {
            this.mode = SaveMode.Ignore;
        } else {
            throw new WayangException(
                    String.format("Specified write mode for SparkTableSink does not exist: %s", mode));
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.tablesink.load";
    }
}
