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

package org.apache.wayang.clickhouse.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.clickhouse.ClickHouse;
import org.apache.wayang.clickhouse.channels.ChannelConversions;
import org.apache.wayang.clickhouse.platform.ClickHousePlatform;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcExecutionOperator;
import org.apache.wayang.spark.channels.RddChannel;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the ClickHouse execution operators beyond SQL generation:
 * copies, channels, cost-model configuration and the sink ENGINE fallbacks.
 * No ClickHouse server is needed.
 */
class ClickHouseOperatorsTest {

    private final FunctionCompiler compiler = new FunctionCompiler();

    private static TransformationDescriptor<Record, Record> key(String... sql) {
        TransformationDescriptor<Record, Record> descriptor = new TransformationDescriptor<>(
                (Record r) -> r, Record.class, Record.class);
        return sql.length == 2 ? descriptor.withSqlImplementation(sql[0], sql[1]) : descriptor;
    }

    private static ReduceDescriptor<Record> sum() {
        return new ReduceDescriptor<>((a, b) -> a, Record.class)
                .withSqlImplementation("SUM(amount) AS total_amount");
    }

    /** One instance of every operator that reads from and writes to the SQL channel. */
    private static List<JdbcExecutionOperator> queryOperators() {
        return Arrays.asList(
                new ClickHouseFilterOperator(
                        new PredicateDescriptor<>((Record r) -> true, Record.class)
                                .withSqlImplementation("region = 'AMER'")),
                new ClickHouseProjectionOperator("region", "amount"),
                new ClickHouseJoinOperator<>(key("orders", "customer_id"), key("customers", "cust_id")),
                new ClickHouseGlobalReduceOperator(sum()),
                new ClickHouseReduceByOperator(key("region", "region"), sum()),
                new ClickHouseSortOperator(key("amount", "ASC")));
    }

    @Test
    void copiesKeepTypeAndSqlClause() {
        for (JdbcExecutionOperator operator : queryOperators()) {
            ExecutionOperator copy = operator.copy();

            assertNotSame(operator, copy);
            assertSame(operator.getClass(), copy.getClass());
            assertEquals(
                    operator.createSqlClause(null, this.compiler),
                    ((JdbcExecutionOperator) copy).createSqlClause(null, this.compiler));
        }
    }

    @Test
    void queryOperatorsUseTheClickHouseSqlChannel() {
        ChannelDescriptor sqlChannel = ClickHousePlatform.getInstance().getSqlQueryChannelDescriptor();
        for (JdbcExecutionOperator operator : queryOperators()) {
            assertSame(ClickHousePlatform.getInstance(), operator.getPlatform());
            assertEquals(Collections.singletonList(sqlChannel), operator.getSupportedInputChannels(0));
            assertEquals(Collections.singletonList(sqlChannel), operator.getSupportedOutputChannels(0));
        }
    }

    @Test
    void tableSourceHasNoInputChannels() {
        ClickHouseTableSource source = new ClickHouseTableSource("analytics.orders", "order_id");

        assertThrows(UnsupportedOperationException.class, () -> source.getSupportedInputChannels(0));
        assertEquals(
                Collections.singletonList(ClickHousePlatform.getInstance().getSqlQueryChannelDescriptor()),
                source.getSupportedOutputChannels(0));
    }

    @Test
    void tableSourceCopyConstructorKeepsTable() {
        ClickHouseTableSource source = new ClickHouseTableSource("analytics.orders", "order_id", "region");

        ClickHouseTableSource copy = new ClickHouseTableSource(source);

        assertEquals("analytics.orders", copy.getTableName());
        assertEquals("analytics.orders", copy.createSqlClause(null, this.compiler));
    }

    @Test
    void projectionAcceptsDescriptor() {
        ClickHouseProjectionOperator projection = new ClickHouseProjectionOperator(
                ProjectionDescriptor.createForRecords(
                        new RecordType("order_id", "region", "amount"), "order_id", "amount"));

        assertEquals("order_id, amount", projection.createSqlClause(null, this.compiler));
    }

    @Test
    void tableSinkHasNoOutputChannels() {
        ClickHouseTableSinkOperator sink = new ClickHouseTableSinkOperator(
                "wayang_it.result", new String[]{"order_id"});

        assertEquals(
                Collections.singletonList(ClickHousePlatform.getInstance().getSqlQueryChannelDescriptor()),
                sink.getSupportedInputChannels(0));
        assertThrows(UnsupportedOperationException.class, () -> sink.getSupportedOutputChannels(0));
    }

    @Test
    void tableSinkFallsBackToDefaultEngine() {
        String[] columns = {"order_id"};

        assertEquals(ClickHouseTableSinkOperator.DEFAULT_ENGINE,
                new ClickHouseTableSinkOperator("t", columns).getEngine());
        assertEquals(ClickHouseTableSinkOperator.DEFAULT_ENGINE,
                new ClickHouseTableSinkOperator("t", columns, null).getEngine());
        assertEquals(ClickHouseTableSinkOperator.DEFAULT_ENGINE,
                new ClickHouseTableSinkOperator("t", columns, "   ").getEngine());
        assertEquals("Log", new ClickHouseTableSinkOperator("t", columns, "  Log  ").getEngine());
    }

    @Test
    void tableSinkEngineFromLogicalSinkProperties() {
        // No properties at all.
        assertEquals(ClickHouseTableSinkOperator.DEFAULT_ENGINE,
                new ClickHouseTableSinkOperator(
                        new TableSink<Record>(null, "overwrite", "t", "order_id")).getEngine());

        // Properties without the engine key.
        assertEquals(ClickHouseTableSinkOperator.DEFAULT_ENGINE,
                new ClickHouseTableSinkOperator(
                        new TableSink<Record>(new Properties(), "overwrite", "t", "order_id")).getEngine());

        // Blank value is ignored, surrounding whitespace is trimmed.
        Properties blank = new Properties();
        blank.setProperty(ClickHouseTableSinkOperator.ENGINE_PROPERTY, "  ");
        assertEquals(ClickHouseTableSinkOperator.DEFAULT_ENGINE,
                new ClickHouseTableSinkOperator(
                        new TableSink<Record>(blank, "overwrite", "t", "order_id")).getEngine());

        Properties padded = new Properties();
        padded.setProperty(ClickHouseTableSinkOperator.ENGINE_PROPERTY, " Memory ");
        assertEquals("Memory",
                new ClickHouseTableSinkOperator(
                        new TableSink<Record>(padded, "overwrite", "t", "order_id")).getEngine());
    }

    @Test
    void tableSinkRejectsUnsafeTableName() {
        assertThrows(IllegalArgumentException.class,
                () -> new ClickHouseTableSinkOperator("result; DROP TABLE orders", new String[]{"order_id"}));
    }

    @Test
    void defaultsProvideACostModelForEveryOperator() {
        Configuration configuration = new Configuration();
        // Touch the platform so its defaults file is registered.
        assertNotNull(ClickHouse.platform());

        List<JdbcExecutionOperator> operators = new java.util.ArrayList<>(queryOperators());
        operators.add(new ClickHouseTableSource("orders", "order_id"));
        operators.add(new ClickHouseTableSinkOperator("result", new String[]{"order_id"}));

        for (JdbcExecutionOperator operator : operators) {
            String key = operator.getLoadProfileEstimatorConfigurationKey();
            assertTrue(key.startsWith("wayang.clickhouse."), key);
            assertTrue(configuration.getOptionalStringProperty(key).isPresent(),
                    "missing default for " + key);
            assertTrue(operator.createLoadProfileEstimator(configuration).isPresent(),
                    "no load profile estimator for " + key);
        }
    }

    @Test
    void channelConversionsLeadToJavaAndSpark() {
        assertEquals(2, ChannelConversions.ALL.size());
        ChannelDescriptor sqlChannel = ClickHousePlatform.getInstance().getSqlQueryChannelDescriptor();
        for (ChannelConversion conversion : ChannelConversions.ALL) {
            assertEquals(sqlChannel, conversion.getSourceChannelDescriptor());
        }
        assertEquals(StreamChannel.DESCRIPTOR,
                ChannelConversions.SQL_TO_STREAM_CONVERSION.getTargetChannelDescriptor());
        assertEquals(RddChannel.UNCACHED_DESCRIPTOR,
                ChannelConversions.SQL_TO_UNCACHED_RDD_CONVERSION.getTargetChannelDescriptor());
    }

    @Test
    void conversionsPluginRequiresClickHouseAndJava() {
        assertTrue(ClickHouse.conversionPlugin().getRequiredPlatforms()
                .contains(ClickHousePlatform.getInstance()));
        assertTrue(ClickHouse.conversionPlugin().getRequiredPlatforms()
                .contains(JavaPlatform.getInstance()));
        // setProperties is a no-op for both plugins; it must not throw.
        ClickHouse.conversionPlugin().setProperties(new Configuration());
        ClickHouse.plugin().setProperties(new Configuration());
    }
}
