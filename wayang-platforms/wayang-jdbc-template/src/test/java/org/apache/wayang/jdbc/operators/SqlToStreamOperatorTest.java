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
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.test.HsqldbFilterOperator;
import org.apache.wayang.jdbc.test.HsqldbJoinOperator;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link SqlToStreamOperator}.
 */
class SqlToStreamOperatorTest extends OperatorTestBase {

    @Test
    void testWithHsqldb() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);

        CrossPlatformExecutor cpe = new CrossPlatformExecutor(job, new FullInstrumentationStrategy());
        when(job.getCrossPlatformExecutor()).thenReturn(cpe);
        final JavaExecutor javaExecutor = new JavaExecutor(JavaPlatform.getInstance(), job);

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testWithHsqldb (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testWithHsqldb VALUES (0, 'zero');");
            statement.execute("INSERT INTO testWithHsqldb VALUES (1, 'one');");
            statement.execute("INSERT INTO testWithHsqldb VALUES (2, 'two');");
        }

        final ExecutionOperator filterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(x -> false, Record.class)
        );
        final SqlQueryChannel sqlQueryChannel = new SqlQueryChannel(
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor(),
                filterOperator.getOutput(0)
        );
        SqlQueryChannel.Instance sqlQueryChannelInstance = sqlQueryChannel.createInstance(
                hsqldbPlatform.createExecutor(job),
                mock(OptimizationContext.OperatorContext.class),
                0
        );
        sqlQueryChannelInstance.setSqlQuery("SELECT * FROM testWithHsqldb;");
        ExecutionTask producer = new ExecutionTask(filterOperator);
        producer.setOutputChannel(0, sqlQueryChannel);

        StreamChannel.Instance streamChannelInstance =
                new StreamChannel(StreamChannel.DESCRIPTOR, mock(OutputSlot.class)).createInstance(
                        javaExecutor,
                        mock(OptimizationContext.OperatorContext.class),
                        0
                );

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        evaluate(
                sqlToStreamOperator,
                new ChannelInstance[]{sqlQueryChannelInstance},
                new ChannelInstance[]{streamChannelInstance}
        );

        List<Record> output = streamChannelInstance.<Record>provideStream().collect(Collectors.toList());
        List<Record> expected = Arrays.asList(
                new Record(0, "zero"),
                new Record(1, "one"),
                new Record(2, "two")
        );

        assertEquals(expected, output);
    }

    @Test
    void testWithEmptyHsqldb() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);

        CrossPlatformExecutor cpe = new CrossPlatformExecutor(job, new FullInstrumentationStrategy());
        when(job.getCrossPlatformExecutor()).thenReturn(cpe);
        final JavaExecutor javaExecutor = new JavaExecutor(JavaPlatform.getInstance(), job);

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testWithEmptyHsqldb (a INT, b VARCHAR(6));");
        }

        final ExecutionOperator filterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(x -> false, Record.class)
        );
        final SqlQueryChannel sqlQueryChannel = new SqlQueryChannel(
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor(),
                filterOperator.getOutput(0)
        );
        SqlQueryChannel.Instance sqlQueryChannelInstance = sqlQueryChannel.createInstance(
                hsqldbPlatform.createExecutor(job),
                mock(OptimizationContext.OperatorContext.class),
                0
        );
        sqlQueryChannelInstance.setSqlQuery("SELECT * FROM testWithEmptyHsqldb;");
        ExecutionTask producer = new ExecutionTask(filterOperator);
        producer.setOutputChannel(0, sqlQueryChannel);

        StreamChannel.Instance streamChannelInstance =
                new StreamChannel(StreamChannel.DESCRIPTOR, mock(OutputSlot.class)).createInstance(
                        javaExecutor,
                        mock(OptimizationContext.OperatorContext.class),
                        0
                );

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        evaluate(
                sqlToStreamOperator,
                new ChannelInstance[]{sqlQueryChannelInstance},
                new ChannelInstance[]{streamChannelInstance}
        );

        List<Record> output = streamChannelInstance.<Record>provideStream().collect(Collectors.toList());
        assertTrue(output.isEmpty());
    }

    @Test
    void testJoinWithHsqldbYieldsTuple2() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);

        CrossPlatformExecutor cpe = new CrossPlatformExecutor(job, new FullInstrumentationStrategy());
        when(job.getCrossPlatformExecutor()).thenReturn(cpe);
        final JavaExecutor javaExecutor = new JavaExecutor(JavaPlatform.getInstance(), job);

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data for join.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE usersJoin (id INT, name VARCHAR(20));");
            statement.execute("INSERT INTO usersJoin VALUES (1, 'Alice');");
            statement.execute("INSERT INTO usersJoin VALUES (2, 'Bob');");

            statement.execute("CREATE TABLE ordersJoin (order_id INT, user_id INT, amount INT);");
            statement.execute("INSERT INTO ordersJoin VALUES (101, 1, 50);");
            statement.execute("INSERT INTO ordersJoin VALUES (102, 2, 75);");
        }

        final ExecutionOperator joinOperator = new HsqldbJoinOperator<Integer>(
                new TransformationDescriptor<Record, Integer>(
                        (record) -> (Integer) record.getField(0),
                        Record.class,
                        Integer.class
                ).withSqlImplementation("usersJoin", "id"),
                new TransformationDescriptor<Record, Integer>(
                        (record) -> (Integer) record.getField(1),
                        Record.class,
                        Integer.class
                ).withSqlImplementation("ordersJoin", "user_id")
        );

        final SqlQueryChannel sqlQueryChannel = new SqlQueryChannel(
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor(),
                joinOperator.getOutput(0)
        );
        ExecutionTask producer = new ExecutionTask(joinOperator);
        producer.setOutputChannel(0, sqlQueryChannel);

        SqlQueryChannel.Instance sqlQueryChannelInstance = sqlQueryChannel.createInstance(
                hsqldbPlatform.createExecutor(job),
                mock(OptimizationContext.OperatorContext.class),
                0
        );
        sqlQueryChannelInstance.setSqlQuery("SELECT * FROM usersJoin JOIN ordersJoin ON usersJoin.id=ordersJoin.user_id;");

        StreamChannel.Instance streamChannelInstance =
                new StreamChannel(StreamChannel.DESCRIPTOR, mock(OutputSlot.class)).createInstance(
                        javaExecutor,
                        mock(OptimizationContext.OperatorContext.class),
                        0
                );

        SqlToStreamOperator<Tuple2<Record, Record>> sqlToStreamOperator =
                new SqlToStreamOperator<>(HsqldbPlatform.getInstance(), joinOperator.getOutput(0).getType());

        evaluate(
                sqlToStreamOperator,
                new ChannelInstance[]{sqlQueryChannelInstance},
                new ChannelInstance[]{streamChannelInstance}
        );

        List<Tuple2<Record, Record>> output = streamChannelInstance.<Tuple2<Record, Record>>provideStream().collect(Collectors.toList());
        assertEquals(2, output.size());

        // Verify each element is a Tuple2<Record, Record> and can be consumed safely without ClassCastException
        Tuple2<Record, Record> firstTuple = output.get(0);
        assertEquals(new Record(1, "Alice"), firstTuple.getField0());
        assertEquals(new Record(101, 1, 50), firstTuple.getField1());

        Tuple2<Record, Record> secondTuple = output.get(1);
        assertEquals(new Record(2, "Bob"), secondTuple.getField0());
        assertEquals(new Record(102, 2, 75), secondTuple.getField1());

        // Simulate downstream Java map step accessing typed tuple fields
        List<String> mapped = output.stream()
                .map(t -> t.getField0().getField(1) + ":" + t.getField1().getField(2))
                .collect(Collectors.toList());
        assertEquals(Arrays.asList("Alice:50", "Bob:75"), mapped);
    }

    @Test
    void testTypeAdaptationAndValidation() {
        SqlToStreamOperator<?> op = new SqlToStreamOperator<>(HsqldbPlatform.getInstance());
        assertEquals(Record.class, op.getInput(0).getType().getDataUnitType().getTypeClass());
        assertEquals(Record.class, op.getOutput(0).getType().getDataUnitType().getTypeClass());

        DataSetType<Tuple2> tupleType = DataSetType.createDefaultUnchecked(Tuple2.class);
        op.adaptType(tupleType);
        assertEquals(Tuple2.class, op.getInput(0).getType().getDataUnitType().getTypeClass());
        assertEquals(Tuple2.class, op.getOutput(0).getType().getDataUnitType().getTypeClass());
    }

}
