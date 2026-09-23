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

package org.apache.wayang.jdbc.execution;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.operators.JdbcFilterOperator;
import org.apache.wayang.jdbc.operators.JdbcParquetSource;
import org.apache.wayang.jdbc.operators.JdbcProjectionOperator;
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.jdbc.operators.SqlToStreamOperator;
import org.apache.wayang.jdbc.test.HsqldbFilterOperator;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.apache.wayang.jdbc.test.HsqldbProjectionOperator;
import org.apache.wayang.jdbc.test.HsqldbTableSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link JdbcExecutor}.
 */
class JdbcExecutorTest {

    @Test
    void testCreateSqlStringResolvesConfiguredParquetSourceMapping() throws SQLException {
        Configuration configuration = new Configuration();
        configuration.setProperty(
                "wayang.hsqldb.parquetsource.mappings",
                "file:///warehouse/orders=orders_external"
        );
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));

        HsqldbParquetSource source = new HsqldbParquetSource("file:///warehouse/orders", null, "id", "name");
        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);

        StringBuilder query = JdbcExecutor.createSqlString(
                executor,
                source,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                Collections.emptyList(),
                configuration
        );

        assertEquals("SELECT * FROM orders_external", query.toString());
    }

    @Test
    void testCreateSqlStringAutoCreatesConfiguredParquetSourceRelation() throws SQLException {
        Configuration configuration = new Configuration();
        configuration.setProperty(
                "wayang.hsqldb.parquetsource.mappings",
                "file:///warehouse/auto-orders=auto_orders_external"
        );
        configuration.setProperty("wayang.hsqldb.parquetsource.auto-create", "true");
        configuration.setProperty(
                "wayang.hsqldb.parquetsource.auto-create.template",
                "CREATE TABLE IF NOT EXISTS ${relation} (id INTEGER)"
        );
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));

        HsqldbParquetSource source = new HsqldbParquetSource("file:///warehouse/auto-orders", null, "id");
        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);

        StringBuilder query = JdbcExecutor.createSqlString(
                executor,
                source,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                Collections.emptyList(),
                configuration
        );

        assertEquals("SELECT * FROM auto_orders_external", query.toString());
        try (Connection connection = HsqldbPlatform.getInstance()
                .createDatabaseDescriptor(configuration)
                .createJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM auto_orders_external")) {
            resultSet.next();
            assertEquals(0, resultSet.getLong(1));
        }
    }

    @Test
    void testCreateSqlStringAutoCreatesGeneratedParquetSourceRelation() throws SQLException {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.hsqldb.parquetsource.auto-create", "true");
        configuration.setProperty(
                "wayang.hsqldb.parquetsource.auto-create.relation-prefix",
                "auto_parquet_"
        );
        configuration.setProperty(
                "wayang.hsqldb.parquetsource.auto-create.template",
                "CREATE TABLE IF NOT EXISTS ${relation} (id INTEGER)"
        );
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));

        HsqldbParquetSource source = new HsqldbParquetSource("file:///warehouse/generated-orders", null, "id");
        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);

        String query = JdbcExecutor.createSqlString(
                executor,
                source,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                Collections.emptyList(),
                configuration
        ).toString();

        assertTrue(query.startsWith("SELECT * FROM auto_parquet_"));
        String relationName = query.substring("SELECT * FROM ".length());
        try (Connection connection = HsqldbPlatform.getInstance()
                .createDatabaseDescriptor(configuration)
                .createJdbcConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM " + relationName)) {
            resultSet.next();
            assertEquals(0, resultSet.getLong(1));
        }
    }

    @Test
    void testExecuteWithPlainTableSource() throws SQLException {
        Configuration configuration = new Configuration();
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("customer");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(tableSourceTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        tableSourceTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));
        assertEquals(
                "SELECT * FROM customer",
                sqlQueryChannelInstance.getSqlQuery()
        );
    }

    @Test
    void testExecuteWithFilter() throws SQLException {
        Configuration configuration = new Configuration();
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("customer");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        JdbcFilterOperator ageFilterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> {
                            throw new UnsupportedOperationException();
                        },
                        Record.class
                ).withSqlImplementation("age >= 18")
        );
        ExecutionTask ageFilterTask = new ExecutionTask(ageFilterOperator);
        ageFilterTask.setStage(sqlStage);
        tableSourceTask.getOutputChannel(0).addConsumer(ageFilterTask, 0);
        ageFilterTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, ageFilterOperator.getOutput(0)));

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(ageFilterTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        ageFilterTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));
        assertEquals(
                "SELECT * FROM customer WHERE age >= 18",
                sqlQueryChannelInstance.getSqlQuery()
        );
    }

    @Test
    void testExecuteWithProjection() throws SQLException {
        Configuration configuration = new Configuration();
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("customer");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        JdbcProjectionOperator projectionOperator = new HsqldbProjectionOperator("name", "age");
        ExecutionTask projectionTask = new ExecutionTask(projectionOperator);
        projectionTask.setStage(sqlStage);
        tableSourceTask.getOutputChannel(0).addConsumer(projectionTask, 0);
        projectionTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, projectionOperator.getOutput(0)));

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(projectionTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        projectionTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));
        assertEquals(
                "SELECT name, age FROM customer",
                sqlQueryChannelInstance.getSqlQuery()
        );
    }

    @Test
    void testExecuteWithProjectionAndFilters() throws SQLException {
        Configuration configuration = new Configuration();
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("customer");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        JdbcFilterOperator ageFilterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> {
                            throw new UnsupportedOperationException();
                        },
                        Record.class
                ).withSqlImplementation("age >= 18")
        );
        ExecutionTask ageFilterTask = new ExecutionTask(ageFilterOperator);
        ageFilterTask.setStage(sqlStage);
        tableSourceTask.getOutputChannel(0).addConsumer(ageFilterTask, 0);
        ageFilterTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, ageFilterOperator.getOutput(0)));

        JdbcFilterOperator nameFilterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> {
                            throw new UnsupportedOperationException();
                        },
                        Record.class
                ).withSqlImplementation("name IS NOT NULL")
        );
        ExecutionTask nameFilterTask = new ExecutionTask(nameFilterOperator);
        nameFilterTask.setStage(sqlStage);
        ageFilterTask.getOutputChannel(0).addConsumer(nameFilterTask, 0);
        nameFilterTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, nameFilterOperator.getOutput(0)));

        JdbcProjectionOperator projectionOperator = new HsqldbProjectionOperator("name", "age");
        ExecutionTask projectionTask = new ExecutionTask(projectionOperator);
        projectionTask.setStage(sqlStage);
        nameFilterTask.getOutputChannel(0).addConsumer(projectionTask, 0);
        projectionTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, projectionOperator.getOutput(0)));

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(projectionTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        projectionTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));
        assertEquals(
                "SELECT name, age FROM customer WHERE age >= 18 AND name IS NOT NULL",
                sqlQueryChannelInstance.getSqlQuery()
        );
    }

    private static class HsqldbParquetSource extends JdbcParquetSource {

        HsqldbParquetSource(String inputUrl, String[] projection, String... columnNames) {
            super(inputUrl, projection, columnNames);
        }

        @Override
        public HsqldbPlatform getPlatform() {
            return HsqldbPlatform.getInstance();
        }
    }
}
