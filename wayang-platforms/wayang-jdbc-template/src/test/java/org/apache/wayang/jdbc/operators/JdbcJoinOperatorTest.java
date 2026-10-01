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
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.test.HsqldbJoinOperator;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.apache.wayang.jdbc.test.HsqldbTableSource;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.jdbc.execution.JdbcExecutor;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link SqlToStreamOperator}.
 */
class JdbcJoinOperatorTest extends OperatorTestBase {
    @BeforeEach
    void initDB() throws SQLException {
        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();
        try (
            Connection jdbcConnection = hsqldbPlatform
                .createDatabaseDescriptor(configuration)
                .createJdbcConnection()
        ) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("DROP TABLE IF EXISTS testA");
            statement.execute("DROP TABLE IF EXISTS testB");
            statement.execute("DROP TABLE IF EXISTS orders");
            statement.execute("DROP TABLE IF EXISTS shipments");
        }
    }

    @Test
    void testParquetSourceJoinResolvesConfiguredSourceMappings() throws SQLException {
        Configuration configuration = new Configuration();
        configuration.setProperty(
                "wayang.hsqldb.parquetsource.mappings",
                "file:///warehouse/testA=testA;file:///warehouse/testB=testB"
        );

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        HsqldbParquetSource sourceA = new HsqldbParquetSource("file:///warehouse/testA", null, "a", "b");
        HsqldbParquetSource sourceB = new HsqldbParquetSource("file:///warehouse/testB", null, "a", "b");

        ExecutionTask sourceATask = new ExecutionTask(sourceA);
        sourceATask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, sourceA.getOutput(0)));
        sourceATask.setStage(sqlStage);

        ExecutionTask sourceBTask = new ExecutionTask(sourceB);
        sourceBTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, sourceB.getOutput(0)));
        sourceBTask.setStage(sqlStage);

        final ExecutionOperator joinOperator = new HsqldbJoinOperator<Integer>(
            new TransformationDescriptor<Record, Integer>(
                (record) -> (Integer) record.getField(0),
                Record.class,
                Integer.class
            ).withSqlImplementation("file:///warehouse/testA", "a"),
            new TransformationDescriptor<Record, Integer>(
                (record) -> (Integer) record.getField(0),
                Record.class,
                Integer.class
            ).withSqlImplementation("file:///warehouse/testB", "a")
        );

        ExecutionTask joinTask = new ExecutionTask(joinOperator);
        sourceATask.getOutputChannel(0).addConsumer(joinTask, 0);
        sourceBTask.getOutputChannel(0).addConsumer(joinTask, 1);
        joinTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, joinOperator.getOutput(0)));
        joinTask.setStage(sqlStage);

        when(sqlStage.getStartTasks()).thenReturn(new LinkedHashSet<>(Arrays.asList(sourceBTask, sourceATask)));
        when(sqlStage.getAllTasks()).thenReturn(new LinkedHashSet<>(Arrays.asList(sourceBTask, sourceATask, joinTask)));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(joinTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        joinTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));

        assertEquals(
            "SELECT * FROM testA JOIN testB ON testB.a=testA.a",
            sqlQueryChannelInstance.getSqlQuery()
        );
    }
    @Test
    void testWithHsqldb() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testA (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testA VALUES (0, 'zero');");
            statement.execute("CREATE TABLE testB (a INT, b INT);");
            statement.execute("INSERT INTO testB VALUES (0, 100);");
        }

        JdbcTableSource tableSourceA = new HsqldbTableSource("testA");
        JdbcTableSource tableSourceB = new HsqldbTableSource("testB");

        ExecutionTask tableSourceATask = new ExecutionTask(tableSourceA);
        tableSourceATask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSourceA.getOutput(0)));
        tableSourceATask.setStage(sqlStage);

        ExecutionTask tableSourceBTask = new ExecutionTask(tableSourceB);
        tableSourceBTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSourceB.getOutput(0)));
        tableSourceBTask.setStage(sqlStage);

        final ExecutionOperator joinOperator = new HsqldbJoinOperator<Integer>(
            new TransformationDescriptor<Record, Integer>(
                (record) -> (Integer) record.getField(0),
                Record.class,
                Integer.class
            ).withSqlImplementation("testA", "a"),
            new TransformationDescriptor<Record, Integer>(
                (record) -> (Integer) record.getField(0),
                Record.class,
                Integer.class
            ).withSqlImplementation("testB", "a")
        );

        ExecutionTask joinTask = new ExecutionTask(joinOperator);
        tableSourceATask.getOutputChannel(0).addConsumer(joinTask, 0);
        tableSourceBTask.getOutputChannel(0).addConsumer(joinTask, 1);
        joinTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, joinOperator.getOutput(0)));
        joinTask.setStage(sqlStage);

        // Deliberately list the right source first: JdbcExecutor must still choose
        // the join's left source for the FROM clause.
        when(sqlStage.getStartTasks()).thenReturn(new LinkedHashSet<>(Arrays.asList(
                tableSourceBTask, tableSourceATask)));
        when(sqlStage.getAllTasks()).thenReturn(new LinkedHashSet<>(Arrays.asList(
                tableSourceBTask, tableSourceATask, joinTask)));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(joinTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        joinTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));

        System.out.println();

        assertEquals(
            "SELECT * FROM testA JOIN testB ON testB.a=testA.a",
            sqlQueryChannelInstance.getSqlQuery()
        );
    }

    @Test
    void testMultiConditionJoinWithHsqldb() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection();
        try {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE orders (order_id INT, customer_id INT, product_id INT, quantity INT);");
            statement.execute("INSERT INTO orders VALUES (1, 100, 1001, 5);");
            statement.execute("INSERT INTO orders VALUES (2, 101, 1002, 3);");
            statement.execute("INSERT INTO orders VALUES (3, 100, 1003, 2);");
            statement.execute("CREATE TABLE shipments (order_id INT, customer_id INT, ship_date VARCHAR(10));");
            statement.execute("INSERT INTO shipments VALUES (1, 100, '2024-01-15');");
            statement.execute("INSERT INTO shipments VALUES (2, 101, '2024-01-16');");
            statement.execute("INSERT INTO shipments VALUES (3, 999, '2024-01-17');");

            JdbcTableSource tableSourceOrders = new HsqldbTableSource("orders");
            JdbcTableSource tableSourceShipments = new HsqldbTableSource("shipments");

            ExecutionTask tableSourceOrdersTask = new ExecutionTask(tableSourceOrders);
            tableSourceOrdersTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSourceOrders.getOutput(0)));
            tableSourceOrdersTask.setStage(sqlStage);

            ExecutionTask tableSourceShipmentsTask = new ExecutionTask(tableSourceShipments);
            tableSourceShipmentsTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSourceShipments.getOutput(0)));
            tableSourceShipmentsTask.setStage(sqlStage);

            final ExecutionOperator joinOperator = new HsqldbJoinOperator<Record>(
                new TransformationDescriptor<Record, Record>(
                    (record) -> new Record(record.getField(0), record.getField(1)),
                    Record.class,
                    Record.class
                ).withSqlImplementation("orders", "order_id,customer_id"),
                new TransformationDescriptor<Record, Record>(
                    (record) -> new Record(record.getField(0), record.getField(1)),
                    Record.class,
                    Record.class
                ).withSqlImplementation("shipments", "order_id,customer_id")
            );

            ExecutionTask joinTask = new ExecutionTask(joinOperator);
            tableSourceOrdersTask.getOutputChannel(0).addConsumer(joinTask, 0);
            tableSourceShipmentsTask.getOutputChannel(0).addConsumer(joinTask, 1);
            joinTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, joinOperator.getOutput(0)));
            joinTask.setStage(sqlStage);

            when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceOrdersTask));
            when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(joinTask));

            ExecutionStage nextStage = mock(ExecutionStage.class);

            SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
            ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
            joinTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
            sqlToStreamTask.setStage(nextStage);

            JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
            executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

            SqlQueryChannel.Instance sqlQueryChannelInstance =
                    (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));

            String generatedSql = sqlQueryChannelInstance.getSqlQuery();
            assertEquals(
                "SELECT * FROM orders JOIN shipments ON orders.order_id=shipments.order_id AND orders.customer_id=shipments.customer_id",
                generatedSql
            );

            java.sql.ResultSet resultSet = statement.executeQuery(generatedSql);
            
            int rowCount = 0;
            boolean foundOrder1 = false;
            boolean foundOrder2 = false;
            
            while (resultSet.next()) {
                rowCount++;
                int orderId = resultSet.getInt("order_id");
                int customerId = resultSet.getInt("customer_id");
                String shipDate = resultSet.getString("ship_date");
                
                if (orderId == 1 && customerId == 100) {
                    foundOrder1 = true;
                    assertEquals("2024-01-15", shipDate);
                    assertEquals(1001, resultSet.getInt("product_id"));
                    assertEquals(5, resultSet.getInt("quantity"));
                } else if (orderId == 2 && customerId == 101) {
                    foundOrder2 = true;
                    assertEquals("2024-01-16", shipDate);
                    assertEquals(1002, resultSet.getInt("product_id"));
                    assertEquals(3, resultSet.getInt("quantity"));
                }
            }
            
            assertEquals(2, rowCount, "Should return exactly 2 rows (order_id=3 with customer_id=100 should not match shipment with customer_id=999)");
            assertEquals(true, foundOrder1, "Should find order 1 with customer 100");
            assertEquals(true, foundOrder2, "Should find order 2 with customer 101");
            
            resultSet.close();
        } finally {
            jdbcConnection.close();
        }
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
