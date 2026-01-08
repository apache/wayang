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
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link SqlToStreamOperator}.
 */
class JdbcJoinOperatorTest extends OperatorTestBase {
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

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceATask));
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
            "SELECT * FROM testA JOIN testB ON testB.a=testA.a;",
            sqlQueryChannelInstance.getSqlQuery()
        );
    }
}
