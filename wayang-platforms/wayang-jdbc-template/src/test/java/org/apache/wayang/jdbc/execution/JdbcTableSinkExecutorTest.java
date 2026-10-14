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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.platform.AtomicExecution;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.platform.PartialExecution;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.operators.JdbcTableSinkOperator;
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.apache.wayang.jdbc.test.HsqldbTableSinkOperator;
import org.apache.wayang.jdbc.test.HsqldbTableSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for in-database table sink execution via {@link JdbcExecutor#executeSinkStage}.
 */
class JdbcTableSinkExecutorTest {

    @Test
    void testOverwriteModeCreatesNewTable() throws SQLException {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.core.log.enabled", "true");
        configuration.setProperty("wayang.hsqldb.cpu.mhz", "2700");
        configuration.setProperty("wayang.hsqldb.cores", "1");
        configuration.setProperty(
                "wayang.hsqldb.tablesource.load",
                "{\"in\":0,\"out\":1,\"cpu\":\"${1}\",\"ram\":\"0\",\"p\":1.0}"
        );
        configuration.setProperty(
                "wayang.hsqldb.tablesink.load",
                "{\"in\":1,\"out\":0,\"cpu\":\"${1}\",\"ram\":\"0\",\"p\":1.0}"
        );
        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create source table with data
        try (Connection conn = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS source_overwrite;");
            stmt.execute("DROP TABLE IF EXISTS target_overwrite;");
            stmt.execute("CREATE TABLE source_overwrite (id INT, name VARCHAR(50));");
            stmt.execute("INSERT INTO source_overwrite VALUES (1, 'Jenny');");
            stmt.execute("INSERT INTO source_overwrite VALUES (2, 'Nick');");
            stmt.execute("INSERT INTO source_overwrite VALUES (3, 'Klaudia');");
        }

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(
                new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor =
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        //Build the execution stage source to sink
        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("source_overwrite");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0,
                new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        JdbcTableSinkOperator sinkOp = new HsqldbTableSinkOperator(
                "target_overwrite", new String[]{"id", "name"});
        sinkOp.setMode("overwrite");
        ExecutionTask sinkTask = new ExecutionTask(sinkOp);
        sinkTask.setStage(sqlStage);
        tableSourceTask.getOutputChannel(0).addConsumer(sinkTask, 0);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(sinkTask));
        when(sqlStage.getAllTasks()).thenReturn(new HashSet<>(Arrays.asList(tableSourceTask, sinkTask)));

        // Execute
        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        optimizationContext.addOneTimeOperator(tableSource);
        optimizationContext.addOneTimeOperator(sinkOp);
        executor.execute(sqlStage, optimizationContext, job.getCrossPlatformExecutor());

        assertEquals(1, job.getCrossPlatformExecutor().getPartialExecutions().size());
        PartialExecution partialExecution =
                job.getCrossPlatformExecutor().getPartialExecutions().iterator().next();
        Set<String> estimatorKeys = partialExecution.getAtomicExecutionGroups().stream()
                .flatMap(group -> group.getAtomicExecutions().stream())
                .map(AtomicExecution::getLoadProfileEstimator)
                .map(estimator -> estimator.getConfigurationKey())
                .collect(Collectors.toSet());
        assertEquals(
                new HashSet<>(Arrays.asList(
                        "wayang.hsqldb.tablesource.load",
                        "wayang.hsqldb.tablesink.load"
                )),
                estimatorKeys
        );

        // Verify table was created and contains all 3 rows
        try (Connection conn = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM target_overwrite;");
            rs.next();
            assertEquals(3, rs.getInt(1));

            rs = stmt.executeQuery("SELECT id, name FROM target_overwrite ORDER BY id;");
            rs.next();
            assertEquals(1, rs.getInt("id"));
            assertEquals("Jenny", rs.getString("name"));
            rs.next();
            assertEquals(2, rs.getInt("id"));
            assertEquals("Nick", rs.getString("name"));
            rs.next();
            assertEquals(3, rs.getInt("id"));
            assertEquals("Klaudia", rs.getString("name"));
        }
    }

    @Test
    void testOverwriteModeReplacesExistingTable() throws SQLException {
        Configuration configuration = new Configuration();
        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create source table and a pre-existing target table
        try (Connection conn = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS source_replace;");
            stmt.execute("DROP TABLE IF EXISTS target_replace;");
            stmt.execute("CREATE TABLE source_replace (id INT, val VARCHAR(50));");
            stmt.execute("INSERT INTO source_replace VALUES (1, 'new_data');");
            // Pre existing target table with different schema and data
            stmt.execute("CREATE TABLE target_replace (x INT, y INT, z INT);");
            stmt.execute("INSERT INTO target_replace VALUES (50, 20, 10);");
        }

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(
                new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor =
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("source_replace");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0,
                new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        JdbcTableSinkOperator sinkOp = new HsqldbTableSinkOperator(
                "target_replace", new String[]{"id", "val"});
        sinkOp.setMode("overwrite");
        ExecutionTask sinkTask = new ExecutionTask(sinkOp);
        sinkTask.setStage(sqlStage);
        tableSourceTask.getOutputChannel(0).addConsumer(sinkTask, 0);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(sinkTask));

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());
        assertEquals(0, job.getCrossPlatformExecutor().getPartialExecutions().size());

        // Verify target was replaced. Old data should be gone, new schema and data present
        try (Connection conn = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM target_replace;");
            rs.next();
            assertEquals(1, rs.getInt(1));

            rs = stmt.executeQuery("SELECT id, val FROM target_replace;");
            rs.next();
            assertEquals(1, rs.getInt("id"));
            assertEquals("new_data", rs.getString("val"));
        }
    }

    @Test
    void testAppendModeInsertsIntoExistingTable() throws SQLException {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.core.log.enabled", "false");
        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        //Create source and target table. Target has existing data.
        try (Connection conn = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS source_append;");
            stmt.execute("DROP TABLE IF EXISTS target_append;");
            stmt.execute("CREATE TABLE source_append (id INT, name VARCHAR(50));");
            stmt.execute("INSERT INTO source_append VALUES (10, 'Ten');");
            stmt.execute("INSERT INTO source_append VALUES (20, 'Twenty');");
            stmt.execute("CREATE TABLE target_append (id INT, name VARCHAR(50));");
            stmt.execute("INSERT INTO target_append VALUES (1, 'Existing');");
        }

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(
                new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor =
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("source_append");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0,
                new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        JdbcTableSinkOperator sinkOp = new HsqldbTableSinkOperator(
                "target_append", new String[]{"id", "name"});
        sinkOp.setMode("append");
        ExecutionTask sinkTask = new ExecutionTask(sinkOp);
        sinkTask.setStage(sqlStage);
        tableSourceTask.getOutputChannel(0).addConsumer(sinkTask, 0);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(sinkTask));

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());
        assertEquals(0, job.getCrossPlatformExecutor().getPartialExecutions().size());

        // Verify existing data remains and new data is appended
        try (Connection conn = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM target_append;");
            rs.next();
            assertEquals(3, rs.getInt(1));

            // Verify the pre-existing row is still there
            rs = stmt.executeQuery("SELECT COUNT(*) FROM target_append WHERE id = 1;");
            rs.next();
            assertEquals(1, rs.getInt(1));

            // Verify the new rows were appended
            rs = stmt.executeQuery("SELECT COUNT(*) FROM target_append WHERE id >= 10;");
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testOverwriteClauseGeneration() {
        JdbcTableSinkOperator sinkOp = new HsqldbTableSinkOperator(
                "my_table", new String[]{"col1"});
        sinkOp.setMode("overwrite");
        assertEquals("CREATE TABLE \"my_table\" AS (", sinkOp.createSqlClause(null, null));
    }

    @Test
    void testAppendClauseGeneration() {
        JdbcTableSinkOperator sinkOp = new HsqldbTableSinkOperator(
                "my_table", new String[]{"col1"});
        sinkOp.setMode("append");
        assertEquals("INSERT INTO \"my_table\"", sinkOp.createSqlClause(null, null));
    }

    @Test
    void testMultipartTableNameQuotedClauseGeneration() {
        JdbcTableSinkOperator sinkOp = new HsqldbTableSinkOperator(
                "public.my_table", new String[]{"col1"});
        sinkOp.setMode("overwrite");
        assertEquals("CREATE TABLE \"public\".\"my_table\" AS (", sinkOp.createSqlClause(null, null));
    }

    @Test
    void testInvalidTableNameThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator("target; DROP TABLE users; --", new String[]{"col1"}));
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator("target\"table", new String[]{"col1"}));
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator("target table", new String[]{"col1"}));
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator("123table", new String[]{"col1"}));
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator("", new String[]{"col1"}));
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator(null, new String[]{"col1"}));
    }

    @Test
    void testInvalidColumnNamesThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator("my_table", new String[]{"col; DROP TABLE users; --"}));
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator("my_table", new String[]{"col 1"}));
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator("my_table", new String[]{"col\"name"}));
        assertThrows(IllegalArgumentException.class, () ->
                new HsqldbTableSinkOperator("my_table", new String[]{""}));
    }
}

