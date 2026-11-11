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
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.execution.DatabaseDescriptor;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.jdbc.test.HsqldbFilterOperator;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
    void testExhaustionClosesResources() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        when(jdbc.resultSet.next()).thenReturn(true, true, false);
        SqlToStreamOperator.ResultSetIterator iterator = jdbc.iterator();

        assertEquals(new Record(1), iterator.next());
        jdbc.verifyOpen();
        assertEquals(new Record(1), iterator.next());
        assertFalse(iterator.hasNext());
        jdbc.verifyClosed();
        iterator.close();
        jdbc.verifyClosed();
    }

    @Test
    void testEmptyResultsCloseResources() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        when(jdbc.resultSet.next()).thenReturn(false);

        assertFalse(jdbc.iterator().hasNext());
        jdbc.verifyClosed();
    }

    @Test
    void testStatementCreationFailureClosesConnection() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        SQLException failure = new SQLException("createStatement");
        when(jdbc.connection.createStatement()).thenThrow(failure);

        assertSame(failure, assertThrows(WayangException.class, jdbc::iterator).getCause());
        verify(jdbc.connection).close();
        verify(jdbc.statement, never()).close();
        verify(jdbc.resultSet, never()).close();
    }

    @Test
    void testQueryFailureClosesStatementAndConnection() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        SQLException failure = new SQLException("executeQuery");
        when(jdbc.statement.executeQuery("SELECT 1")).thenThrow(failure);
        doThrow(new SQLException("close statement")).when(jdbc.statement).close();

        assertSame(failure, assertThrows(WayangException.class, jdbc::iterator).getCause());
        InOrder order = inOrder(jdbc.statement, jdbc.connection);
        order.verify(jdbc.statement).close();
        order.verify(jdbc.connection).close();
        verify(jdbc.resultSet, never()).close();
    }

    @Test
    void testUncheckedQueryFailureClosesResources() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        IllegalStateException failure = new IllegalStateException("driver failure");
        when(jdbc.statement.executeQuery("SELECT 1")).thenThrow(failure);

        assertSame(failure, assertThrows(IllegalStateException.class, jdbc::iterator));
        InOrder order = inOrder(jdbc.statement, jdbc.connection);
        order.verify(jdbc.statement).close();
        order.verify(jdbc.connection).close();
        verify(jdbc.resultSet, never()).close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"next", "metadata", "columnCount", "row"})
    void testInitialReadFailureClosesResources(String operation) throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        SQLException failure = new SQLException(operation);
        jdbc.failRead(operation, failure);

        assertSame(failure, assertThrows(WayangException.class, jdbc::iterator).getCause());
        jdbc.verifyClosed();
    }

    @ParameterizedTest
    @ValueSource(strings = {"next", "metadata", "columnCount", "row"})
    void testLaterReadFailureClosesResources(String operation) throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        SqlToStreamOperator.ResultSetIterator iterator = jdbc.iterator();
        SQLException failure = new SQLException(operation);
        jdbc.failRead(operation, failure);
        doThrow(new SQLException("close result set")).when(jdbc.resultSet).close();

        assertSame(failure, assertThrows(WayangException.class, iterator::next).getCause());
        assertFalse(iterator.hasNext());
        jdbc.verifyClosed();
        iterator.close();
        jdbc.verifyClosed();
    }

    @Test
    void testUncheckedReadFailureClosesResources() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        SqlToStreamOperator.ResultSetIterator iterator = jdbc.iterator();
        IllegalStateException failure = new IllegalStateException("driver failure");
        when(jdbc.resultSet.getObject(1)).thenThrow(failure);

        assertSame(failure, assertThrows(IllegalStateException.class, iterator::next));
        jdbc.verifyClosed();
    }

    @Test
    void testExplicitCloseIsIdempotent() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        SqlToStreamOperator.ResultSetIterator iterator = jdbc.iterator();
        assertTrue(iterator.hasNext());
        jdbc.verifyOpen();

        iterator.close();
        iterator.close();

        assertFalse(iterator.hasNext());
        jdbc.verifyClosed();
    }

    @Test
    void testCloseFailuresDoNotPreventRemainingCleanup() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        SqlToStreamOperator.ResultSetIterator iterator = jdbc.iterator();
        doThrow(new SQLException("close result set")).when(jdbc.resultSet).close();
        doThrow(new IllegalStateException("close statement")).when(jdbc.statement).close();
        doThrow(new SQLException("close connection")).when(jdbc.connection).close();

        iterator.close();
        iterator.close();

        jdbc.verifyClosed();
    }

    @Test
    void testExplicitStreamClose() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        JavaExecutor executor = createJavaExecutor();
        StreamChannel.Instance output = jdbc.evaluate(executor);
        Stream<Record> stream = output.provideStream();
        assertEquals(new Record(1), stream.findFirst().get());
        jdbc.verifyOpen();

        stream.close();
        stream.close();
        jdbc.verifyClosed();
        executor.getCrossPlatformExecutor().shutdown();
        jdbc.verifyClosed();
    }

    @ParameterizedTest
    @ValueSource(strings = {"findFirst", "limit", "unconsumed", "downstreamFailure"})
    void testJobShutdownClosesUnexhaustedStream(String consumption) throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        JavaExecutor executor = createJavaExecutor();
        StreamChannel.Instance output = jdbc.evaluate(executor);
        Stream<Record> stream = output.provideStream();
        if ("findFirst".equals(consumption)) {
            assertEquals(new Record(1), stream.findFirst().get());
        } else if ("limit".equals(consumption)) {
            assertEquals(1, stream.limit(1).count());
        } else if ("downstreamFailure".equals(consumption)) {
            IllegalArgumentException failure = new IllegalArgumentException("downstream");
            assertSame(failure, assertThrows(IllegalArgumentException.class,
                    () -> stream.forEach(record -> {
                        throw failure;
                    })));
        }
        jdbc.verifyOpen();
        output.dispose();
        executor.getCrossPlatformExecutor().shutdown();

        jdbc.verifyClosed();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testJobShutdownClosesStreamWithLiveChannel(boolean downstreamFails) throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        JavaExecutor executor = createJavaExecutor();
        CrossPlatformExecutor crossPlatformExecutor = executor.getCrossPlatformExecutor();
        crossPlatformExecutor.registerGlobal(executor);
        StreamChannel.Instance output = jdbc.evaluate(executor);
        // An unfinished task or lazy successor can still hold its input channel at job shutdown.
        output.noteObtainedReference();
        try {
            Stream<Record> stream = output.provideStream();
            if (downstreamFails) {
                IllegalStateException failure = new IllegalStateException("downstream");
                assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> stream.forEach(record -> {
                            throw failure;
                        })));
                doThrow(new SQLException("close result set")).when(jdbc.resultSet).close();
            } else {
                assertEquals(new Record(1), stream.findFirst().get());
            }
            jdbc.verifyOpen();

            crossPlatformExecutor.shutdown();

            // Releasing the job's reference alone cannot dispose an executor with live channels.
            assertFalse(executor.isDisposed());
            assertTrue(executor.getNumReferences() > 0);
            jdbc.verifyClosed();
        } finally {
            output.noteDiscardedReference(true);
        }
        jdbc.verifyClosed();
    }

    @Test
    void testPublishingFailureClosesResources() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        JavaExecutor executor = createJavaExecutor();
        StreamChannel.Instance output = mock(StreamChannel.Instance.class);
        IllegalStateException failure = new IllegalStateException("accept stream");
        doThrow(failure).when(output).accept(any(Stream.class));

        assertSame(failure, assertThrows(IllegalStateException.class, () -> jdbc.evaluate(executor, output)));
        jdbc.verifyClosed();
        executor.getCrossPlatformExecutor().shutdown();
        jdbc.verifyClosed();
    }

    @Test
    void testChannelDisposalDoesNotCloseLazyDownstreamStream() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        when(jdbc.resultSet.next()).thenReturn(true, true, false);
        JavaExecutor executor = createJavaExecutor();
        StreamChannel.Instance output = jdbc.evaluate(executor);
        Stream<Record> downstream = output.<Record>provideStream().map(record -> record);

        output.dispose();
        jdbc.verifyOpen();
        assertEquals(Arrays.asList(new Record(1), new Record(1)), downstream.collect(Collectors.toList()));
        jdbc.verifyClosed();
        executor.getCrossPlatformExecutor().shutdown();
        jdbc.verifyClosed();
    }

    @Test
    void testLineageFailureClosesPublishedResources() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        JavaExecutor executor = createJavaExecutor();
        StreamChannel.Instance output = mock(StreamChannel.Instance.class);
        IllegalStateException failure = new IllegalStateException("output lineage");
        when(output.getLineage()).thenThrow(failure);

        assertSame(failure, assertThrows(IllegalStateException.class, () -> jdbc.evaluate(executor, output)));
        jdbc.verifyClosed();
        executor.getCrossPlatformExecutor().shutdown();
        jdbc.verifyClosed();
    }

    @Test
    void testExplicitCloseWithoutCrossPlatformExecutor() throws SQLException {
        JdbcResources jdbc = new JdbcResources();
        JavaExecutor executor = mock(JavaExecutor.class);
        when(executor.getConfiguration()).thenReturn(configuration);
        StreamChannel.Instance output = jdbc.evaluate(executor);
        jdbc.verifyOpen();

        output.provideStream().close();

        jdbc.verifyClosed();
    }

    private static class JdbcResources {
        private final Connection connection = mock(Connection.class);
        private final Statement statement = mock(Statement.class);
        private final ResultSet resultSet = mock(ResultSet.class);
        private final ResultSetMetaData metadata = mock(ResultSetMetaData.class);

        private JdbcResources() throws SQLException {
            when(this.connection.createStatement()).thenReturn(this.statement);
            when(this.statement.executeQuery("SELECT 1")).thenReturn(this.resultSet);
            when(this.resultSet.next()).thenReturn(true);
            when(this.resultSet.getMetaData()).thenReturn(this.metadata);
            when(this.metadata.getColumnCount()).thenReturn(1);
            when(this.resultSet.getObject(1)).thenReturn(1);
        }

        private SqlToStreamOperator.ResultSetIterator iterator() {
            return new SqlToStreamOperator.ResultSetIterator(this.connection, "SELECT 1");
        }

        private void failRead(String operation, SQLException failure) throws SQLException {
            switch (operation) {
                case "next":
                    when(this.resultSet.next()).thenThrow(failure);
                    break;
                case "metadata":
                    when(this.resultSet.getMetaData()).thenThrow(failure);
                    break;
                case "columnCount":
                    when(this.metadata.getColumnCount()).thenThrow(failure);
                    break;
                case "row":
                    when(this.resultSet.getObject(1)).thenThrow(failure);
                    break;
                default:
                    throw new IllegalArgumentException(operation);
            }
        }

        private StreamChannel.Instance evaluate(JavaExecutor executor) {
            StreamChannel.Instance output = new StreamChannel(StreamChannel.DESCRIPTOR, null)
                    .createInstance(executor, null, 0);
            this.evaluate(executor, output);
            return output;
        }

        private void evaluate(JavaExecutor executor, StreamChannel.Instance output) {
            JdbcPlatformTemplate platform = mock(JdbcPlatformTemplate.class);
            DatabaseDescriptor database = mock(DatabaseDescriptor.class);
            when(platform.createDatabaseDescriptor(executor.getConfiguration())).thenReturn(database);
            when(database.createJdbcConnection()).thenReturn(this.connection);
            ExecutionOperator producer = mock(ExecutionOperator.class);
            when(producer.getPlatform()).thenReturn(platform);
            SqlQueryChannel channel = new SqlQueryChannel(HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor(), null);
            new ExecutionTask(producer, 0, 1).setOutputChannel(0, channel);
            SqlQueryChannel.Instance input = channel.createInstance(null, null, 0);
            input.setSqlQuery("SELECT 1");
            SqlToStreamOperator operator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
            operator.evaluate(new ChannelInstance[]{input}, new ChannelInstance[]{output},
                    executor, createOperatorContext(operator));
        }

        private void verifyOpen() throws SQLException {
            verify(this.resultSet, never()).close();
            verify(this.statement, never()).close();
            verify(this.connection, never()).close();
        }

        private void verifyClosed() throws SQLException {
            InOrder order = inOrder(this.resultSet, this.statement, this.connection);
            order.verify(this.resultSet).close();
            order.verify(this.statement).close();
            order.verify(this.connection).close();
            verify(this.resultSet).close();
            verify(this.statement).close();
            verify(this.connection).close();
        }
    }
}
