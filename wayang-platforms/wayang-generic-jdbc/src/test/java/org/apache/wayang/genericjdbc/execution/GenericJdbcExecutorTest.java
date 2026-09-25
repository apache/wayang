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

package org.apache.wayang.genericjdbc.execution;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.genericjdbc.platform.GenericJdbcPlatform;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for {@link GenericJdbcExecutor}.
 * Verifies that the executor properly creates and disposes a JDBC connection.
 */
class GenericJdbcExecutorTest {

    private static Configuration createTestConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setProperty(
                "wayang.genericjdbc.jdbc.url",
                "jdbc:hsqldb:mem:wayang_test_executor_lifecycle;DB_CLOSE_DELAY=-1"
        );
        configuration.setProperty("wayang.genericjdbc.jdbc.user", "SA");
        configuration.setProperty("wayang.genericjdbc.jdbc.password", "");
        configuration.setProperty(
                "wayang.genericjdbc.jdbc.driverName",
                "org.hsqldb.jdbcDriver"
        );
        return configuration;
    }

    private static Job createMockJob(Configuration configuration) {
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor())
                .thenReturn(new CrossPlatformExecutor(
                        job,
                        new NoInstrumentationStrategy()
                ));
        return job;
    }

    /**
     * Tests that GenericJdbcExecutor creates a non-null JDBC connection
     * upon construction.
     *
     * This is a regression test for the bug where connection creation was
     * commented out, leaving the connection field permanently null.
     */
    @Test
    void testConnectionIsCreatedUponConstruction() throws Exception {
        Configuration configuration = createTestConfiguration();
        Job job = createMockJob(configuration);
        GenericJdbcPlatform platform = GenericJdbcPlatform.getInstance();

        GenericJdbcExecutor executor = new GenericJdbcExecutor(platform, job);

        Field connectionField =
                GenericJdbcExecutor.class.getDeclaredField("connection");
        connectionField.setAccessible(true);

        Connection connection =
                (Connection) connectionField.get(executor);

        assertNotNull(
                connection,
                "GenericJdbcExecutor must create a JDBC connection upon construction"
        );
        assertNotNull(
                connection.getMetaData(),
                "Connection must be valid and able to return metadata"
        );
    }

    /**
     * Tests that dispose() closes the JDBC connection without throwing.
     */
    @Test
    void testDisposeClosesConnection() throws Exception {
        Configuration configuration = createTestConfiguration();
        Job job = createMockJob(configuration);
        GenericJdbcPlatform platform = GenericJdbcPlatform.getInstance();

        GenericJdbcExecutor executor = new GenericJdbcExecutor(platform, job);

        Field connectionField =
                GenericJdbcExecutor.class.getDeclaredField("connection");
        connectionField.setAccessible(true);

        Connection connection =
                (Connection) connectionField.get(executor);

        assertNotNull(
                connection,
                "Connection must exist before dispose"
        );

        assertFalse(
                connection.isClosed(),
                "Connection must be open before dispose"
        );

        assertDoesNotThrow(
                () -> executor.dispose(),
                "dispose() must not throw when closing a valid connection"
        );

        assertTrue(
                connection.isClosed(),
                "Connection must be closed after dispose()"
        );
    }
}