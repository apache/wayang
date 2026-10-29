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

package org.apache.wayang.applications;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuckDBDemoTest {
    @TempDir
    Path directory;

    @Test
    void initializesOnlyOnRequestAndUsesExistingData() throws Exception {
        String jdbcUrl = "jdbc:duckdb:" + directory.resolve("example.duckdb");
        Path config = directory.resolve("example.properties");
        Files.writeString(config, "wayang.duckdb.jdbc.url = " + jdbcUrl.replace("\\", "/") + "\n");
        String configUrl = config.toUri().toString();
        DuckDBDemo.main(new String[]{configUrl, "--init"});
        try (var connection = DriverManager.getConnection(jdbcUrl);
             var statement = connection.createStatement()) {
            statement.execute("INSERT INTO wayang_demo.orders VALUES (7, 103, 'AMER', 42.0)");
        }
        DuckDBDemo.main(new String[]{configUrl});
        try (var connection = DriverManager.getConnection(jdbcUrl);
             var statement = connection.createStatement()) {
            try (var result = statement.executeQuery("SELECT count(*) FROM wayang_demo.orders")) {
                assertTrue(result.next());
                assertEquals(7, result.getInt(1));
            }
            try (var result = statement.executeQuery(
                    "SELECT count(*), sum(amount) FROM wayang_demo.projection_result"
            )) {
                assertTrue(result.next());
                assertEquals(4, result.getInt(1));
                assertEquals(3872.75, result.getDouble(2), 0.001);
            }
        }
        assertThrows(Exception.class, () -> DuckDBDemo.main(new String[]{configUrl, "--init"}));
        try (var connection = DriverManager.getConnection(jdbcUrl);
             var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT count(*) FROM wayang_demo.orders")) {
            assertTrue(result.next());
            assertEquals(7, result.getInt(1));
        }
    }
}
