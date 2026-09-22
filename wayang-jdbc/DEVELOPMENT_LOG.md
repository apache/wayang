<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Wayang JDBC Development Log

## Design Baseline

The external JDBC layer is a read-only gateway. Apache Wayang does not own
table storage, so JDBC operations that imply database-owned mutable state are
not emulated. Updates, DDL, transactions, savepoints, procedures, batches,
generated keys, and writable result sets are explicitly unsupported.

The first implementation uses:

```text
JDBC client -> driver -> TCP protocol -> server -> SqlContext -> Wayang execution
```

Query results are materialized in memory and exposed through paged server
cursors. Streaming execution is intentionally deferred.

## Foundation Completed

The initial implementation established:

* the protocol, driver, and server Maven modules
* a versioned length-prefixed JSON protocol
* driver service registration and `jdbc:wayang://` URL parsing
* SQL API query results containing both rows and Calcite-derived columns
* logical server sessions, query dispatch, fetch cursors, and protocol errors
* the driver socket client and the first `Connection`, `Statement`,
  `ResultSet`, `ResultSetMetaData`, and `DatabaseMetaData` implementations

## 2026-07-23 — Phase 1 Production Completion

### Read-only boundary

* Added an authoritative server-side Calcite AST check before query execution.
* Accepted query nodes, including read-only `WITH` and `EXPLAIN` forms.
* Rejected DDL, DML, transaction commands, calls, and other state-changing
  statements with `UNSUPPORTED_OPERATION` and SQLState `0A000`.
* Added a conservative lexical fallback so unsupported write statements remain
  rejected even when Calcite cannot parse their syntax.

### SQL results and JDBC objects

* Corrected `ResultSet` cursor states for empty, before-first, on-row, and
  after-last results.
* Hardened response identifiers, cursor invariants, column definitions, and row
  widths against malformed protocol data.
* Added JDBC-default value coercion based on `ColumnInfo`, including exact
  decimal decoding, binary data, temporal values, primitive range checks, and
  JDBC 4.2 typed `getObject`.
* Completed common string, numeric, temporal, stream, metadata, and
  `wasNull()` access paths.
* Made fetch-size changes affect later fetches and applied maximum-row limits
  while iterating.
* Tracked statements from their connection and closed dependent result sets and
  server cursors deterministically.
* Kept query cancellation, query timeout, writable results, and unsupported
  statement forms explicit instead of reporting capabilities that are not
  implemented.

### Catalog metadata

* Added immutable SQL catalog, schema, and table metadata snapshots to
  `SqlContext`.
* Derived tables and columns from the same configured Calcite schema used for
  query validation and execution.
* Implemented JDBC-shaped schema, table, and column responses with exact
  catalog selection, wildcard identifier filtering, table-type filtering, and
  JDBC ordering.
* Added useful SQL type information and read-only capability reporting for
  JDBC metadata consumers.
* Corrected metadata result ordering and type/class mappings.

### Transport and server lifecycle

* Aligned the driver and server default port at `9999`.
* Added bounded client, session, cursor, and fetch resources.
* Bound logical sessions to the socket that created them and released sessions
  and cursors on disconnect.
* Validated statement ownership when closing or cancelling cursors.
* Added a protocol ping for connection validation.
* Made terminal protocol/transport failures invalidate the client connection.
* Added deterministic server startup rollback and shutdown of sockets,
  sessions, cursors, and worker threads.
* Prevented cursor leaks or lost batches when a result cannot be encoded.

### Scope intentionally deferred

The current request explicitly excluded adding unit or end-to-end test sources.
The next validation phase should add focused protocol, driver, server, paging,
metadata, getter, lifecycle, and `DriverManager` loopback coverage.

External compatibility passes with DBeaver and DataGrip are also deferred. The
driver currently uses normal Maven transitive dependencies and does not produce
a shaded JDBC-tool bundle.

### Known Phase 1 limitations

* Query results are fully materialized before cursor paging.
* JDBC cancellation cannot interrupt a Wayang job that is already running.
* Authentication, authorization, and TLS are not implemented.
* Prepared statements and advanced JDBC metadata are not implemented.
* Nested Calcite schema hierarchies cannot be losslessly represented by the
  current single JDBC schema field and are not flattened into dotted names.

### Verification

Production sources were compiled on Java 17 with focused Maven reactors.
The final serial verification commands are:

```bash
env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH ./mvnw -pl :wayang-jdbc-protocol,:wayang-jdbc-driver,:wayang-jdbc-server -am -DskipTests -Dmaven.javadoc.skip=true package
```

```bash
env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH ./mvnw -pl :wayang-api-sql -am -DskipTests -Dmaven.javadoc.skip=true compile
```

## 2026-07-27 - First Java Client Test Scope

The first automated validation scope now uses a plain Java JDBC client. It does
not depend on DBeaver, DataGrip, or another external tool.

Added tests:

* `ProtocolMessageCodecTest` for ping message round-trip, decimal row payloads,
  and maximum frame enforcement.
* `WayangJdbcUrlTest` for URL acceptance, host/port/database parsing, property
  precedence, percent decoding, and invalid URL rejection.
* `WayangJdbcClientProtocolTest` for driver handling of mismatched request IDs,
  server EOF, and unsupported-operation protocol errors.
* `CursorStoreTest` for cursor paging consistency, failed response factories,
  cursor replacement, connection cleanup, and capacity enforcement.
* `JdbcServerSessionManagerTest` for logical connection ownership, per-client
  limits, total limits, and client cleanup.
* `JdbcRequestDispatcherTest` for server-side read-only rejection, cursor paging
  and final-fetch cleanup, client ownership, and connection-close cleanup.
* `DefaultSqlMetadataProviderTest` for JDBC metadata catalog selection, wildcard
  filtering, escaped patterns, table-type filtering, column attributes, and
  ordering.
* `WayangJdbcServerJavaClientTest` for a `DriverManager` loopback client
  against an in-process `WayangJdbcServer`.

The loopback test covers:

* `Connection`, `Statement`, `ResultSet`, `ResultSetMetaData`, and
  `DatabaseMetaData` through standard `java.sql` interfaces
* cursor paging through fetch size
* object, primitive, decimal, date, binary, and null getters
* `wasNull()`
* server-side rejection of write SQL before query execution
* metadata browsing for schemas, tables, and columns
* `Connection.isValid(...)` using the protocol ping

Implementation adjustment:

* `Connection.isValid(...)` now sends the server ping instead of only checking
  local closed state.
* `WayangJdbcServer` has a package-private constructor for tests to inject a
  metadata provider while keeping the public API unchanged.

Verification:

```bash
env -u npm_config_prefix JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/bin:/bin ./mvnw -pl :wayang-jdbc-protocol,:wayang-jdbc-driver,:wayang-jdbc-server -am -Dtest=ProtocolMessageCodecTest,WayangJdbcUrlTest,WayangJdbcClientProtocolTest,CursorStoreTest,JdbcServerSessionManagerTest,JdbcRequestDispatcherTest,DefaultSqlMetadataProviderTest,WayangJdbcServerJavaClientTest -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.javadoc.skip=true test
```

Result: build success, 29 JDBC gateway tests run, 0 failures.

The unfiltered JDBC reactor was also run:

```bash
env -u npm_config_prefix JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/bin:/bin ./mvnw -pl :wayang-jdbc-protocol,:wayang-jdbc-driver,:wayang-jdbc-server -am -Dmaven.javadoc.skip=true test
```

Result: build success across the selected JDBC modules and their local reactor
dependencies. The JDBC modules ran 29 tests with 0 failures. One upstream
Python dependency test was skipped because `numpy` is not installed in this
environment; it is not related to the JDBC module.

Use `-am` for this command in the current snapshot workspace. Running only the
three JDBC modules without `-am` can resolve same-version Wayang snapshot
dependencies from the remote snapshot repository instead of the local checkout.

## 2026-07-27 - Beginner Demo Quickstart

Added a self-contained demo under `wayang-jdbc/demo` so users do not need to
manually create a Calcite model or Wayang properties file just to verify the
JDBC gateway.

The demo includes:

* `data/people.csv` sample input data
* `start-demo-server.sh`, which builds the server artifacts, writes a concrete
  `/tmp/wayang-jdbc-demo/wayang.properties` file with the correct absolute data
  path, and starts `WayangJdbcServer` on `127.0.0.1:9999`
* `WayangJdbcDemoClient.java`, a plain Java `DriverManager` client
* `run-demo-client.sh`, which builds the driver artifacts, compiles the demo
  client, connects to the server, and runs a read-only `SELECT`

The README quickstart now starts with these two commands:

```bash
bash wayang-jdbc/demo/start-demo-server.sh
```

```bash
bash wayang-jdbc/demo/run-demo-client.sh
```
