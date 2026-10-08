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

# Wayang JDBC

This module provides the first external JDBC interface for Apache Wayang.

Apache Wayang is not a database and does not own table storage. The JDBC layer
is a read-only SQL gateway:

```text
JDBC client -> Wayang JDBC driver -> Wayang JDBC server -> Wayang SQL API -> Wayang execution
```

The Phase 1 production path supports standard JDBC connections, read-only
statement execution, paged result-set access, result metadata, and catalog
browsing for the Calcite model configured in the server.

See `DEVELOPMENT_LOG.md` for the date-wise implementation log, decisions,
verification, and remaining work.

## Module Roles

* `wayang-jdbc-protocol` contains the versioned messages and length-prefixed
  JSON codec shared by the driver and server.

* `wayang-jdbc-client` implements the client-side `java.sql` interfaces and
  communicates with the server over TCP.

* `wayang-jdbc-server` owns logical JDBC sessions, delegates queries to the
  Wayang SQL API, and returns rows and Calcite-backed metadata.

The existing `wayang-platforms/wayang-jdbc-template`,
`wayang-platforms/wayang-postgres`, `wayang-platforms/wayang-sqlite3`, and
`wayang-platforms/wayang-generic-jdbc` modules solve the opposite direction:
Wayang reading from JDBC-accessible databases. They are not the external JDBC
client interface.

## First Supported Scope

The first usable version is intentionally read-only. Its supported surface is:

* `DriverManager.getConnection("jdbc:wayang://host:port[/database]")`
* forward-only, read-only `Connection`, `Statement`, and `ResultSet` objects
* `Statement.executeQuery(String)` and `Statement.execute(String)` for queries
* cursor paging through `ResultSet.next()`
* object, string, numeric, binary, and temporal result getters
* JDBC 4.2 typed `ResultSet.getObject(..., Class<?>)`
* `ResultSet.wasNull()` and common stream getters
* `ResultSetMetaData` basics: column count, name, label, JDBC type, type name,
  precision, scale, nullability.
* `DatabaseMetaData` driver/product information, capability flags, SQL type
  information, schemas, tables, and columns

The following remain deliberately unsupported and throw
`SQLFeatureNotSupportedException` with SQLState `0A000` where JDBC exposes an
operation:

* `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, `MERGE`
* transactions and savepoints
* prepared and callable statements
* stored procedures
* writable or scrollable result sets
* batched statements
* generated keys
* query cancellation and timeouts
* SQL features not supported by the Wayang SQL API

The server independently parses every submitted statement and accepts only
Calcite query nodes, including read-only `WITH` and `EXPLAIN` forms. This
server-side gate is authoritative even when a caller bypasses normal JDBC
methods such as `executeUpdate`.

## Connecting

The URL format is:

```text
jdbc:wayang://host[:port][/database][?property=value&...]
```

The default port is `9999`. Connection properties can be supplied either in
the URL query or through the `Properties` passed to `DriverManager`; URL query
values take precedence. Supported client properties are exposed through
`Driver.getPropertyInfo`.

`user` and `password` are carried in the opening protocol request for future
authentication integration. Phase 1 does not authenticate or encrypt
connections, so the gateway should be used only on a trusted network.

The server entry point accepts:

```text
WayangJdbcServer [host] [port] [configuration-file]
```

It defaults to `127.0.0.1:9999`. The server configuration must provide the
Calcite model used by `SqlContext`, including the `wayang.calcite.model`
property required by the SQL API.

## Quickstart: Plain Java Client

If you only want to verify that the JDBC module works, use the bundled demo.
It creates the required configuration file for you.

Terminal 1, from the repository root:

```bash
bash wayang-jdbc/demo/start-demo-server.sh
```

Terminal 2, from the repository root:

```bash
bash wayang-jdbc/demo/run-demo-client.sh
```

The demo starts the server on `127.0.0.1:9999`, lists the CSV files configured
under schema `fs`, lets you select one, connects through `DriverManager`, and
executes SQL against the selected logical table.

For example, selecting `people.csv` runs queries such as:

```sql
SELECT COUNT(*) AS total_rows FROM fs.people
SELECT * FROM fs.people LIMIT 5
```

The sample data is in `wayang-jdbc/demo/data/`. The CSV files remain in that
directory; the demo exposes them as logical SQL-style tables such as
`fs.people` and queries them through the client-side JDBC layer and JDBC
server.

The external JDBC interface has two running pieces:

1. Start a `WayangJdbcServer` process with a Wayang configuration that points
   to the Calcite model.
2. Put `wayang-jdbc-client` and its runtime dependencies on the client
   application's classpath, then connect with normal `java.sql` APIs.

### 1. Build the JDBC artifacts

From the repository root:

```bash
env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH ./mvnw -pl :wayang-jdbc-server -am -DskipTests -Dmaven.javadoc.skip=true install
```

This builds and installs the JDBC server, driver, protocol, and required
Wayang snapshot artifacts into the local Maven repository.

### 2. Prepare the server configuration

Create or reuse a Wayang configuration file that contains the Calcite model
used for SQL validation, query planning, execution, and metadata browsing:

```properties
wayang.calcite.model={"version":"1.0","defaultSchema":"fs","schemas":[...]}
```

The value is the inline Calcite model JSON string expected by the Wayang SQL
API. Table names, schema names, and metadata visible through JDBC come from
this model. The optional `/database` part of the JDBC URL is exposed as the
JDBC catalog/session database; it does not create storage.

For manual startup, the configuration path must point to a real file. Do not
paste placeholder paths such as `/path/to/wayang.properties`. If you are only
testing locally, prefer the bundled demo script because it creates this file
for you.

### 3. Start the Wayang JDBC server

Build a runtime classpath for the server module:

```bash
./mvnw -pl :wayang-jdbc-server -DincludeScope=runtime -Dmdep.outputFile=target/runtime-classpath.txt dependency:build-classpath
```

Start the server:

```bash
java -cp "wayang-jdbc/wayang-jdbc-server/target/apache-wayang-jdbc-server-1.1.2-SNAPSHOT.jar:$(cat wayang-jdbc/wayang-jdbc-server/target/runtime-classpath.txt)" org.apache.wayang.jdbc.server.WayangJdbcServer 127.0.0.1 9999 /path/to/wayang.properties
```

Arguments are:

```text
WayangJdbcServer [host] [port] [configuration-file]
```

If omitted, `host` defaults to `127.0.0.1` and `port` defaults to `9999`.

### 4. Add the driver to a Java application

For a Maven client project, use the driver dependency:

```xml
<dependency>
    <groupId>org.apache.wayang</groupId>
    <artifactId>wayang-jdbc-driver</artifactId>
    <version>1.1.2-SNAPSHOT</version>
</dependency>
```

The driver depends on `wayang-jdbc-protocol` and Jackson. Maven and Gradle
resolve those dependencies transitively after the artifacts have been installed
or published.

If you run a plain `javac`/`java` command or configure a JDBC GUI tool from
local files, include all runtime JARs. You can copy them with:

```bash
./mvnw -pl :wayang-jdbc-driver -DincludeScope=runtime -DoutputDirectory=target/driver-libs dependency:copy-dependencies
```

Then put these on the client classpath:

* `wayang-jdbc-client/target/apache-wayang-jdbc-driver-1.1.2-SNAPSHOT.jar`
* every JAR in `wayang-jdbc-client/target/driver-libs/`

The JDBC driver class is:

```text
org.apache.wayang.jdbc.driver.WayangDriver
```

Java applications normally do not need to instantiate the driver directly
because the driver JAR contains the `META-INF/services/java.sql.Driver` service
loader entry. Calling `Class.forName(...)` is still safe for explicit loading.

### 5. Connect and query from plain Java

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

public class WayangJdbcExample {

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.wayang.jdbc.driver.WayangDriver");

        Properties properties = new Properties();
        properties.setProperty("user", "demo");
        properties.setProperty("connectTimeout", "5000");

        String url = "jdbc:wayang://127.0.0.1:9999/analytics";

        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT * FROM PEOPLE")) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                for (int column = 1; column <= columnCount; column++) {
                    System.out.printf(
                            "%s=%s%n",
                            metaData.getColumnLabel(column),
                            resultSet.getObject(column)
                    );
                }
            }
        }
    }
}
```

URL examples:

```text
jdbc:wayang://127.0.0.1:9999
jdbc:wayang://127.0.0.1:9999/analytics
jdbc:wayang://127.0.0.1:9999/analytics?user=demo&connectTimeout=5000
```

Supported connection properties:

* `user` - optional user name carried to the server for future authentication.
* `password` - optional password carried to the server for future
  authentication.
* `connectTimeout` - TCP connect and initial handshake timeout in
  milliseconds.

The connection is read-only. Use `SELECT`, `VALUES`, `TABLE`, and read-only
forms supported by the Wayang SQL API. Operations such as `CREATE`, `INSERT`,
`UPDATE`, `DELETE`, `MERGE`, transactions, procedures, and writable result sets
are intentionally unsupported.

## Query and Result Contract

`SqlContext.executeSqlWithMetadata(String)` returns:

```java
SqlQueryResult {
    List<SqlColumn> columns;
    Collection<Record> rows;
}
```

Column metadata comes from the optimized Calcite `RelNode` row type. The
server converts each value to a stable wire representation, and the driver
coerces it back according to the advertised JDBC type. This preserves decimal
precision and the JDBC default mappings for numeric, binary, and temporal
values.

Phase 1 materializes each query result in server memory. The protocol exposes
in-memory cursor IDs so the driver can fetch it in pages; this is paging, not
streaming execution. Server-side cursor and connection limits prevent
unbounded resource growth, and disconnecting a client releases its sessions
and cursors.

## Metadata Contract

The server answers metadata requests from the same Calcite root schema used to
validate and execute SQL:

* schemas from the configured Calcite root schema
* tables from schema table maps
* columns from each table row type
* Java/Calcite type mapping to `java.sql.Types`
* JDBC wildcard matching for schema, table, and column patterns
* exact matching for catalog selectors

Metadata rows use the JDBC-mandated column names, shapes, and sort order for
the implemented `DatabaseMetaData` methods. Metadata result sets are
read-only.

## Package Boundary

The current external JDBC modules use `org.apache.wayang.jdbc.driver` and
`org.apache.wayang.jdbc.protocol`. Internal JDBC platform code already uses
`org.apache.wayang.jdbc.*` under `wayang-platforms/wayang-jdbc-template`.

To keep implementation risk low, the current package layout is retained for the
first JDBC gateway implementation. Classes should still be named clearly with
driver/server/protocol responsibilities to avoid mixing external JDBC gateway
code with internal JDBC platform operators.

## Packaging for JDBC Tools

The driver artifact has normal Maven runtime dependencies on
`wayang-jdbc-protocol` and Jackson. Maven and Gradle consumers receive those
dependencies transitively. A JDBC tool configured from local JAR files must be
given the driver JAR and its runtime dependencies; the current build does not
produce a shaded all-in-one driver.

## Test Scope

The first automated client scope is a plain Java JDBC client. The test starts
`WayangJdbcServer` on an ephemeral local port and connects with
`DriverManager.getConnection(...)`, then verifies query execution, cursor
paging, result metadata, typed getters, `wasNull`, metadata browsing, read-only
write rejection, and `Connection.isValid(...)`.

Additional focused tests cover driver protocol failures, server EOF handling,
unsupported-operation error mapping, server-side cursor lifecycle, logical
session ownership, dispatcher cleanup, and JDBC metadata filtering/order.

Run the focused Java-client JDBC gateway tests from the repository root with:

```bash
env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH ./mvnw -pl :wayang-jdbc-protocol,:wayang-jdbc-driver,:wayang-jdbc-server -am -Dtest=ProtocolMessageCodecTest,WayangJdbcUrlTest,WayangJdbcClientProtocolTest,CursorStoreTest,JdbcServerSessionManagerTest,JdbcRequestDispatcherTest,DefaultSqlMetadataProviderTest,WayangJdbcServerJavaClientTest -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.javadoc.skip=true test
```

Run the unfiltered JDBC reactor tests with:

```bash
env JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH ./mvnw -pl :wayang-jdbc-protocol,:wayang-jdbc-driver,:wayang-jdbc-server -am -Dmaven.javadoc.skip=true test
```

The `-am` flag is intentional in this snapshot workspace so same-version
Wayang dependencies are built from the checkout rather than resolved from a
remote snapshot repository.

## Phase 1 Limitations

* Results are fully materialized before paging.
* A running Wayang job cannot yet be interrupted through JDBC cancellation.
* Authentication, authorization, and TLS are not implemented.
* Nested Calcite schema hierarchies are not flattened into invalid dotted JDBC
  schema identifiers; browsing is limited to the representable schema level.
* DBeaver and DataGrip compatibility validation, streaming, optional live
  Calcite metadata fixture hardening, and JDBC-tool packaging remain follow-up
  work.
