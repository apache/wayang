<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# DuckDB Example

`org.apache.wayang.applications.DuckDBDemo` runs filter and projection plans
through Wayang against an embedded DuckDB database. Java 17 is required. No
Docker installation or database server is needed.

Create a properties file, for example `/tmp/duckdb-example.properties`:

```properties
wayang.duckdb.jdbc.url = jdbc:duckdb:/tmp/wayang-example.duckdb
wayang.duckdb.demo.orders = wayang_demo.orders
wayang.duckdb.demo.filter-result = wayang_demo.filter_result
wayang.duckdb.demo.projection-result = wayang_demo.projection_result
```

Use an absolute database path with an existing parent directory. The example
uses multiple JDBC connections, so it requires a persistent file. On Windows,
use a path such as `C:/Temp/wayang-example.duckdb`.

From the repository root, build and install the application and its dependencies:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications -am \
  -DskipTests -Dpython.worker.tests.skip=true install
```

To create sample data and run the plans:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications exec:java \
  -Dexec.mainClass=org.apache.wayang.applications.DuckDBDemo \
  "-Dexec.args=file:///tmp/duckdb-example.properties --init"
```

The `--init` option creates six sample orders. It fails if the input table
already exists, so it cannot replace existing input data. If a different input
schema is configured, create that schema before initialization.

To use an existing table, omit `--init`:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications exec:java \
  -Dexec.mainClass=org.apache.wayang.applications.DuckDBDemo \
  "-Dexec.args=file:///tmp/duckdb-example.properties"
```

The input table must contain `order_id BIGINT`, `customer_id BIGINT`,
`region VARCHAR`, and `amount DOUBLE`. Configure distinct input and output table
names, and create any custom output schemas before running the example. Each run
replaces the two configured output tables.

On Windows, replace `./mvnw` with `.\mvnw.cmd` and use a configuration URL such
as `file:///C:/Temp/duckdb-example.properties`.
