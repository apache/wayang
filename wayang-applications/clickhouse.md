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

# ClickHouse Example

This example runs filter and projection plans against an existing ClickHouse
table. Wayang pushes `region = 'AMER'` and the selected columns into SQL
executed by ClickHouse.

## Prerequisites

- Java 17
- A ClickHouse deployment reachable from the machine running Wayang
- A ClickHouse user with `SELECT` permission on the example table

Wayang does not start ClickHouse.

## Prepare the table

Using the ClickHouse client, replace `default` with a database available in
your deployment, then run:

```sql
CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.orders (
    order_id Int64,
    region String,
    product String,
    amount Float64,
    order_date String
) ENGINE = MergeTree ORDER BY order_id;

INSERT INTO default.orders VALUES
    (1, 'AMER', 'book', 25.50, '2026-01-10'),
    (2, 'EMEA', 'desk', 300.00, '2026-01-11'),
    (3, 'AMER', 'chair', 85.25, '2026-01-12');
```

## Configure Wayang

Create a properties file such as `/tmp/clickhouse-example.properties`:

```properties
wayang.clickhouse.jdbc.url = jdbc:clickhouse://clickhouse.example.com:8123/default
wayang.clickhouse.jdbc.user = default
wayang.clickhouse.jdbc.password =
wayang.clickhouse.demo.table = default.orders
```

Change the endpoint, credentials, database, and table for your deployment.

## Build and run

Build the application and its dependencies from the repository root:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications -am \
  -DskipTests -Dpython.worker.tests.skip=true install
```

Run the example:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications exec:java \
  -Dexec.mainClass=org.apache.wayang.applications.ClickHouseDemo \
  "-Dexec.args=file:///tmp/clickhouse-example.properties"
```

Both sections should return the two `AMER` rows. The projection section returns
only `region`, `product`, and `amount`.

## Troubleshooting

- **Connection refused:** verify the host, HTTP port (default `8123`), and
  network access from Wayang.
- **Authentication failure:** check the user and password, or JDBC URL options
  required by the deployment.
- **Table not found:** use a fully qualified `database.table` name.
