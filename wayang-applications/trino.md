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

# Trino Example

This example runs filter and projection plans against an existing Trino table.
Wayang pushes `region = 'AMER'` and the selected columns into SQL executed by
Trino.

## Prerequisites

- Java 17
- A Trino deployment reachable from the machine running Wayang
- A writable catalog for preparing the sample data, or an existing readable table
- A Trino user with `SELECT` permission on the example table

Wayang does not start Trino, its catalog, or its storage services.

## Prepare the table

Using the Trino CLI or another SQL client, replace `iceberg.sales` with a catalog
and schema available in your deployment, then run:

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.sales;

CREATE TABLE iceberg.sales.orders (
    order_id BIGINT,
    region VARCHAR,
    product VARCHAR,
    amount DOUBLE,
    order_date VARCHAR
);

INSERT INTO iceberg.sales.orders VALUES
    (1, 'AMER', 'book', 25.50, '2026-01-10'),
    (2, 'EMEA', 'desk', 300.00, '2026-01-11'),
    (3, 'AMER', 'chair', 85.25, '2026-01-12');
```

If the catalog is read-only, point the example at an existing table with these
five columns in this order.

## Configure Wayang

Create a properties file such as `/tmp/trino-example.properties`:

```properties
wayang.trino.jdbc.url = jdbc:trino://trino.example.com:8080/iceberg/sales
wayang.trino.jdbc.user = wayang
wayang.trino.jdbc.password =
wayang.trino.demo.table = iceberg.sales.orders
```

Change the endpoint, credentials, catalog, schema, and table for your deployment.
Trino JDBC options such as SSL can be added to the JDBC URL.

## Build and run

Build the application and its dependencies from the repository root:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications -am \
  -DskipTests -Dpython.worker.tests.skip=true install
```

Run the example:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications exec:java \
  -Dexec.mainClass=org.apache.wayang.applications.TrinoDemo \
  "-Dexec.args=file:///tmp/trino-example.properties"
```

Both sections should return the two `AMER` rows. The projection section returns
only `region`, `product`, and `amount`. On Windows, replace `./mvnw` with
`.\mvnw.cmd` and use a URL such as `file:///C:/Temp/trino-example.properties`.

## Troubleshooting

- **Connection refused:** verify the host, port, and network access from Wayang.
- **Catalog, schema, or table not found:** check the fully qualified table name
  with the Trino CLI.
- **Authentication or TLS failure:** add the credentials and JDBC URL options
  required by the deployment.
- **Create table is unsupported:** prepare the five columns in a catalog that
  supports writes, or use an existing table.
