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

# Presto Example

This example reads an existing PrestoDB table, filters rows whose `region` is
`AMER`, and returns only the `region`, `product`, and `amount` columns. Wayang
pushes the filter and projection into the SQL query executed by Presto.

## Prerequisites

- Java 17
- A PrestoDB deployment reachable from the machine running Wayang
- A catalog and schema in which you can create or read the example table
- A Presto user with `SELECT` permission on that table

Wayang does not start or configure PrestoDB. The endpoint can be a local,
shared, or hosted deployment.

## Prepare the table

Run the following with your Presto CLI or SQL client. Replace
`memory.default.orders` with a table name supported by your catalog when the
memory connector is unavailable.

```sql
CREATE TABLE memory.default.orders (
    order_id BIGINT,
    region VARCHAR,
    product VARCHAR,
    amount DOUBLE,
    order_date VARCHAR
);

INSERT INTO memory.default.orders VALUES
    (1, 'AMER', 'book', 25.50, '2026-01-10'),
    (2, 'EMEA', 'desk', 300.00, '2026-01-11'),
    (3, 'AMER', 'chair', 85.25, '2026-01-12');
```

The example accepts any table with these five columns in this order:
`order_id`, `region`, `product`, `amount`, and `order_date`.

## Configure Wayang

Create `/tmp/presto-example.properties`:

```properties
wayang.presto.jdbc.url = jdbc:presto://presto.example.com:8080/memory/default
wayang.presto.jdbc.user = wayang
wayang.presto.demo.table = memory.default.orders
```

Change the URL, credentials, and fully qualified table name for your deployment.
Add `wayang.presto.jdbc.password` only when the deployment requires it; the
Presto driver rejects an empty password on a non-TLS connection. TLS and other
driver options can be included in the JDBC URL.

## Build and run

From the repository root:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications -am \
  -DskipTests -Dpython.worker.tests.skip=true install

./mvnw -Pskip-prerequisite-check -pl wayang-applications exec:java \
  -Dexec.mainClass=org.apache.wayang.applications.PrestoDemo \
  "-Dexec.args=file:///tmp/presto-example.properties"
```

The output should contain the two `AMER` rows and only three columns. On
Windows, use `.\mvnw.cmd` and a URL such as
`file:///C:/Temp/presto-example.properties`.

## Troubleshooting

- **Connection refused:** check the host, port, and network access from the
  Wayang machine.
- **Catalog, schema, or table not found:** use a fully qualified table name and
  confirm it with `SELECT * FROM catalog.schema.table` in a Presto client.
- **Authentication failed:** set the user, password, and any TLS or access-token
  options required by your Presto JDBC endpoint.
