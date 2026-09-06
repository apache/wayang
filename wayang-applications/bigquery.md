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

# BigQuery Example

This example demonstrates the BigQuery cost model and executes filter/projection
pushdown against an existing BigQuery table.

## Prerequisites

- Java 17
- A Google Cloud project with the BigQuery API enabled
- A dataset and a service account that can run queries and read the example table
- A service-account JSON key, or another authentication mode supported by the
  bundled Google BigQuery JDBC driver

Wayang does not provision a BigQuery emulator or Google Cloud resources.

## Prepare the table

Run this query in the BigQuery console or with your preferred BigQuery client,
after replacing `my-project` with your project ID:

```sql
CREATE SCHEMA IF NOT EXISTS `my-project.sales`;

CREATE OR REPLACE TABLE `my-project.sales.orders` AS
SELECT 1 AS order_id, 'AMER' AS region, 'book' AS product,
       25.50 AS amount, '2026-01-10' AS order_date
UNION ALL
SELECT 2, 'EMEA', 'desk', 300.00, '2026-01-11'
UNION ALL
SELECT 3, 'AMER', 'chair', 85.25, '2026-01-12';
```

You can instead use an existing table with `order_id`, `region`, `product`,
`amount`, and `order_date` columns in that order.

## Configure Wayang

Create a properties file such as `/tmp/bigquery-example.properties`:

```properties
wayang.bigquery.jdbc.url = jdbc:bigquery://https://www.googleapis.com/bigquery/v2;ProjectId=my-project;OAuthType=0;OAuthServiceAcctEmail=service-account@example.com;OAuthPvtKeyPath=/path/to/key.json
wayang.bigquery.jdbc.user =
wayang.bigquery.jdbc.password =
wayang.bigquery.demo.table = `my-project.sales.orders`
```

Authentication is configured through the JDBC URL supported by the BigQuery
JDBC driver. Keep the backticks around the fully qualified table name because
BigQuery project IDs can contain hyphens. Do not commit the service account key
or a properties file containing credentials.

## Build and run

Build the application and its dependencies from the repository root:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications -am \
  -DskipTests -Dpython.worker.tests.skip=true install
```

Run all segments against the configured table:

```bash
./mvnw -Pskip-prerequisite-check -pl wayang-applications exec:java \
  -Dexec.mainClass=org.apache.wayang.applications.BigQueryDemo \
  "-Dexec.args=file:///tmp/bigquery-example.properties all"
```

The optional second argument is `cost`, `filter`, `projection`, or `all`.
`filter`, `projection`, and `all` execute against the configured BigQuery table
and require the JDBC URL. The `cost` mode does not connect to BigQuery, so the
JDBC URL can be omitted when running that mode alone.

On Windows, replace `./mvnw` with `.\mvnw.cmd` and use a file URL such as
`file:///C:/Temp/bigquery-example.properties`.

## Troubleshooting

- **Project or dataset not found:** check `ProjectId` in the JDBC URL and the
  fully qualified table name.
- **Permission denied:** grant the identity permission to create query jobs and
  read the table data.
- **Private key error:** use an absolute path in `OAuthPvtKeyPath` and confirm
  that the Wayang process can read the JSON file.
- **Only testing configuration:** run the `cost` mode; it does not contact
  BigQuery or require credentials.
