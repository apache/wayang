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

# Wayang JDBC Demo

This demo is the easiest way to verify that CSV files exposed through the demo
Calcite model can be queried through the Wayang JDBC driver implementation.

Run from the repository root.

To start the Wayang JDBC server with the demo CSV directory exposed as schema
`fs`, run:

```bash
bash wayang-jdbc/demo/start-demo-server.sh
```

To inspect the available CSV files, select one, and execute SQL through the
client-side JDBC layer and JDBC server, run:

```bash
bash wayang-jdbc/demo/run-demo-client.sh
```

The server script:

* builds the JDBC server and dependencies
* creates `/tmp/wayang-jdbc-demo/wayang.properties`
* points the Calcite file schema at `wayang-jdbc/demo/data`
* starts `org.apache.wayang.jdbc.server.WayangJdbcServer` on `127.0.0.1:9999`

The client script:

* compiles `CsvSelectionOperationsDemo.java`
* lists CSV files from `wayang-jdbc/demo/data`
* shows each file's SQL-style table name, such as `fs.people`
* lets you select a CSV file by number or name
* connects to `jdbc:wayang://127.0.0.1:9999/demo`
* executes SQL through the client-side JDBC layer and JDBC server

For example, selecting `heart_disease_risk` analyzes
`data/heart_disease_risk_2026.csv` as the logical table
`fs.heart_disease_risk_2026`.

The demo runs:

```sql
SELECT COUNT(*) AS total_rows FROM fs.heart_disease_risk_2026
SELECT * FROM fs.heart_disease_risk_2026 LIMIT 5
```

The demo is intentionally read-only. The CSV files remain in `data/`; Wayang
does not store them as database tables. The JDBC server exposes them through
the configured SQL model and executes read-only queries over them.
