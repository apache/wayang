<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Integration Tests – SQL Platforms

End-to-end tests that drive JDBC platform operators through the Wayang API
against a user-managed database. Each suite **skips** (it does not fail) when
the server is unreachable.

| Test | Module | Needs |
|------|--------|-------|
| `PrestoOperatorsIT` | `wayang-presto` | a Presto deployment |
| `ClickHouseOperatorsIT` | `wayang-clickhouse` | a ClickHouse deployment |

---

## Prerequisites

- **JDK 17** — required. The Scala in `wayang-spark` does not compile on JDK 21+,
  and the build targets release 17, so JDK 11 is too old. Point Maven at a JDK 17:
  ```bash
  export JAVA_HOME=/path/to/jdk-17      # e.g. .../corretto-17.jdk/Contents/Home
  ```
- **Maven 3.8+**
- **Docker**

### Common Maven flags

The repo's root build runs RAT + license + prerequisite checks that are noisy for
local runs; skip them:

```
-Drat.skip=true -Dlicense.skip=true -Dmaven.javadoc.skip=true -Pskip-prerequisite-check
```

> First build only: drop `-o` (offline) so Maven can download dependencies.

---

## Presto

The test creates and seeds its own `memory.wayang_it` tables in a user-managed
Presto deployment with the memory connector enabled. It drops the fixtures
afterward.

```bash
# Run the operator tests against the configured deployment (JDK 17)
PRESTO_HOST=presto.example.com PRESTO_PORT=8080 PRESTO_USER=wayang \
JAVA_HOME=/path/to/jdk-17 \
mvn -o test -pl wayang-platforms/wayang-presto \
  -Dtest=PrestoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -Drat.skip=true -Dlicense.skip=true -Dmaven.javadoc.skip=true -Pskip-prerequisite-check
```

Expected: `Tests run: 4, Failures: 0, Errors: 0, Skipped: 0`.

Connection settings are supplied through `PRESTO_HOST`, `PRESTO_PORT`, and
`PRESTO_USER`.

---

## ClickHouse

The test creates and seeds its own `wayang_it` database/tables in a user-managed
ClickHouse deployment, then drops the fixtures afterward.

```bash
CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=8123 CLICKHOUSE_USER=default \
JAVA_HOME=/path/to/jdk-17 \
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-clickhouse -am \
  -Dtest=ClickHouseOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false \
  -Drat.skip=true -Dlicense.skip=true -Dmaven.javadoc.skip=true test
```

Connection settings are supplied through `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`,
`CLICKHOUSE_USER`, and optional `CLICKHOUSE_PASSWORD`. Applications use
`wayang.clickhouse.jdbc.*` properties instead.

## Notes

- **Pushdown is cost-gated.** On tiny tables Wayang's optimizer prefers a full
  scan + Java-side filter/projection, so pushdown only appears once a table is
  large enough (hence the test scales to 120k rows). Presto tests also assert
  that the expected SQL reached the server (`system.runtime.queries`).
- **Join.** A JDBC join is verified through the operator's SQL-clause contract
  executed on the platform, not the high-level `WayangContext` API alone — the
  logical `JoinOperator` emits `Tuple2<Record,Record>`, which cannot connect to a
  `Record` sink before the SQL pushdown flattens it.
- **Trailing semicolons.** Strict SQL parsers (Presto, Trino, BigQuery,
  ClickHouse JDBC in some modes) reject a trailing `;` in `executeQuery`, so
  `wayang-jdbc-template` does not emit one.
- **ClickHouse table sinks.** Overwrite mode emits
  `CREATE TABLE ... ENGINE = MergeTree ORDER BY tuple() AS SELECT ...`.
