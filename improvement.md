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

# Presto engine-only integration test

## 1. What this branch demonstrates

The question this branch answers is **not** "does Presto execute some single
operator?" but:

> From `WayangContext.execute(...)` to the end of the whole Wayang plan, do all
> data processing **and** the final sink run inside Presto, **without** registering
> `Java.basicPlugin()`?

On this branch the answer is **yes**. `PrestoOperatorsIT`:

- registers **only** `Presto.plugin()` — no `Java.basicPlugin()`;
- ends **every** Wayang plan in a Presto `TableSink`, which compiles to a single
  `CREATE TABLE ... AS SELECT` executed inside Presto;
- after `WayangContext.execute(...)` returns, JUnit reads the result table with a
  plain JDBC query (assertion only — not part of the Wayang plan);
- handles the join `Tuple2<Record, Record>` vs flat `Record` mismatch with a
  test-only flatten mapping (see §4). This is a test-only scheme, not a final
  decision on Tuple-to-Record semantics for JDBC platforms.

This mirrors the Trino-only work on `wayang-trino-only-test`; the contrast is the
older mixed branch `wayang-presto`, which registered both `Java.basicPlugin()` and
`Presto.plugin()` and ended most operator tests in a Java `LocalCallbackSink`.

## 2. Execution shape

```text
Presto TableSource -> Presto operator(s) -> Presto TableSink
                                               |
                                               v
                                CREATE TABLE memory.wayang_it.operator_result AS SELECT ...

WayangContext.execute(...) returns
                                               |
                                               v
                            JUnit queries the result table over JDBC (assertions only)
```

The final JDBC query is part of the test only: it is not in the Wayang logical
plan, it is not a Wayang Java execution operator, and it does not process plan
data on Presto's behalf — it just inspects what Presto already wrote.

## 3. The shared executor change

All JDBC platforms share `wayang-jdbc-template`'s `JdbcExecutor`. When a stage's
terminal task is a `JdbcTableSinkOperator`, `JdbcExecutor.executeSinkStage(...)`
composes and runs the `CREATE TABLE ... AS SELECT` directly on the connection.

The previous Presto branch's `executeSinkStage` had two gaps that only surface
once **every** test ends in a `TableSink`:

1. It asserted a stage has a single source, so a join (orders + customers, two
   sources) could not be composed. It also lacked the `selectStartTask(...)` helper
   that picks the correct left/`FROM` table.
2. It only collected filter, projection and join; it threw `WayangException` for
   global reduce, reduce-by and sort, and passed `null` for them to
   `createSqlString(...)`.

This branch ports the engine-only `executeSinkStage` (identical to the file on
`wayang-trino-only-test`): it uses `selectStartTask(...)` for multi-source joins
and collects global reduce / reduce-by / sort, passing them into the existing
`createSqlString(...)`. The file is platform-agnostic. (Assertions are enabled
under Maven — `pom.xml` `enableAssertions=true` — so without this change a
join/reduce/sort sink would fail loudly, not silently.)

## 4. The join flatten mapping

A logical `JoinOperator` emits `Tuple2<Record, Record>`, while a pushed-down JDBC
join already emits a flat `Record`. The test wires an explicit flatten `MapOperator`
(named `JOIN_FLATTEN_NAME`) and registers a test-only `JoinFlattenMapping` on the
configuration whitelist; the mapping rewrites that named map into a
`PrestoProjectionOperator`, so the flatten is also pushed into Presto SQL and the
plan stays entirely in Presto. (Same approach as the Trino-only test, using
`PrestoProjectionOperator` + `PrestoPlatform`.)

## 5. Coverage and results

`PrestoOperatorsIT` runs 13 tests (8 operator-level + 5 high-level
`JavaPlanBuilder`) covering `TableSource`, `Filter`, `Projection`, `Join`,
`GlobalReduce`, `ReduceBy`, `Sort`, `TableSink`. Each composes a
`CREATE TABLE ... AS SELECT` and additionally asserts, via
`system.runtime.queries`, that the expected SQL actually reached Presto.

The high-level tests also rely on the `withSqlUdf` / `withSqlUdfs` additions to
`DataQuantaBuilder.scala` (ported from `wayang-trino-only-test`) so reduce / join /
sort builders can carry SQL implementations.

```bash
PRESTO_HOST=presto.example.com PRESTO_PORT=8080 PRESTO_USER=wayang \
JAVA_HOME=<jdk17> mvn test -pl wayang-platforms/wayang-presto -am \
  -Dtest=PrestoOperatorsIT -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
  -Drat.skip=true -Dlicense.skip=true -Pskip-prerequisite-check
```

Expected: `Tests run: 13, Failures: 0, Errors: 0, Skipped: 0`. The suite scales
its fixtures to 120k rows and creates/drops its own `memory.wayang_it` schema.
If Presto is unreachable the whole class is skipped (not failed).
