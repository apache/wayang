<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0.
  You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0.
-->

# Wayang Platform ClickHouse

This module connects Wayang to a user-managed ClickHouse deployment through JDBC.
Configure the endpoint and credentials in a Wayang properties file:

```properties
wayang.clickhouse.jdbc.url = jdbc:clickhouse://clickhouse.example.com:8123/default
wayang.clickhouse.jdbc.user = default
wayang.clickhouse.jdbc.password =
# Optional ENGINE clause for CREATE TABLE ... AS SELECT (overwrite sinks)
wayang.clickhouse.tablesink.engine = MergeTree ORDER BY tuple()
```

The runnable filter/projection example and its table requirements are documented
in [`wayang-applications/clickhouse.md`](../../wayang-applications/clickhouse.md).

## Notes

- Table sinks in `overwrite` mode emit
  `CREATE TABLE <t> ENGINE = <engine> AS SELECT ...` because ClickHouse requires
  an engine. Override the engine via `wayang.clickhouse.tablesink.engine` or the
  same key on the `TableSink` `Properties`.
- ClickHouse identifiers are case-sensitive. Prefer `database.table` names.
- Joins build the hash table from the right input; nullable join behaviour
  follows the server's `join_use_nulls` setting (default `0`).

## Integration tests

`ClickHouseOperatorsIT` is intended for connector development. It creates and
removes its own fixture tables in the `wayang_it` database. If ClickHouse is
unreachable the class is skipped (not failed).

```bash
CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=8123 CLICKHOUSE_USER=default \
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-clickhouse -am \
  -Dtest=ClickHouseOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false test
```

These environment variables configure the integration test only. Applications
use the `wayang.clickhouse.jdbc.*` properties described above.
