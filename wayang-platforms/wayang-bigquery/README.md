<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0.
  You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0.
-->

# Wayang Platform BigQuery

This module connects Wayang to a user-managed Google BigQuery project through
JDBC. Supply the project and authentication settings in the JDBC URL; the
example format is documented in `wayang-bigquery-defaults.properties`.

```properties
wayang.bigquery.jdbc.url = jdbc:bigquery://https://www.googleapis.com/bigquery/v2;ProjectId=my-project;OAuthType=0;OAuthServiceAcctEmail=service-account@example.com;OAuthPvtKeyPath=/path/to/key.json
wayang.bigquery.jdbc.user =
wayang.bigquery.jdbc.password =
```

The runnable filter/projection example and its table requirements are documented
in [`wayang-applications/bigquery.md`](../../wayang-applications/bigquery.md).

## Integration tests

`BigQueryOperatorsIT` is intended for connector development. The service
account must be able to create query jobs and create, read, and delete tables in
the selected dataset. The test creates its fixtures and removes their tables
afterward; it leaves the dataset in place.

Configure the test with environment variables and run it from the repository
root:

```bash
BIGQUERY_PROJECT=my-project \
BIGQUERY_SA_EMAIL=wayang-bq@my-project.iam.gserviceaccount.com \
BIGQUERY_KEY_PATH=/absolute/path/to/key.json \
BIGQUERY_DATASET=wayang_it \
BIGQUERY_LOCATION=US \
./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-bigquery -am \
  -Dtest=BigQueryOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
  -DfailIfNoTests=false test
```

The equivalent system properties are `bigquery.project`, `bigquery.saEmail`,
`bigquery.keyPath`, `bigquery.dataset`, and `bigquery.location`. These settings
configure the integration test only; applications use the
`wayang.bigquery.jdbc.*` properties described above.

Cost calibration uses `BigQueryCostPilotIT`; see
[`guides/cost-profiling.md`](../../guides/cost-profiling.md).
