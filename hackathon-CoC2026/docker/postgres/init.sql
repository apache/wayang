-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Seed table used by examples/PostgresSmokeTest.java to prove the Postgres
-- platform is reachable and queryable from inside the hackathon container.
-- Runs automatically the first time the postgres data volume is created
-- (standard postgres image behavior for /docker-entrypoint-initdb.d).

CREATE TABLE IF NOT EXISTS word_counts_seed (
    word  VARCHAR(64) NOT NULL,
    count INTEGER     NOT NULL
);

INSERT INTO word_counts_seed (word, count) VALUES
    ('wayang',   42),
    ('spark',    17),
    ('postgres', 9),
    ('hackathon',5),
    ('glasgow',  3);
