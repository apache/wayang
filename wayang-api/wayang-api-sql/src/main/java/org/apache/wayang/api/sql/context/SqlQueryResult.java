/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.context;

import org.apache.wayang.basic.data.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Rows and result metadata produced by the Wayang SQL API.
 */
public class SqlQueryResult {

    private final List<SqlColumn> columns;

    private final Collection<Record> rows;

    public SqlQueryResult(final List<SqlColumn> columns, final Collection<Record> rows) {
        this.columns = List.copyOf(columns);
        this.rows = new ArrayList<>(rows);
    }

    public List<SqlColumn> getColumns() {
        return this.columns;
    }

    public Collection<Record> getRows() {
        return new ArrayList<>(this.rows);
    }
}
