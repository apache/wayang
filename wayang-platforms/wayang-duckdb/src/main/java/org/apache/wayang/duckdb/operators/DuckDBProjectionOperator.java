/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.duckdb.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.jdbc.operators.JdbcProjectionOperator;

/**
 * DuckDB implementation of a column projection. The selected fields are pushed
 * down as the SQL {@code SELECT} list.
 */
public class DuckDBProjectionOperator extends JdbcProjectionOperator implements DuckDBExecutionOperator {

    public DuckDBProjectionOperator(String... fieldNames) {
        super(fieldNames);
    }

    public DuckDBProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    public DuckDBProjectionOperator(MapOperator<Record, Record> that) {
        super(that);
    }

    @Override
    protected DuckDBProjectionOperator createCopy() {
        return new DuckDBProjectionOperator(this);
    }

}
