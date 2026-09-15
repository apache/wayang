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
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.jdbc.operators.JdbcReduceByOperator;

/**
 * DuckDB implementation of the {@link ReduceByOperator}. The grouping key and
 * the reduction are pushed down as a SQL {@code GROUP BY} plus aggregate (e.g.
 * {@code SELECT region, SUM(amount) ... GROUP BY region}).
 */
public class DuckDBReduceByOperator extends JdbcReduceByOperator implements DuckDBExecutionOperator {

    public DuckDBReduceByOperator(TransformationDescriptor<Record, Record> keyDescriptor,
                                  ReduceDescriptor<Record> reduceDescriptor) {
        super(keyDescriptor, reduceDescriptor);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public DuckDBReduceByOperator(ReduceByOperator<Record, Record> that) {
        super(that);
    }

    @Override
    protected DuckDBReduceByOperator createCopy() {
        return new DuckDBReduceByOperator(this);
    }
}
