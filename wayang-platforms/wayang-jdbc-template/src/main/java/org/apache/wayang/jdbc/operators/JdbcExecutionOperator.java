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

package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

public interface JdbcExecutionOperator extends ExecutionOperator {

    /**
     * Creates a SQL clause for this instance. For {@link TableSource}s it returns an identifier for the table
     * usable in a {@code FROM} clause. For {@link JdbcProjectionOperator}s it returns a list usable in a
     * {@code SELECT} clause. For {@link JdbcFilterOperator}s it creates a condition usable in a {@code WHERE} clause.
     * For {@link JdbcJoinOperator} it returns a INNER JOIN statement usable in a {@code JOIN} clause.
     * Also, these different clauses should be compatible for connected {@link JdbcExecutionOperator}s.
     *
     * @param compiler used to create SQL code
     * @return the SQL clause
     */
    String createSqlClause(Connection connection, FunctionCompiler compiler);

    /**
     * Creates a SQL clause under the given configuration.
     */
    default String createSqlClause(Connection connection, FunctionCompiler compiler, Configuration configuration) {
        return this.createSqlClause(connection, compiler);
    }

    @Override
    JdbcPlatformTemplate getPlatform();

    @Override
    default List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(this.getPlatform().getSqlQueryChannelDescriptor());
    }

    @Override
    default List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(this.getPlatform().getSqlQueryChannelDescriptor());
    }

}
