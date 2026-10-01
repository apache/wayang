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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.util.List;

/**
 * Marks JDBC operators that can start a SQL stage and provide a relation for a
 * {@code FROM} clause.
 */
public interface JdbcSourceOperator extends JdbcExecutionOperator {

    /**
     * Name or expression used to identify this source in SQL metadata, e.g., in
     * join descriptors.
     */
    String getSourceName();

    /**
     * Name or expression used for this source under the given configuration.
     */
    default String getSourceName(Configuration configuration) {
        return this.getSourceName();
    }

    /**
     * Creates a SQL clause for this source under the given configuration.
     */
    default String createSqlClause(Connection connection, FunctionCompiler compiler, Configuration configuration) {
        return this.createSqlClause(connection, compiler);
    }

    /**
     * Prepares this source for SQL generation, e.g., by registering a temporary
     * relation. Implementations can keep this as a no-op when no preparation is
     * required.
     */
    default void prepareSource(Connection connection, FunctionCompiler compiler, Configuration configuration) {
    }

    @Override
    default List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("JDBC source operators have no input channels.");
    }
}
