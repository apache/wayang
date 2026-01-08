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

import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;

import java.sql.Connection;
import java.util.Optional;

/**
 * PostgreSQL implementation for the {@link JoinOperator}.
 */
public abstract class JdbcJoinOperator<KeyType>
    extends JoinOperator<Record, Record, KeyType>
    implements JdbcExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @see JoinOperator#JoinOperator(Record, Record...)
     */
    public JdbcJoinOperator(
        TransformationDescriptor<Record, KeyType> keyDescriptor0,
        TransformationDescriptor<Record, KeyType> keyDescriptor1
    ) {
        super(
            keyDescriptor0,
            keyDescriptor1,
            DataSetType.createDefault(Record.class),
            DataSetType.createDefault(Record.class)
        );
    }

    /**
     * Copies an instance
     *
     * @param that that should be copied
     */
    public JdbcJoinOperator(JoinOperator<Record, Record, KeyType> that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        final Tuple<String, String> left = this.keyDescriptor0.getSqlImplementation();
        final Tuple<String, String> right = this.keyDescriptor1.getSqlImplementation();
        final String leftTableName = left.field0;
        final String leftKey = left.field1;
        final String rightTableName = right.field0;
        final String rightKey = right.field1;

        return "JOIN " + rightTableName + " ON " +
            rightTableName + "." + rightKey
            + "=" + leftTableName + "." + leftKey;
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.join.load", this.getPlatform().getPlatformId());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JdbcExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor0, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor1, configuration);
        return optEstimator;
    }
}
