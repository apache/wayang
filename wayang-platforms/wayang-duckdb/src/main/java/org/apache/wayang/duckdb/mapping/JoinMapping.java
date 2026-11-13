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

package org.apache.wayang.duckdb.mapping;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.duckdb.operators.DuckDBJoinOperator;
import org.apache.wayang.duckdb.platform.DuckDBPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Maps a {@link JoinOperator} whose key descriptors are SQL-implementable to a
 * {@link DuckDBJoinOperator}.
 */
@SuppressWarnings("unchecked")
public class JoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                DuckDBPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        OperatorPattern<JoinOperator<Record, Record, Object>> operatorPattern = new OperatorPattern<>(
                "join",
                new JoinOperator<Record, Record, Object>(
                        null,
                        null,
                        DataSetType.createDefault(Record.class),
                        DataSetType.createDefault(Record.class)
                ),
                false
        )
            .withAdditionalTest(op -> op.getKeyDescriptor0() instanceof TransformationDescriptor)
            .withAdditionalTest(op -> op.getKeyDescriptor1() instanceof TransformationDescriptor)
            .withAdditionalTest(op -> op.getKeyDescriptor0().getSqlImplementation() != null)
            .withAdditionalTest(op -> op.getKeyDescriptor1().getSqlImplementation() != null);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<JoinOperator<Record, Record, Object>>(
                (matchedOperator, epoch) -> new DuckDBJoinOperator<Object>(matchedOperator).at(epoch)
        );
    }
}
