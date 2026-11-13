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

package org.apache.wayang.clickhouse.mapping;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.clickhouse.operators.ClickHouseSortOperator;
import org.apache.wayang.clickhouse.platform.ClickHousePlatform;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SortOperator} to {@link ClickHouseSortOperator}.
 */
@SuppressWarnings("unchecked")
public class SortMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                ClickHousePlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern<SortOperator<Record, Record>> operatorPattern = new OperatorPattern<>(
                "sort",
                new SortOperator<Record, Record>(null, DataSetType.createDefault(Record.class)),
                false)
                .withAdditionalTest(op -> op.getKeyDescriptor().getSqlImplementation() != null);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SortOperator<Record, Record>>(
                (matchedOperator, epoch) -> new ClickHouseSortOperator(matchedOperator).at(epoch)
        );
    }
}
