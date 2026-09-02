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

package org.apache.wayang.semantic.mappings;

import java.util.Collection;
import java.util.Collections;

import org.apache.wayang.basic.operators.SemanticFilterOperator;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializablePredicate;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.operators.JavaFilterOperator;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.semantic.udf.SemanticAlgorithm;

public class JavaFilterMapping implements Mapping {
    private final SemanticAlgorithm<Object, Boolean> model;

    public JavaFilterMapping(final SemanticAlgorithm<Object, Boolean> model) {
        this.model = model;
    }

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(),
                this.createReplacementSubplanFactory(), JavaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        return SubplanPattern.createSingleton(new OperatorPattern<SemanticFilterOperator<?>>("semantic_filter",
                new SemanticFilterOperator<>(DataSetType.NONE), false).withAdditionalTest(op -> op.targetModels != null)
                        .withAdditionalTest(op -> op.targetModels.contains(model)));
    }

    private <I> ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SemanticFilterOperator<I>>((matchedOperator, epoch) -> {
            final SerializablePredicate<I> predicate = input -> model.impl.apply(input, matchedOperator.getPrompt());
            final PredicateDescriptor<I> predicateDescriptor = new PredicateDescriptor<>(predicate,
                    matchedOperator.getOutput().getType().getDataUnitType().getTypeClass(), model.loadProfileEstimator);
            return new JavaFilterOperator<>(predicateDescriptor).at(epoch);
        });
    }
}
