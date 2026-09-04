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

package org.apache.wayang.basic.operators;

import java.util.Set;

/**
 * Common contract for operators whose UDF is not plain Java code but a natural-language
 * {@code prompt} that gets executed by a large language model (LLM), e.g. a
 * {@link SemanticFilterOperator} or a future {@code SemanticJoinOperator}.
 * <p>
 * This is deliberately kept as a slim interface rather than an abstract base class. Semantic
 * operators still need to pick the operator shape that fits their arity/semantics, e.g. a
 * filter extends {@link org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator} while a
 * join would extend {@link org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator}. As
 * Java only allows single class inheritance, tying this abstraction to a specific operator
 * base class would force every semantic operator into the same shape. Implementors are free
 * to (and are expected to) extend whichever operator base class matches their actual
 * input/output arity, while still sharing the prompt/target-model contract defined here.
 */
public interface SemanticOperator {

    /**
     * @return the prompt describing the semantic algorithm that this operator should execute
     */
    String getPrompt();

    /**
     * @return the set of models that are allowed to execute this operator's {@link #getPrompt()},
     * or {@code null} if no target models have been configured
     */
    Set<Object> getTargetModels();

    /**
     * Adds a model to the set of models that are allowed to execute this operator's
     * {@link #getPrompt()}.
     *
     * @param model the model to add
     */
    void addTargetModel(Object model);
}
