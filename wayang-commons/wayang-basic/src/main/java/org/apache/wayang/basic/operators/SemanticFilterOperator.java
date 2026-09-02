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

import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

public class SemanticFilterOperator<T> extends UnaryToUnaryOperator<T, T> {
    private final String prompt; 

    public final Set<Object> targetModels;
    
    public SemanticFilterOperator(final DataSetType<T> type, final String prompt) {
        super(type, type, false);
        this.prompt = prompt;
        this.targetModels = null;
    }

    public SemanticFilterOperator(final DataSetType<T> type, final String prompt, final Set<Object> targetModels) {
        super(type, type, false);
        this.prompt = prompt;
        this.targetModels = targetModels;
    }

    public SemanticFilterOperator(final DataSetType<T> type) {
        super(type, type, false);
        this.prompt = "";
        this.targetModels = null;
    }

    public SemanticFilterOperator(final DataSetType<T> inputType, final DataSetType<T> outputType,
            final boolean isSupportingBroadcastInputs) {
        super(inputType, outputType, isSupportingBroadcastInputs);
        this.prompt = "";
        this.targetModels = null;
        }

    public SemanticFilterOperator(final UnaryToUnaryOperator<T, T> that) {
        super(that);
        this.prompt = "";
        this.targetModels = null;
    }

    public String getPrompt() {
        return prompt;
    }

    public void addTargetModel(final Object model) {
        targetModels.add(model);
    }
}
