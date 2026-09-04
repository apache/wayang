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

package org.apache.wayang.semantic.operators;

import java.util.Collection;

import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.semantic.execution.OllamaExecutor;
import org.apache.wayang.semantic.platform.OllamaPlatform;

/**
 * Execution operator for the {@link OllamaPlatform}.
 */
public interface OllamaExecutionOperator extends ExecutionOperator {

    @Override
    default OllamaPlatform getPlatform() {
        return OllamaPlatform.getInstance();
    }

    /**
     * Evaluates this operator by calling out to Ollama. Mirrors
     * {@code org.apache.wayang.java.operators.JavaExecutionOperator#evaluate}, just for the
     * {@link OllamaPlatform} instead of the Java platform.
     *
     * @param inputs          {@link ChannelInstance}s that satisfy the inputs of this operator
     * @param outputs         {@link ChannelInstance}s that collect the outputs of this operator
     * @param ollamaExecutor  that executes this instance
     * @param operatorContext optimization information for this instance
     * @return {@link Collection}s of what has been executed and produced
     */
    Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            OllamaExecutor ollamaExecutor,
            OptimizationContext.OperatorContext operatorContext);

}
