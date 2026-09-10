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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.wayang.basic.operators.SemanticFilterOperator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.StreamChannel;

/**
 * Base for {@link OllamaExecutionOperator}s that implement a {@link SemanticFilterOperator} by calling
 * out to one specific model. Every concrete subclass hardcodes exactly one model's evaluation logic
 * instead of taking it as an injected UDF, so {@link #getPrompt()} (inherited from the matched logical
 * {@link SemanticFilterOperator}) remains the single source of truth for the prompt; a subclass is free
 * to use it or to ignore it in favor of its own hardcoded behavior.
 * <p>
 * Input/output channels are the same {@link StreamChannel}/{@link CollectionChannel} that
 * {@code JavaFilterOperator} supports, so data can flow between the surrounding Java pipeline and an
 * Ollama-backed operator without any channel conversion.
 */
public abstract class OllamaFilterOperator<Type> extends SemanticFilterOperator<Type> implements OllamaExecutionOperator {

    protected OllamaFilterOperator(final DataSetType<Type> type, final String prompt) {
        super(type, prompt);
    }

    /**
     * Creates a new instance of the same concrete subclass for the given type/prompt.
     */
    protected abstract OllamaFilterOperator<Type> newInstance(DataSetType<Type> type, String prompt);

    @Override
    protected final ExecutionOperator createCopy() {
        return this.newInstance(this.getInputType(), this.getPrompt());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        if (this.getInput(index).isBroadcast()) return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
