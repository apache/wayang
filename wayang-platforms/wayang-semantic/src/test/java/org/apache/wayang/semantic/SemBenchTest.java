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

package org.apache.wayang.semantic;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.operators.SemanticFilterOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.semantic.execution.OllamaExecutor;
import org.apache.wayang.semantic.operators.OllamaFilterOperator;
import org.apache.wayang.semantic.platform.OllamaPlatform;
import org.apache.wayang.semantic.plugin.SemanticPlugin;
import org.junit.jupiter.api.Test;

class SemBenchTest {
    private static List<Review> loadReviews() {
        return Arrays.asList(new Review("taken_1", "The movie was fantastic. Great acting and an engaging story."),
                new Review("taken_2", "I was disappointed. The plot was boring and too long."),
                new Review("taken_3", "Absolutely loved it! One of the best movies I have seen this year."),
                new Review("taken_3", "Terrible experience. I would not recommend it to anyone."),
                new Review("taken_4", "It was okay. Not great, not terrible."));
    }

    @Test
    void testSemBenchMoviesWithOllama() {
        final Configuration configuration = new Configuration();
        configuration.setProperty("wayang.java.filter.load", """
            {
            "in":1,
            "out":1,
            "cpu":"${25*in0 + 350000}",
            "ram":"100000",
            "p":0.9
            }
            """
        );
        configuration.setProperty("wayang.semantic.ollama.model1.load",
            """
            {
            "in": 1,
            "out": 1,
            "cpu": "${500*in0 + 56789}",
            "ram": "10000",
            "disk": "0",
            "net": "0",
            "p": 0.9,
            "overhead": 0,
            "ru": "${wayang:logGrowth(0.1, 0.1, 1000000, in0)}"
            }
            """);
        configuration.setProperty("wayang.semantic.ollama.model2.load",
            """
            {
            "in": 1,
            "out": 1,
            "cpu": "${500*in0 + 56789}",
            "ram": "10000",
            "disk": "0",
            "net": "0",
            "p": 0.9,
            "overhead": 0,
            "ru": "${wayang:logGrowth(0.1, 0.1, 1000000, in0)}"
            }
            """);
        configuration.setProperty("wayang.semantic.ollama.model3.load",
            """
            {
            "in": 1,
            "out": 1,
            "cpu": "${50*in0 + 5678}",
            "ram": "1000",
            "disk": "0",
            "net": "0",
            "p": 0.9,
            "overhead": 0,
            "ru": "${wayang:logGrowth(0.1, 0.1, 1000000, in0)}"
            }
            """
        );

        // Each Ollama model is its own physical operator + Mapping; the optimizer picks among them by cost.
        // There is exactly one prompt, owned by the logical SemanticFilterOperator (set via .semanticFilter(...)),
        // and each physical operator decides for itself whether it needs that prompt.
        final SemanticPlugin plugin = Semantic.plugin()
                .withMapping(new OllamaModel1FilterMapping())
                .withMapping(new OllamaModel2FilterMapping())
                .withMapping(new OllamaModel3FilterMapping());

        final WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(plugin);
        final JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext);

        final Collection<Long> positiveReviewCnt = planBuilder.loadCollection(loadReviews())
                .filter(review -> "taken_3".equals(review.getId()))
                .semanticFilter("Analyze the review after the | and write either \"POSITIVE\" if the review has a positive sentiment and \"NEGATIVE\" if the review has a negative sentiment.")
                    .withTargetModels(OllamaModel1FilterOperator.class, OllamaModel2FilterOperator.class, OllamaModel3FilterOperator.class)
                .count()
                .collect();
    }
}

class Review {
    private final String id;
    private final String reviewText;

    public Review(final String id, final String reviewText) {
        this.id = id;
        this.reviewText = reviewText;
    }

    public String getId() {
        return id;
    }

    public String getReviewText() {
        return reviewText;
    }

    @Override
    public String toString() {
        return "Review{id='" + id + "', reviewText='" + reviewText + "'}";
    }
}

/**
 * Physical operator for the "model1" Ollama implementation: a fixed sentiment prompt, ignores the
 * logical operator's {@link #getPrompt()}.
 */
final class OllamaModel1FilterOperator<Type> extends OllamaFilterOperator<Type> {

    OllamaModel1FilterOperator(final DataSetType<Type> type, final String prompt) {
        super(type, prompt);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final OllamaExecutor ollamaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Stream<Review> filtered = ((JavaChannelInstance) inputs[0]).<Review>provideStream().filter(review -> {
            try {
                return OllamaSemanticFilter.isPositiveSentiment(review);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Ollama call failed", e);
            }
        });
        ((StreamChannel.Instance) outputs[0]).accept(filtered);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.semantic.ollama.model1.load";
    }

    @Override
    protected OllamaFilterOperator<Type> newInstance(final DataSetType<Type> type, final String prompt) {
        return new OllamaModel1FilterOperator<>(type, prompt);
    }
}

/**
 * Physical operator for the "model2" Ollama implementation: a different fixed sentiment prompt, also
 * ignores the logical operator's {@link #getPrompt()}.
 */
final class OllamaModel2FilterOperator<Type> extends OllamaFilterOperator<Type> {

    OllamaModel2FilterOperator(final DataSetType<Type> type, final String prompt) {
        super(type, prompt);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final OllamaExecutor ollamaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Stream<Review> filtered = ((JavaChannelInstance) inputs[0]).<Review>provideStream().filter(review -> {
            try {
                return OllamaSemanticFilter.isPositiveSentiment2(review);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Ollama call failed", e);
            }
        });
        ((StreamChannel.Instance) outputs[0]).accept(filtered);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.semantic.ollama.model2.load";
    }

    @Override
    protected OllamaFilterOperator<Type> newInstance(final DataSetType<Type> type, final String prompt) {
        return new OllamaModel2FilterOperator<>(type, prompt);
    }
}

/**
 * Physical operator for the "model3" Ollama implementation: the only one that actually uses the
 * logical operator's {@link #getPrompt()}.
 */
final class OllamaModel3FilterOperator<Type> extends OllamaFilterOperator<Type> {

    OllamaModel3FilterOperator(final DataSetType<Type> type, final String prompt) {
        super(type, prompt);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final OllamaExecutor ollamaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Stream<Review> filtered = ((JavaChannelInstance) inputs[0]).<Review>provideStream().filter(review -> {
            try {
                return OllamaSemanticFilter.isPositiveSentiment3(review, this.getPrompt());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Ollama call failed", e);
            }
        });
        ((StreamChannel.Instance) outputs[0]).accept(filtered);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.semantic.ollama.model3.load";
    }

    @Override
    protected OllamaFilterOperator<Type> newInstance(final DataSetType<Type> type, final String prompt) {
        return new OllamaModel3FilterOperator<>(type, prompt);
    }
}

abstract class AbstractOllamaFilterMapping implements Mapping {

    private final Class<? extends OllamaFilterOperator> targetOperatorClass;

    AbstractOllamaFilterMapping(final Class<? extends OllamaFilterOperator> targetOperatorClass) {
        this.targetOperatorClass = targetOperatorClass;
    }

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(),
                this.createReplacementSubplanFactory(), OllamaPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        return SubplanPattern.createSingleton(new OperatorPattern<SemanticFilterOperator<?>>("semantic_filter",
                new SemanticFilterOperator<>(DataSetType.NONE), false)
                        .withAdditionalTest(op -> op.getTargetModels() != null)
                        .withAdditionalTest(op -> op.getTargetModels().contains(this.targetOperatorClass)));
    }

    protected abstract <I> OllamaFilterOperator<I> createOperator(DataSetType<I> type, String prompt);

    private <I> ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SemanticFilterOperator<I>>((matchedOperator, epoch) ->
                this.<I>createOperator(matchedOperator.getInputType(), matchedOperator.getPrompt()).at(epoch));
    }
}

final class OllamaModel1FilterMapping extends AbstractOllamaFilterMapping {
    OllamaModel1FilterMapping() {
        super(OllamaModel1FilterOperator.class);
    }

    @Override
    protected <I> OllamaFilterOperator<I> createOperator(final DataSetType<I> type, final String prompt) {
        return new OllamaModel1FilterOperator<>(type, prompt);
    }
}

final class OllamaModel2FilterMapping extends AbstractOllamaFilterMapping {
    OllamaModel2FilterMapping() {
        super(OllamaModel2FilterOperator.class);
    }

    @Override
    protected <I> OllamaFilterOperator<I> createOperator(final DataSetType<I> type, final String prompt) {
        return new OllamaModel2FilterOperator<>(type, prompt);
    }
}

final class OllamaModel3FilterMapping extends AbstractOllamaFilterMapping {
    OllamaModel3FilterMapping() {
        super(OllamaModel3FilterOperator.class);
    }

    @Override
    protected <I> OllamaFilterOperator<I> createOperator(final DataSetType<I> type, final String prompt) {
        return new OllamaModel3FilterOperator<>(type, prompt);
    }
}

final class OllamaSemanticFilter {
    private static final String OLLAMA_API_URL = "http://apache-wayang-ollama:11434/api/generate";
    private static final String MODEL_NAME = "tinyllama";
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    public static boolean isPositiveSentiment(final Review review) throws IOException, InterruptedException {
        final String prompt = String.format("Analyze the sentiment of this movie review. "
                + "Reply with only 'POSITIVE' or 'NEGATIVE'.\n\n" + "Review: %s", review.getReviewText());
        final String response = callOllama(prompt);
        return response.contains("POSITIVE");
    }

    public static boolean isPositiveSentiment2(final Review review) throws IOException, InterruptedException {
        final String prompt = String.format(
                "Analyze the sentiment of this movie review, words like love, fantastic and great are positive modifiers. "
                        + "Reply with only 'POSITIVE' or 'NEGATIVE'.\n\n" + "Review: %s",
                review.getReviewText());
        final String response = callOllama(prompt);
        return response.contains("POSITIVE");
    }

    public static boolean isPositiveSentiment3(final Review review, final String prompt)
            throws IOException, InterruptedException {
        final String response = callOllama(prompt + " | " + review.getReviewText());
        return response.contains("POSITIVE");
    }

    private static String callOllama(final String prompt) throws IOException, InterruptedException {
        final String requestBody = String.format("{\"model\": \"%s\", \"prompt\": \"%s\", \"stream\": false}",
                MODEL_NAME, escapeJson(prompt));

        final HttpRequest request = HttpRequest.newBuilder().uri(URI.create(OLLAMA_API_URL))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofMinutes(2)).build();

        final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Ollama API error: " + response.body());
        }

        return parseOllamaResponse(response.body());
    }

    private static String parseOllamaResponse(final String jsonResponse) {
        int startIdx = jsonResponse.indexOf("\"response\":\"");

        if (startIdx == -1)
            return "";

        startIdx += "\"response\":\"".length();

        final int endIdx = jsonResponse.indexOf("\"", startIdx);

        return jsonResponse.substring(startIdx, endIdx);
    }

    private static String escapeJson(final String str) {
        return str.replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
}
