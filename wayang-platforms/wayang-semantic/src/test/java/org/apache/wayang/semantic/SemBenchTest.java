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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializablePredicate;
import org.apache.wayang.core.optimizer.costs.EstimationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfile;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.WayangArrays;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaCollectionSource;
import org.apache.wayang.java.operators.JavaDoWhileOperator;
import org.apache.wayang.java.operators.JavaLocalCallbackSink;
import org.apache.wayang.semantic.plugin.SemanticPlugin;
import org.apache.wayang.semantic.Semantic;
import org.apache.wayang.semantic.udf.SemanticAlgorithm;
import org.apache.wayang.semantic.operators.*;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.operators.SemanticFilterOperator;
import org.apache.wayang.java.operators.JavaMapOperator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SemBenchTest {
    private static List<Review> loadReviews() {
        return Arrays.asList(new Review("taken_1", "The movie was fantastic. Great acting and an engaging story."),
                new Review("taken_2", "I was disappointed. The plot was boring and too long."),
                new Review("taken_3", "Absolutely loved it! One of the best movies I have seen this year."),
                new Review("taken_3", "Terrible experience. I would not recommend it to anyone."),
                new Review("taken_4", "It was okay. Not great, not terrible."));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
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

        final SemanticAlgorithm ollamaFilter = new SemanticAlgorithm();
        ollamaFilter.impl = (input, prompt) -> {
            try {
                return OllamaSemanticFilter.isPositiveSentiment((Review) input);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Ollama call failed", e);
            }
        };
        ollamaFilter.loadProfileEstimator =
            LoadProfileEstimators.createFromSpecification(
                    "wayang.semantic.ollama.model1.load",
                    configuration
            );

        final SemanticAlgorithm ollamaFilter2 = new SemanticAlgorithm();
        ollamaFilter2.impl = (input, prompt) -> {
            try {
                return OllamaSemanticFilter.isPositiveSentiment2((Review) input);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Ollama call failed", e);
            }
        };
        ollamaFilter2.loadProfileEstimator =
            LoadProfileEstimators.createFromSpecification(
                    "wayang.semantic.ollama.model2.load",
                    configuration
            );

        final SemanticAlgorithm ollamaFilter3 = new SemanticAlgorithm();
        ollamaFilter3.impl = (input, prompt) -> {
            try {
                return OllamaSemanticFilter.isPositiveSentiment3((Review) input, (String) prompt);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Ollama call failed", e);
            }
        };
        ollamaFilter3.loadProfileEstimator =
            LoadProfileEstimators.createFromSpecification(
                    "wayang.semantic.ollama.model3.load",
                    configuration
            );

        final SemanticPlugin plugin = Semantic.plugin()
                .withOperatorMapping(SemanticFilterOperator.class, ollamaFilter)
                .withOperatorMapping(SemanticFilterOperator.class, ollamaFilter2)
                .withOperatorMapping(SemanticFilterOperator.class, ollamaFilter3);

        final WayangContext wayangContext = new WayangContext()
                .withPlugin(Java.basicPlugin())
                .withPlugin(plugin);
        final JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext);

        final Collection<Long> positiveReviewCnt = planBuilder.loadCollection(loadReviews())
                .filter(review -> "taken_3".equals(review.getId()))
                .semanticFilter("Analyze the review after the | and write either \"POSITIVE\" if the review has a positive sentiment and \"NEGATIVE\" if the review has a negative sentiment.")
                    .withTargetModels(ollamaFilter, ollamaFilter2, ollamaFilter3)
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