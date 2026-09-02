<!--

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

-->

# Developing with Semantic Operators in Apache Wayang

This guide explains how to define semantic operators, provide executable implementations, register multiple implementations, estimate their costs, and let Apache Wayang select an implementation during optimization.

The example uses a semantic filter that classifies movie reviews as positive or negative through Ollama.

## 1. Semantic operators

A semantic operator is an operator much like any Wayang operator, however, it takes a prompt as input,
that describes how it should act.

For example:

```java
.semanticFilter(
    "Analyze the review after the | and write either "
        + "\"POSITIVE\" if the review has a positive sentiment and "
        + "\"NEGATIVE\" if the review has a negative sentiment."
)
```

The prompt describes the task. A `SemanticAlgorithm` provides one concrete implementation of that task.
A `SemanticAlgorithm` could theoretically be any UDF you desire, there are no strict requirements on its implementation. 
The implementation only that the requires the UDF is implemented as something that takes an input `Record` and a `prompt` and outputs
whatever datatype is required by the operator.

The Ollama local open-source model is used as an example for this guide, but may also be useful for quick development.

We set up our local model hosting locally using Docker:

```yaml
ollama:
    image: ollama/ollama:latest
    container_name: apache-wayang-ollama
    ports:
        - "11434:11434"
    volumes:
        - ollama-data:/root/.ollama
        - ./docker/ollama-init.sh:/ollama-init.sh
    entrypoint: ["/bin/bash", "/ollama-init.sh"]
    restart: always
    tty: true
    networks:
        - wayang-network
```

```sh
#!/bin/bash
ollama serve &
sleep 10
ollama pull tinyllama
wait
```

We setup the backend call to the model in Wayang:

```java
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
```

and the semantic UDF:

```java
public static boolean isPositiveSentiment(final Review review, final String prompt)
        throws IOException, InterruptedException {
    final String response = callOllama(prompt + " | " + review.getReviewText());
    return response.contains("POSITIVE");
}
```

Now we can define our `SemanticAlgorithm`:

```java
final SemanticAlgorithm ollamaFilter = new SemanticAlgorithm();
ollamaFilter.impl = (input, prompt) -> {
    try {
        return isPositiveSentiment((Review) input);
    } catch (IOException | InterruptedException e) {
        throw new RuntimeException("Ollama call failed", e);
    }
};
```

And also provide a UDF load estimator:

```java
ollamaFilter.loadProfileEstimator =
    LoadProfileEstimators.createFromSpecification(
            "wayang.semantic.ollama.model1.load",
            configuration
    );
```

Note that you should provide a separate configuration for each semantic operator.
Now we register these UDFs with the `SemanticPlugin` this automatically constructs a new operator
per UDF. Please note this may have implications for optimization time depending on your setup.

```java
final SemanticPlugin plugin = Semantic.plugin()
        .withOperatorMapping(SemanticFilterOperator.class, ollamaFilter)
        .withOperatorMapping(SemanticFilterOperator.class, ollamaFilter2)
        .withOperatorMapping(SemanticFilterOperator.class, ollamaFilter3);

final WayangContext wayangContext = new WayangContext()
        .withPlugin(Java.basicPlugin())
        .withPlugin(plugin);
```

Currently, we only have mappings for semantic operators in Java, so you also need the `Java.basicPlugin()`.
Finally, you can construct your Wayang plan:

```java
final Collection<Long> positiveReviewCnt = planBuilder.loadCollection(loadReviews())
        .filter(review -> "taken_3".equals(review.getId()))
        .semanticFilter("Analyze the review after the | and write either \"POSITIVE\" if the review has a positive sentiment and \"NEGATIVE\" if the review has a negative sentiment.")
            .withTargetModels(ollamaFilter, ollamaFilter2, ollamaFilter3)
        .count()
        .collect();
```

You need to provide semantic operators with their target models, even if you plan to use all models you've constructed.

