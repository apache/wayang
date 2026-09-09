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

package org.apache.wayang.ml.encoding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Junction;

public class TreeEncoder {

    public static final String ENCODE_IDS_PROPERTY = "wayang.ml.encoding.encode-ids";

    private final OneHotMappings mappings;

    private Configuration configuration;

    public TreeEncoder(final OneHotMappings mappings) {
        this(mappings, null);
    }

    public TreeEncoder(final OneHotMappings mappings, final Configuration configuration) {
        this.mappings = mappings;
        this.configuration = configuration;
    }

    public OneHotMappings getMappings() {
        return this.mappings;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public void setConfiguration(final Configuration configuration) {
        this.configuration = configuration;
    }

    private boolean resolveEncodeIds(final OptimizationContext optimizationContext) {
        final Configuration config = this.configuration != null
                ? this.configuration
                : (optimizationContext != null ? optimizationContext.getConfiguration() : null);
        if (config != null) {
            return config.getOptionalBooleanProperty(ENCODE_IDS_PROPERTY)
                    .orElseGet(() -> config.getBooleanProperty("wayang.ml.encode-ids", false));
        }
        return false;
    }

    public TreeNode encode(final PlanImplementation plan) {
        return this.encode(plan, this.resolveEncodeIds(plan.getOptimizationContext()));
    }

    public TreeNode encode(final PlanImplementation plan, final boolean encodeIds) {
        final List<TreeNode> result = new ArrayList<TreeNode>();

        final HashMap<Operator, Collection<Operator>> tree = new HashMap<>();
        final List<ExecutionOperator> sinks = plan.getOperators().stream().filter(Operator::isSink).toList();

        final Map<OutputSlot<?>, Junction> junctions = plan.getJunctions();

        for (final Operator sink : sinks) {
            final TreeNode sinkNode = traversePIOperator(sink, plan.getOptimizationContext(), encodeIds, junctions,
                    tree);
            result.add(sinkNode);
        }

        if (result.size() == 0) {
            return null;
        }

        final TreeNode resultNode = result.get(0);
        resultNode.rebalance();

        return resultNode;
    }

    public TreeNode encode(final WayangPlan plan, final OptimizationContext optimizationContext) {
        return this.encode(plan, optimizationContext, this.resolveEncodeIds(optimizationContext));
    }

    public TreeNode encode(final WayangPlan plan, final OptimizationContext optimizationContext,
            final boolean encodeIds) {
        final List<TreeNode> result = new ArrayList<TreeNode>();
        plan.prune();

        final HashMap<Operator, Collection<Operator>> tree = new HashMap<>();
        final Collection<Operator> sinks = plan.getSinks();

        for (final Operator sink : sinks) {
            final TreeNode sinkNode = traverse(sink, tree, optimizationContext, encodeIds);
            result.add(sinkNode);
        }

        if (result.size() == 0) {
            return null;
        }

        assert result.size() == 1 : "result size was not 1";

        final TreeNode resultNode = result.get(0);

        // rebalance to make it a guaranteed binary tree
        resultNode.rebalance();

        return resultNode;
    }

    private TreeNode traversePIOperator(final Operator current, final OptimizationContext optimizationContext,
            final boolean encodeIds, final Map<OutputSlot<?>, Junction> junctions,
            final HashMap<Operator, Collection<Operator>> visited) {
        if (visited.containsKey(current)) {
            return null;
        }

        final TreeNode currentNode = new TreeNode();

        if (current.isAlternative()) {
            final Operator original = ((OperatorAlternative) current).getAlternatives().get(0).getContainedOperators()
                    .stream().findFirst()
                    .orElseThrow(() -> new WayangException("Operator could not be retrieved from Alternatives"));
            mappings.addOriginalOperator(original);

            currentNode.encoded = OneHotEncoder.encodeOperator(original, optimizationContext, encodeIds);
        } else {
            mappings.addOriginalOperator(current);

            if (current.isExecutionOperator()) {
                currentNode.encoded = OneHotEncoder.encodeOperator((ExecutionOperator) current, optimizationContext,
                        encodeIds);
            } else {
                currentNode.encoded = OneHotEncoder.encodeOperator(current, optimizationContext, encodeIds);
            }
        }

        final Collection<Junction> currentJunctions = junctions.values().stream().filter(junction -> {
            for (final InputSlot<?> input : current.getAllInputs()) {
                if (junction.getTargetInputs().contains(input)) {
                    return true;
                }
            }

            return false;
        }).toList();

        final Collection<ExecutionOperator> inputs = currentJunctions.stream().map(Junction::getSourceOperator)
                .toList();

        for (final Operator input : inputs) {
            TreeNode next;
            final Collection<ExecutionTask> conversions = currentJunctions.stream()
                    .filter(junction -> junction.getSourceOperator() == input)
                    .flatMap(junction -> junction.getConversionTasks().stream()).toList();

            // fit conversions in between current and its inputs
            if (conversions.size() > 0) {
                final Queue<ExecutionTask> conversionQueue = new LinkedList<>();
                conversionQueue.addAll(conversions);

                next = traverseWithNext(conversionQueue, junctions, visited, input, optimizationContext, encodeIds);
            } else {
                next = traversePIOperator(input, optimizationContext, encodeIds, junctions, visited);
            }

            if (currentNode.left == null) {
                currentNode.left = next;
            } else {
                currentNode.right = next;
            }
        }

        return currentNode;
    }

    private TreeNode traverseWithNext(final Queue<ExecutionTask> conversions,
            final Map<OutputSlot<?>, Junction> junctions, final HashMap<Operator, Collection<Operator>> visited,
            final Operator next, final OptimizationContext optimizationContext, final boolean encodeIds) {
        if (visited.containsKey(next)) {
            return null;
        }

        if (conversions.isEmpty()) {
            return traversePIOperator(next, optimizationContext, encodeIds, junctions, visited);
        }

        final ExecutionTask currentTask = conversions.poll();
        final ExecutionOperator current = currentTask.getOperator();
        final TreeNode currentNode = new TreeNode();

        if (current.isAlternative()) {
            final Operator original = ((OperatorAlternative) current).getAlternatives().get(0).getContainedOperators()
                    .stream().findFirst()
                    .orElseThrow(() -> new WayangException("Operator could not be retrieved from Alternatives"));
            mappings.addOriginalOperator(original);

            currentNode.encoded = OneHotEncoder.encodeOperator(original, optimizationContext, encodeIds);
            currentNode.operator = original;
        } else {
            mappings.addOriginalOperator(current);
            currentNode.operator = current;

            if (current.isExecutionOperator()) {
                currentNode.encoded = OneHotEncoder.encodeOperator((ExecutionOperator) current, optimizationContext,
                        encodeIds);
            } else {
                currentNode.encoded = OneHotEncoder.encodeOperator(current, optimizationContext, encodeIds);
            }
        }

        final TreeNode nextNode = traverseWithNext(conversions, junctions, visited, next, optimizationContext,
                encodeIds);

        if (currentNode.left == null) {
            currentNode.left = nextNode;
        } else {
            currentNode.right = nextNode;
        }

        return currentNode;
    }

    private TreeNode traverse(final Operator current, final HashMap<Operator, Collection<Operator>> visited,
            final OptimizationContext optimizationContext, final boolean encodeIds) {
        if (visited.containsKey(current)) {
            return null;
        }

        final TreeNode currentNode = new TreeNode();

        if (current.isAlternative()) {
            final Operator original = ((OperatorAlternative) current).getAlternatives().get(0).getContainedOperators()
                    .stream().findFirst()
                    .orElseThrow(() -> new WayangException("Operator could not be retrieved from Alternatives"));
            mappings.addOriginalOperator(original);

            currentNode.encoded = OneHotEncoder.encodeOperator(original, optimizationContext, encodeIds);
            currentNode.operator = original;
        } else {
            mappings.addOriginalOperator(current);
            currentNode.operator = current;

            if (current.isExecutionOperator()) {
                currentNode.encoded = OneHotEncoder.encodeOperator((ExecutionOperator) current, optimizationContext,
                        encodeIds);
            } else {
                currentNode.encoded = OneHotEncoder.encodeOperator(current, optimizationContext, encodeIds);
            }
        }

        // Add for later reconstruction in TreeDecoder
        final List<Operator> inputs = Arrays.stream(current.getAllInputs()).filter(input -> input.getOccupant() != null)
                .map(input -> input.getOccupant().getOwner()).toList();

        for (final Operator input : inputs) {
            final TreeNode next = traverse(input, visited, optimizationContext, encodeIds);

            if (currentNode.getLeft() == null) {
                currentNode.left = next;
            } else {
                currentNode.right = next;
            }
        }

        return currentNode;
    }

}
