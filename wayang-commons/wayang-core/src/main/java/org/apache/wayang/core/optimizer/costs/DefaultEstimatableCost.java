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

package org.apache.wayang.core.optimizer.costs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Iterator;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.enumeration.LoopImplementation;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.executionplan.Channel;

public class DefaultEstimatableCost implements EstimatableCost {
    //~ Instance fields --------------------------------------------------------
    public static EstimatableCostFactory FACTORY = new Factory();

    /**
     * The squashed cost estimate to execute this instance. This will be used to select the best plan!
     */
    private double squashedCostEstimateCache = Double.NaN, squashedCostEstimateWithoutOverheadCache = Double.NaN;

    /**
     * The parallel cost estimate . This will store both calculated squashed cost and cost that will be used to select the best enumerated plan!
     */
    private Tuple<List<ProbabilisticDoubleInterval>, List<Double>> parallelCostEstimateCache = null;

    /**
     * This will be used to store the parallel cost of each operator.
     */
    private List<Tuple<Operator, Tuple<List<ProbabilisticDoubleInterval>, List<Double>>>> calculatedParallelOperatorCostCache = new ArrayList<>();

    //~ Methods ----------------------------------------------------------------
    @Override public PlanImplementation pickBestExecutionPlan(
        Collection<PlanImplementation> executionPlans,
        ExecutionPlan existingPlan,
        Set<Channel> openChannels,
        Set<ExecutionStage> executedStages
    ) {
        final PlanImplementation bestPlanImplementation = executionPlans.stream()
                .min(PlanImplementation.costComparator())
                .orElseThrow(() -> new WayangException("Could not find an execution plan."));
        return bestPlanImplementation;
    }

    /**
     * Retrieves the cost estimate for this instance including any overhead.
     *
     * @return the cost estimate
     */
    @Override public ProbabilisticDoubleInterval getEstimate(PlanImplementation plan, boolean isIncludeOverhead) {
        if (plan.getOptimizationContext().getConfiguration().getBooleanProperty("wayang.core.optimizer.enumeration.parallel-tasks")) {
            return this.getParallelEstimate(plan, isIncludeOverhead);
        } else {
            return this.getCostEstimate(plan, isIncludeOverhead);
        }
    }

    /**
     * Retrieves the cost estimate for this instance.
     *
     * @param isIncludeOverhead whether to include global overhead in the {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the cost estimate
     */
    ProbabilisticDoubleInterval getCostEstimate(final PlanImplementation plan, boolean isIncludeOverhead) {
        ProbabilisticDoubleInterval costEstimateWithoutOverheadCache, costEstimateCache;
        final ProbabilisticDoubleInterval operatorCosts = plan.getOperators().stream()
                .map(op -> plan.getOptimizationContext().getOperatorContext(op).getCostEstimate())
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        final ProbabilisticDoubleInterval junctionCosts = plan.getOptimizationContext().getDefaultOptimizationContexts().stream()
                .flatMap(optCtx -> plan.getJunctions().values().stream().map(jct -> jct.getCostEstimate(optCtx)))
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        final ProbabilisticDoubleInterval loopCosts = plan.getLoopImplementations().values().stream()
                .map(LoopImplementation::getCostEstimate)
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        costEstimateWithoutOverheadCache = operatorCosts.plus(junctionCosts).plus(loopCosts);
        ProbabilisticDoubleInterval overheadCosts = plan.getUtilizedPlatforms().stream()
                .map(platform -> {
                    Configuration configuraiton = plan.getOptimizationContext().getConfiguration();
                    long startUpTime = configuraiton.getPlatformStartUpTimeProvider().provideFor(platform);
                    TimeToCostConverter timeToCostConverter = configuraiton.getTimeToCostConverterProvider().provideFor(platform);
                    return timeToCostConverter.convert(new TimeEstimate(startUpTime, startUpTime, 1d));
                })
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        costEstimateCache = costEstimateWithoutOverheadCache.plus(overheadCosts);
        return isIncludeOverhead ? costEstimateCache : costEstimateWithoutOverheadCache;
    }

    /**
     * Retrieves the cost estimate for this instance.
     *
     * @param isIncludeOverhead whether to include global overhead in the {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the squashed cost estimate
     */
    @Override public double getSquashedEstimate(final PlanImplementation plan, boolean isIncludeOverhead) {
        assert Double.isNaN(this.squashedCostEstimateCache) == Double.isNaN(this.squashedCostEstimateWithoutOverheadCache);
        if (Double.isNaN(this.squashedCostEstimateCache)) {
            final double operatorCosts = plan.getOperators().stream()
                    .mapToDouble(op -> plan.getOptimizationContext().getOperatorContext(op).getSquashedCostEstimate())
                    .sum();
            final double junctionCosts = plan.getOptimizationContext().getDefaultOptimizationContexts().stream()
                    .flatMapToDouble(optCtx -> plan.getJunctions().values().stream().mapToDouble(jct -> jct.getSquashedCostEstimate(optCtx)))
                    .sum();
            final double loopCosts = plan.getLoopImplementations().values().stream()
                    .mapToDouble(LoopImplementation::getSquashedCostEstimate)
                    .sum();
            this.squashedCostEstimateWithoutOverheadCache = operatorCosts + junctionCosts + loopCosts;
            double overheadCosts = plan.getUtilizedPlatforms().stream()
                    .mapToDouble(platform -> {
                        Configuration configuration = plan.getOptimizationContext().getConfiguration();

                        long startUpTime = configuration.getPlatformStartUpTimeProvider().provideFor(platform);

                        TimeToCostConverter timeToCostConverter = configuration.getTimeToCostConverterProvider().provideFor(platform);
                        ProbabilisticDoubleInterval costs = timeToCostConverter.convert(new TimeEstimate(startUpTime, startUpTime, 1d));

                        final ToDoubleFunction<ProbabilisticDoubleInterval> squasher = configuration.getCostSquasherProvider().provide();
                        return squasher.applyAsDouble(costs);
                    })
                    .sum();
            this.squashedCostEstimateCache = this.squashedCostEstimateWithoutOverheadCache + overheadCosts;
        }
        return isIncludeOverhead ? this.squashedCostEstimateCache : this.squashedCostEstimateWithoutOverheadCache;
    }

    /**
     * Retrieves the cost estimate for this instance taking into account parallel stage execution.
     *
     * @param isIncludeOverhead whether to include global overhead in the {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the cost estimate taking into account parallel stage execution
     */
    @Override public ProbabilisticDoubleInterval getParallelEstimate(PlanImplementation plan, boolean isIncludeOverhead) {
        ProbabilisticDoubleInterval parallelCostEstimateWithoutOverhead, parallelCostEstimate;

        if (this.parallelCostEstimateCache == null) {
            // It means that the squashed cost is not yet called, might be only one possible execution plan
            this.getSquashedParallelEstimate(plan, true);
        }

        final ProbabilisticDoubleInterval loopCosts = plan.getLoopImplementations().values().stream()
                .map(LoopImplementation::getCostEstimate)
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        parallelCostEstimateWithoutOverhead = this.parallelCostEstimateCache.field0.get(0).plus(this.parallelCostEstimateCache.field0.get(1)).plus(loopCosts);
        ProbabilisticDoubleInterval overheadCosts = plan.getUtilizedPlatforms().stream()
                .map(platform -> {
                    Configuration configuration = plan.getOptimizationContext().getConfiguration();
                    long startUpTime = configuration.getPlatformStartUpTimeProvider().provideFor(platform);
                    TimeToCostConverter timeToCostConverter = configuration.getTimeToCostConverterProvider().provideFor(platform);
                    return timeToCostConverter.convert(new TimeEstimate(startUpTime, startUpTime, 1d));
                })
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
        parallelCostEstimate = parallelCostEstimateWithoutOverhead.plus(overheadCosts);
        return isIncludeOverhead ? parallelCostEstimate : parallelCostEstimateWithoutOverhead;
    }

    /**
     * Retrieves the cost estimate for this instance taking into account parallel stage execution.
     *
     * @param isIncludeOverhead whether to include global overhead in the {@link TimeEstimate} (to avoid repeating
     *                          overhead in nested instances)
     * @return the squashed cost estimate taking into account parallel stage execution
     */
    public double getSquashedParallelEstimate(final PlanImplementation plan, boolean isIncludeOverhead) {
        // Collect sink operators by Removing all operators that have an output
        Set<Operator> sinkOperators;
        sinkOperators = plan.getOperators().stream()
                .filter(op -> op.getNumOutputs() == 0)
                .collect(Collectors.toSet());

        // Retrieve operator and junction cost with parallel stage consideration
        double parallelOperatorCosts = 0f;
        double parallelJunctionCosts = 0f;

        // Iterate through all sinks to find the expensive sink
        for (Operator op : sinkOperators) {
            Tuple<List<ProbabilisticDoubleInterval>, List<Double>> tempParallelCostEstimate = plan.getCost().getParallelOperatorJunctionAllCostEstimate(plan, op);
            List<Double> tempSquashedCost = tempParallelCostEstimate.field1;

            if (tempSquashedCost.get(0) + tempSquashedCost.get(1) > parallelOperatorCosts + parallelJunctionCosts) {
                parallelOperatorCosts = tempSquashedCost.get(0);
                parallelJunctionCosts = tempSquashedCost.get(1);
                this.parallelCostEstimateCache = tempParallelCostEstimate;
            }
        }
        final double loopCosts = plan.getLoopImplementations().values().stream()
                .mapToDouble(LoopImplementation::getSquashedCostEstimate)
                .sum();
        final double parallelSquashedCostEstimateWithoutOverhead = parallelOperatorCosts + parallelJunctionCosts + loopCosts;
        double overheadCosts = plan.getUtilizedPlatforms().stream()
                .mapToDouble(platform -> {
                    Configuration configuration = plan.getOptimizationContext().getConfiguration();

                    long startUpTime = configuration.getPlatformStartUpTimeProvider().provideFor(platform);

                    TimeToCostConverter timeToCostConverter = configuration.getTimeToCostConverterProvider().provideFor(platform);
                    ProbabilisticDoubleInterval costs = timeToCostConverter.convert(new TimeEstimate(startUpTime, startUpTime, 1d));

                    final ToDoubleFunction<ProbabilisticDoubleInterval> squasher = configuration.getCostSquasherProvider().provide();
                    return squasher.applyAsDouble(costs);
                })
                .sum();
        final double parallelSquashedCostEstimate = parallelSquashedCostEstimateWithoutOverhead + overheadCosts;
        return isIncludeOverhead ? parallelSquashedCostEstimate : parallelSquashedCostEstimateWithoutOverhead;
    }

    /**
     * Retrieves the cost estimate of input {@link Operator} and input {@link Junction} and recurse if there is input Operators
     *
     * @param operator {@link Operator} that will be used to retreive the cost/squashed costs
     * @return list of probabilisticDoubleInterval where First element is the operator cost and second element is the junction cost; and
     * list of double retreived where First element is the operator squashed cost and second element is the junction squashed cost
     * <p>
     * PS: This function will start with the sink operator
     */


    public Tuple<List<ProbabilisticDoubleInterval>, List<Double>> getParallelOperatorJunctionAllCostEstimate(PlanImplementation plan, Operator operator) {

        Set<Operator> inputOperators = new HashSet<>();
        Set<Junction> inputJunction = new HashSet<>();

        List<ProbabilisticDoubleInterval> probalisticCost = new ArrayList<>();
        List<Double> squashedCost = new ArrayList<>();

        // check if the operator cost was already calculated and cached
        for (Tuple<Operator, Tuple<List<ProbabilisticDoubleInterval>, List<Double>>> t : calculatedParallelOperatorCostCache) {
            if (t.field0 == operator)
                return t.field1;
        }

        if (plan.getOptimizationContext().getOperatorContext(operator) != null) {
            // Get input junctions
            plan.getJunctions().values()
                    .forEach(j -> {
                        for (int itr = 0; itr < j.getNumTargets(); itr++) {
                            if (j.getTargetOperator(itr) == operator)
                                inputJunction.add(j);
                        }
                    });
            // Get input operators associated with input junctions
            inputJunction
                    .forEach((Junction j) -> {
                        inputOperators.add(j.getSourceOperator());
                    });

            if (inputOperators.size() == 0) {
                // If there is no input operator, only the cost of the current operator is returned
                probalisticCost.add(plan.getOptimizationContext().getOperatorContext(operator).getCostEstimate());
                probalisticCost.add(new ProbabilisticDoubleInterval(0f, 0f, 0f));
                squashedCost.add(plan.getOptimizationContext().getOperatorContext(operator).getSquashedCostEstimate());
                squashedCost.add(.0);
                Tuple<List<ProbabilisticDoubleInterval>, List<Double>> returnedCost = new Tuple(probalisticCost, squashedCost);
                this.calculatedParallelOperatorCostCache.add(new Tuple(operator, returnedCost));
                return returnedCost;
            } else if (inputOperators.size() == 1) {
                // If there is only one input operator the cost of the current operator plus the cost of the input operator is returned

                // Get the operator probalistic cost and put it as a first element in probalisticCost
                probalisticCost.add(plan.getOptimizationContext().getOperatorContext(operator).getCostEstimate()
                        .plus(this.getParallelOperatorJunctionAllCostEstimate(plan, inputOperators.iterator().next()).field0.get(0)));
                // Get the junction probalistic cost and put it as a second element in probalisticCost
                probalisticCost.add(inputJunction.iterator().next().getCostEstimate(plan.getOptimizationContext().getDefaultOptimizationContexts().get(0))
                        .plus(this.getParallelOperatorJunctionAllCostEstimate(plan, inputOperators.iterator().next()).field0.get(1)));
                // Get the operator squashed cost and put it as a first element in squashedCost
                squashedCost.add(plan.getOptimizationContext().getOperatorContext(operator).getSquashedCostEstimate()
                        + this.getParallelOperatorJunctionAllCostEstimate(plan, inputOperators.iterator().next()).field1.get(0));
                // Get the junction squashed cost and put it as a second element in squashedCost
                squashedCost.add(inputJunction.iterator().next().getSquashedCostEstimate(plan.getOptimizationContext().getDefaultOptimizationContexts().get(0))
                        + this.getParallelOperatorJunctionAllCostEstimate(plan, inputOperators.iterator().next()).field1.get(1));

                Tuple<List<ProbabilisticDoubleInterval>, List<Double>> returnedCost = new Tuple(probalisticCost, squashedCost);
                this.calculatedParallelOperatorCostCache.add(new Tuple(operator, returnedCost));
                return returnedCost;
            } else {
                // If multiple input operators, the cost returned is the max of input operators
                ProbabilisticDoubleInterval maxControlProbabilistic = new ProbabilisticDoubleInterval(0f, 0f, 0f);
                ProbabilisticDoubleInterval maxJunctionProbabilistic = new ProbabilisticDoubleInterval(0f, 0f, 0f);

                double maxControlSquash = 0;
                double maxJunctionSquash = 0;

                for (Iterator<Operator> op = inputOperators.iterator(); op.hasNext(); ) {
                    Tuple<List<ProbabilisticDoubleInterval>, List<Double>> val = this.getParallelOperatorJunctionAllCostEstimate(plan, op.next());
                    List<ProbabilisticDoubleInterval> valProbalistic = val.field0;
                    List<Double> valSquash = val.field1;
                    // Take the max of the probalistic cost
                    if (valProbalistic.get(0).getAverageEstimate() + valProbalistic.get(1).getAverageEstimate() >
                            maxControlProbabilistic.getAverageEstimate() + maxJunctionProbabilistic.getAverageEstimate()) {
                        // Get the control probalistic cost
                        maxControlProbabilistic = valProbalistic.get(0);
                        // Get the junction probalistic cost
                        maxJunctionProbabilistic = valProbalistic.get(1);
                    }
                    // Take the cost of the squashed cost
                    if (valSquash.get(0) > maxControlSquash) {
                        maxControlSquash = valSquash.get(0);
                    }
                    if (valSquash.get(1) > maxJunctionSquash) {
                        maxJunctionSquash = valSquash.get(1);
                    }
                }
                // Get the operator probalistic cost and put it as a first element in probalisticCost
                probalisticCost.add(plan.getOptimizationContext().getOperatorContext(operator).getCostEstimate().plus(maxControlProbabilistic));
                // Get the junction probalistic cost and put it as a second element in probalisticCost
                probalisticCost.add(inputJunction.iterator().next().getCostEstimate(plan.getOptimizationContext().getDefaultOptimizationContexts().get(0))
                        .plus(maxJunctionProbabilistic));
                // Get the operator squashed cost and put it as a first element in squashedCost
                squashedCost.add(plan.getOptimizationContext().getOperatorContext(operator).getSquashedCostEstimate()
                        + maxControlSquash);
                // Get the junction squashed cost and put it as a second element in squashedCost
                squashedCost.add(inputJunction.iterator().next().getSquashedCostEstimate(plan.getOptimizationContext().getDefaultOptimizationContexts().get(0))
                        + maxJunctionSquash);

                Tuple<List<ProbabilisticDoubleInterval>, List<Double>> returnedCost = new Tuple(probalisticCost, squashedCost);
                this.calculatedParallelOperatorCostCache.add(new Tuple(operator, returnedCost));
                return returnedCost;
            }
        } else {
            // Handle the case of a control not defined in this.operators (exp: loop operators)
            double controlSquash = 0;
            double junctionSquash = 0;
            ProbabilisticDoubleInterval controlProbabilistic = new ProbabilisticDoubleInterval(0f, 0f, 0f);
            ProbabilisticDoubleInterval junctionProbabilistic = new ProbabilisticDoubleInterval(0f, 0f, 0f);

            probalisticCost.add(controlProbabilistic);
            probalisticCost.add(junctionProbabilistic);
            squashedCost.add(controlSquash);
            squashedCost.add(junctionSquash);

            return new Tuple<>(probalisticCost, squashedCost);
        }
    }

    @Override public EstimatableCostFactory getFactory() {
        return FACTORY;
    }

    public static class Factory implements EstimatableCostFactory {
        @Override public EstimatableCost makeCost() {
            return new DefaultEstimatableCost();
        }
    }
}
