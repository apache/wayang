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

package org.apache.wayang.core.optimizer.enumeration;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Slot;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This {@link PlanEnumerationPruningStrategy} follows the idea that we can prune a
 * {@link PlanImplementation}, when there is a further one that is (i) better and (ii) has the exact same
 * operators with still-to-be-connected {@link Slot}s.
 */
public class LatentOperatorPruningStrategy implements PlanEnumerationPruningStrategy {

    private static final Logger logger = LogManager.getLogger(LatentOperatorPruningStrategy.class);

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void prune(PlanEnumeration planEnumeration) {
        // Skip if there is nothing to do...
        if (planEnumeration.getPlanImplementations().size() < 2) return;

        // Group plans.
        final Collection<List<PlanImplementation>> competingPlans =
                planEnumeration.getPlanImplementations().stream()
                        .collect(Collectors.groupingBy(LatentOperatorPruningStrategy::getInterestingProperties))
                        .values();
        final List<PlanImplementation> bestPlans = competingPlans.stream()
                .map(this::selectBestPlanNary)
                .collect(Collectors.toList());
        planEnumeration.getPlanImplementations().retainAll(bestPlans);
    }

    /**
     * Extracts the interesting properties of a {@link PlanImplementation}.
     *
     * @param implementation whose interesting properties are requested
     * @return the interesting properties of the given {@code implementation}
     */
    private static Tuple<Set<Platform>, Collection<ExecutionOperator>> getInterestingProperties(PlanImplementation implementation) {
        return new Tuple<>(
                implementation.getUtilizedPlatforms(),
                implementation.getInterfaceOperators()
        );
    }

    private PlanImplementation selectBestPlanNary(List<PlanImplementation> planImplementation) {
        assert !planImplementation.isEmpty();
        return planImplementation.stream()
                .reduce(this::selectBestPlanBinary)
                .orElseThrow(() -> new WayangException("No plan was selected."));
    }

    private PlanImplementation selectBestPlanBinary(PlanImplementation p1,
                                                    PlanImplementation p2) {
        final double t1 = p1.getSquashedCostEstimate(true);
        final double t2 = p2.getSquashedCostEstimate(true);
        final boolean isPickP1 = PlanImplementation.costComparator().compare(p1, p2) <= 0;
        if (logger.isDebugEnabled()) {
            if (isPickP1) {
                LogManager.getLogger(LatentOperatorPruningStrategy.class).debug(
                        "{} < {}: Choosing {} over {}.", p1.getTimeEstimate(), p2.getTimeEstimate(), p1.getOperators(), p2.getOperators()
                );
            } else {
                LogManager.getLogger(LatentOperatorPruningStrategy.class).debug(
                        "{} < {}: Choosing {} over {}.", p2.getTimeEstimate(), p1.getTimeEstimate(), p2.getOperators(), p1.getOperators()
                );
            }
        }
        return isPickP1 ? p1 : p2;
    }

}
