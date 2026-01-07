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

import java.util.ArrayList;

/**
 * This {@link PlanEnumerationPruningStrategy} retains only the best {@code k} {@link PlanImplementation}s.
 */
public class TopKPruningStrategy implements PlanEnumerationPruningStrategy {

    private int k;

    @Override
    public void configure(Configuration configuration) {
        this.k = (int) configuration.getLongProperty("wayang.core.optimizer.pruning.topk", 5);
    }

    @Override
    public void prune(PlanEnumeration planEnumeration) {
        // Skip if there is nothing to do...
        if (planEnumeration.getPlanImplementations().size() <= this.k) return;

        ArrayList<PlanImplementation> planImplementations = new ArrayList<>(planEnumeration.getPlanImplementations());
        planImplementations.sort(PlanImplementation.costComparator());
        planEnumeration.getPlanImplementations().retainAll(planImplementations.subList(0, this.k));
    }

}
