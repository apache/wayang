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

package org.apache.wayang.core.mapping;

import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.Platform;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Looks for a {@link SubplanPattern} in a {@link WayangPlan} and replaces it with an alternative {@link Operator}s.
 */
public class PlanTransformation {

    protected final Logger logger = LogManager.getLogger(this.getClass());

    private final SubplanPattern pattern;

    private boolean isReplacing = false;

    private final ReplacementSubplanFactory replacementFactory;

    private final Collection<Platform> targetPlatforms;

    public PlanTransformation(SubplanPattern pattern,
                              ReplacementSubplanFactory replacementFactory,
                              Platform... targetPlatforms) {
        this.pattern = pattern;
        this.replacementFactory = replacementFactory;
        this.targetPlatforms = Arrays.asList(targetPlatforms);
    }

    /**
     * Make this instance replace on matches instead of introducing alternatives.
     *
     * @return this instance
     */
    public PlanTransformation thatReplaces() {
        this.isReplacing = true;
        return this;
    }

    /**
     * Apply this transformation exhaustively on the current plan.
     *
     * @param plan  the plan to which the transformation should be applied
     * @param epoch (i) the epoch for new plan parts and (ii) match only operators whose epoch is equal to or
     *              greater than {@code epoch-1}
     * @return the number of applied transformations
     * @see Operator#getEpoch()
     */
    public int transform(WayangPlan plan, int epoch) {
        int numTransformations = 0;
        List<SubplanMatch> matches = this.pattern.match(plan, epoch - 1);
        for (SubplanMatch match : matches) {

            if (!this.meetsPlatformRestrictions(match)) {
                continue;
            }

            final Operator replacement = this.replacementFactory.createReplacementSubplan(match, epoch);

            if (match.getInputMatch() == match.getOutputMatch()) {
                this.logger.debug("Replacing {} with {} in epoch {}.",
                        match.getOutputMatch().getOperator(),
                        replacement,
                        epoch);
            } else {
                this.logger.debug("Replacing {}..{} with {} in epoch {}.",
                        match.getInputMatch().getOperator(),
                        match.getOutputMatch().getOperator(),
                        replacement,
                        epoch);
            }

            if (this.isReplacing) {
                this.replace(plan, match, replacement);
            } else {
                this.introduceAlternative(plan, match, replacement);
            }
            numTransformations++;
        }

        return numTransformations;
    }

    /**
     * Check if this instances does not violate any of the {@link Operator#getTargetPlatforms()} restrictions.
     */
    private boolean meetsPlatformRestrictions(SubplanMatch match) {

        // Short-cut: This transformation is not introducing some platform dependency.
        if (this.getTargetPlatforms().isEmpty()) {
            return true;
        }

        // Short-cut: The matched operators do not require specific platforms.
        if (!match.getTargetPlatforms().isPresent()) {
            return true;
        }
        // Otherwise check if
        return match.getTargetPlatforms().get().containsAll(this.getTargetPlatforms());
    }

    private void introduceAlternative(WayangPlan plan, SubplanMatch match, Operator replacement) {
        // Wrap the match in an OperatorAlternative.
        final Operator originalOutputOperator = match.getOutputMatch().getOperator();
        boolean wasTopLevel = originalOutputOperator.getParent() == null;
        OperatorAlternative operatorAlternative = OperatorAlternative.wrap(match.getInputMatch().getOperator(), originalOutputOperator);

        // Update the plan sinks if necessary.
        if (wasTopLevel && originalOutputOperator.isSink()) {
            plan.replaceSink(originalOutputOperator, operatorAlternative);
        }

        // Add a new alternative to the operatorAlternative.
        operatorAlternative.addAlternative(replacement);
    }

    /**
     * @deprecated use {@link #introduceAlternative(WayangPlan, SubplanMatch, Operator)}
     */
    @Deprecated
    private void replace(WayangPlan plan, SubplanMatch match, Operator replacement) {
        // Disconnect the original input operator and insert the replacement input operator.
        final Operator originalInputOperator = match.getInputMatch().getOperator();
        for (int inputIndex = 0; inputIndex < originalInputOperator.getNumInputs(); inputIndex++) {
            final InputSlot originalInputSlot = originalInputOperator.getInput(inputIndex);
            final OutputSlot occupant = originalInputSlot.getOccupant();
            if (occupant != null) {
                occupant.disconnectFrom(originalInputSlot);
                occupant.connectTo(replacement.getInput(inputIndex));
            }
        }

        // Disconnect the original output operator and insert the replacement output operator.
        final Operator originalOutputOperator = match.getOutputMatch().getOperator();
        for (int outputIndex = 0; outputIndex < originalOutputOperator.getNumOutputs(); outputIndex++) {
            final OutputSlot originalOutputSlot = originalOutputOperator.getOutput(outputIndex);
            for (InputSlot inputSlot : new ArrayList<InputSlot>(originalOutputSlot.getOccupiedSlots())) {
                originalOutputSlot.disconnectFrom(inputSlot);
                replacement.getOutput(outputIndex).connectTo(inputSlot);
            }
        }

        // If the originalOutputOperator was a sink, we need to update the sink in the plan accordingly.
        if (originalOutputOperator.isSink()) {
            plan.getSinks().remove(originalOutputOperator);
            final Collection<Operator> sinks = new PlanTraversal(true, true)
                    .traverse(replacement)
                    .getTraversedNodesWith(Operator::isSink);
            for (Operator sink : sinks) {
                plan.addSink(sink);
            }
        }
    }

    public Collection<Platform> getTargetPlatforms() {
        return this.targetPlatforms;
    }
}
