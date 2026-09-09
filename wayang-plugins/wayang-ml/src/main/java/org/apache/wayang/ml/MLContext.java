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

package org.apache.wayang.ml;

import java.util.Optional;

import org.apache.logging.log4j.Level;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.util.Logging;

/**
 * This is the entry point for users to work with Wayang ML.
 */
public class MLContext extends WayangContext {
    public MLContext() {
        super();
    }

    public MLContext(final Configuration configuration) {
        super(configuration);
    }

    /**
     * Execute a plan.
     *
     * @param wayangPlan the plan to execute
     * @param udfJars    JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    @Override
    public void execute(final WayangPlan wayangPlan, final String... udfJars) {
        this.setLogLevel(Level.ERROR);
        final Job wayangJob = this.createJob("", wayangPlan, udfJars);

        final Configuration config = this.getConfiguration();
        final Configuration jobConfig = wayangJob.getConfiguration();

        wayangJob.execute();

        if (config.getBooleanProperty("wayang.ml.experience.enabled")) {
            final Optional<String> originalOption = config.getOptionalStringProperty("wayang.ml.experience.original");

            final OneHotMappings mappings = new OneHotMappings();
            final TreeEncoder encoder = new TreeEncoder(mappings, config);
            final String original = originalOption.orElse(encoder.encode(wayangPlan, wayangJob.getOptimizationContext()).toString());

            final Optional<String> choicesOption = config
                    .getOptionalStringProperty("wayang.ml.experience.with-platforms");
            final String withChoices = choicesOption
                    .orElse(jobConfig.getStringProperty("wayang.ml.experience.with-platforms"));

            final long execTime = jobConfig.getLongProperty("wayang.ml.experience.exec-time");

            this.logExperience(original, withChoices, execTime);
        }
    }

    private void logExperience(final String original, final String withChoices, final long execTime) {
        if (!this.getConfiguration().getBooleanProperty("wayang.ml.experience.enabled")) {
            return;
        }

        final String content = String.format("%s:%s:%d", original, withChoices, execTime);
        Logging.writeToFile(content, this.getConfiguration().getStringProperty("wayang.ml.experience.file"));
    }
}
