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

package org.apache.wayang.semantic.platform;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.LoadToTimeConverter;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.semantic.execution.OllamaExecutor;

/**
 * {@link Platform} for operators that are executed by calling out to an Ollama-hosted LLM.
 * <p>
 * Kept as its own {@link Platform} (rather than piggy-backing on
 * {@link org.apache.wayang.java.platform.JavaPlatform}) so that the cost of an LLM call is modeled and
 * optimized on its own terms, instead of being reported as if it were free Java-Stream work.
 */
public class OllamaPlatform extends Platform {

    private static final String PLATFORM_NAME = "Ollama";

    private static final String CONFIG_NAME = "semantic.ollama";

    private static final String DEFAULT_CONFIG_FILE = "wayang-semantic-ollama-defaults.properties";

    private static OllamaPlatform instance = null;

    public static OllamaPlatform getInstance() {
        if (instance == null) {
            instance = new OllamaPlatform();
        }
        return instance;
    }

    private OllamaPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public void configureDefaults(final Configuration configuration) {
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIG_FILE));
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new OllamaExecutor(this, job);
    }

    @Override
    public LoadProfileToTimeConverter createLoadProfileToTimeConverter(final Configuration configuration) {
        final int cpuMhz = (int) configuration.getLongProperty("wayang.semantic.ollama.cpu.mhz");
        final int numCores = (int) configuration.getLongProperty("wayang.semantic.ollama.cores");
        final double hdfsMsPerMb = configuration.getDoubleProperty("wayang.semantic.ollama.hdfs.ms-per-mb");
        final double stretch = configuration.getDoubleProperty("wayang.semantic.ollama.stretch");
        return LoadProfileToTimeConverter.createTopLevelStretching(
                LoadToTimeConverter.createLinearCoverter(1 / (numCores * cpuMhz * 1000d)),
                LoadToTimeConverter.createLinearCoverter(hdfsMsPerMb / 1000000d),
                LoadToTimeConverter.createLinearCoverter(0),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate),
                stretch
        );
    }

    @Override
    public TimeToCostConverter createTimeToCostConverter(final Configuration configuration) {
        return new TimeToCostConverter(
                configuration.getDoubleProperty("wayang.semantic.ollama.costs.fix"),
                configuration.getDoubleProperty("wayang.semantic.ollama.costs.per-ms")
        );
    }
}
