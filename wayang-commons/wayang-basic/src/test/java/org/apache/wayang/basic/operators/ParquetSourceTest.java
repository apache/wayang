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

package org.apache.wayang.basic.operators;

import org.apache.wayang.commons.util.profiledb.instrumentation.StopWatch;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link ParquetSource}.
 */
class ParquetSourceTest {

    @TempDir
    Path tempDir;

    @Test
    void cardinalityEstimationFallsBackWhenMetadataIsMissing() throws Exception {
        Path parquetPath = this.tempDir.resolve("orders.parquet");
        Files.write(parquetPath, new byte[]{1, 2, 3, 4});

        Job job = mock(Job.class);
        DefaultOptimizationContext optimizationContext = mock(DefaultOptimizationContext.class);
        when(job.getOptimizationContext()).thenReturn(optimizationContext);
        when(optimizationContext.getJob()).thenReturn(job);
        when(job.getStopWatch()).thenReturn(new StopWatch(new Experiment("mock", new Subject("mock", "mock"))));
        when(optimizationContext.getConfiguration()).thenReturn(new Configuration());

        ParquetSource source = new ParquetSource(parquetPath.toUri().toString(), null, "order_id");
        Optional<CardinalityEstimator> estimator = source.createCardinalityEstimator(
                0,
                optimizationContext.getConfiguration()
        );

        assertTrue(estimator.isPresent());
        CardinalityEstimate estimate = estimator.get().estimate(optimizationContext);
        assertEquals(1000L, estimate.getLowerEstimate());
        assertEquals(100000000L, estimate.getUpperEstimate());
    }
}
