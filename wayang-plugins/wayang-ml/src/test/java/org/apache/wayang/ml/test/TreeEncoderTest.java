/*
 *
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

package org.apache.wayang.ml.test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TreeEncoderTest extends JavaExecutionTestBase {
    @Test
    public void testTreeEncoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());
        
        final ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext(), false);

        Assertions.assertNotNull(exPlan);
        Assertions.assertNotNull(encoded);
        Assertions.assertEquals(0L, encoded.encoded[0]);
    }

    @Test
    public void testEncodePlanImplementationWithDefaultConfig() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Collection<PlanImplementation> planImplementations = buildPlanImplementations(wayangPlan, wayangContext);
        Assertions.assertFalse(planImplementations.isEmpty());

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        for (final PlanImplementation planImplementation : planImplementations) {
            final TreeNode encoded = encoder.encode(planImplementation);
            Assertions.assertNotNull(encoded);
            Assertions.assertEquals(0L, encoded.encoded[0]);
        }
    }

    @Test
    public void testEncodePlanImplementationWithEncodeIdsEnabled() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        config.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Collection<PlanImplementation> planImplementations = buildPlanImplementations(wayangPlan, wayangContext);
        Assertions.assertFalse(planImplementations.isEmpty());

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        for (final PlanImplementation planImplementation : planImplementations) {
            final TreeNode encoded = encoder.encode(planImplementation);
            Assertions.assertNotNull(encoded);
            Assertions.assertNotEquals(0L, encoded.encoded[0]);
        }
    }

    @Test
    public void testEncodeWayangPlanWithConfiguration() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        config.setProperty("wayang.ml.encode-ids", "true");
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        wayangJob.buildInitialExecutionPlan();

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext());

        Assertions.assertNotNull(encoded);
        Assertions.assertNotEquals(0L, encoded.encoded[0]);
    }

    @Test
    public void testTreeEncoderConstructorWithConfiguration() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        config.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        wayangJob.buildInitialExecutionPlan();

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings(), config);
        Assertions.assertEquals(config, encoder.getConfiguration());

        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext());
        Assertions.assertNotNull(encoded);
        Assertions.assertNotEquals(0L, encoded.encoded[0]);
    }

    @Test
    public void testMachineLearningPluginSetProperties() {
        final Configuration config = new Configuration();
        org.apache.wayang.ml.MachineLearning.plugin().setProperties(config);
        Assertions.assertEquals(false, config.getBooleanProperty(TreeEncoder.ENCODE_IDS_PROPERTY));
        Assertions.assertEquals(100L, config.getLongProperty("wayang.ml.tuple.average-size"));
    }
}
