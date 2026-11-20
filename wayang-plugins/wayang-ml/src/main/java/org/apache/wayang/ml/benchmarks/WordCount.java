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

package org.apache.wayang.ml.benchmarks;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.wayang.api.utils.Parameters;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.ml.costs.DefaultPointwiseCost;

import scala.collection.JavaConversions;

/**
 * Example Apache Wayang App that does a word count -- the Hello
 * World of Map/Reduce-like systems.
 */
public class WordCount {

    /**
     * Creates the {@link WayangPlan} for the word count app.
     *
     * @param inputFileUrl the file whose words should be counted
     */
    public static WayangPlan createWayangPlan(final String inputFileUrl,
            final Collection<Tuple2<String, Integer>> collector) throws URISyntaxException, IOException {
        // Assignment mode: none.

        final TextFileSource textFileSource = new TextFileSource(inputFileUrl);
        textFileSource.setName("Load file");

        // for each line (input) output an iterator of the words
        final FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split("\\W+")), String.class, String.class,
                        new ProbabilisticDoubleInterval(100, 10000, 0.8)));
        flatMapOperator.setName("Split words");

        final FilterOperator<String> filterOperator = new FilterOperator<>(str -> !str.isEmpty(), String.class);
        filterOperator.setName("Filter empty words");

        // for each word transform it to lowercase and output a key-value pair (word, 1)
        final MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class), DataUnitType.createBasicUnchecked(Tuple2.class)),
                DataSetType.createDefault(String.class), DataSetType.createDefaultUnchecked(Tuple2.class));
        mapOperator.setName("To lower case, add counter");

        // groupby the key (word) and add up the values (frequency)
        final ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
                new TransformationDescriptor<>(pair -> pair.field0, DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(String.class)),
                new ReduceDescriptor<>(((a, b) -> {
                    a.field1 += b.field1;
                    return a;
                }), DataUnitType.createGroupedUnchecked(Tuple2.class), DataUnitType.createBasicUnchecked(Tuple2.class)),
                DataSetType.createDefaultUnchecked(Tuple2.class));
        reduceByOperator.setName("Add counters");

        // write results to a sink
        final LocalCallbackSink<Tuple2<String, Integer>> sink = LocalCallbackSink.createCollectingSink(collector,
                DataSetType.createDefaultUnchecked(Tuple2.class));
        sink.setName("Collect result");

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }

    public static void main(final String[] args) throws IOException, URISyntaxException {
        try {
            if (args.length == 0) {
                System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
                System.exit(1);
            }

            final List<Tuple2<String, Integer>> collector = new LinkedList<>();
            final WayangPlan wayangPlan = createWayangPlan(args[1], collector);

            final Configuration config = new Configuration();

            config.setProperty("wayang.ml.model.file",
                    "/var/www/html/wayang-plugins/wayang-ml/src/main/resources/cost.onnx");

            config.setProperty("wayang.core.log.enabled", "false");

            config.setCostModel(DefaultPointwiseCost.FACTORY.makeCost());
            final MLContext wayangContext = new MLContext(config);

            final List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
            plugins.stream().forEach(plug -> wayangContext.register(plug));

            wayangContext.execute(wayangPlan, ReflectionUtils.getDeclaringJar(WordCount.class),
                    ReflectionUtils.getDeclaringJar(JavaPlatform.class));

            collector.sort((t1, t2) -> Integer.compare(t2.field1, t1.field1));
            System.out.printf("Found %d words:\n", collector.size());
        } catch (final Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }

}
