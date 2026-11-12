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

package org.apache.wayang.flink.mapping;

import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.flink.plugin.FlinkBasicPlugin;

import java.util.Arrays;
import java.util.Collection;

/**
 * Register for {@link Mapping}s for {@link FlinkBasicPlugin}.
 */
public class Mappings {

    /**
     * Mappings using Flink's DataSets
     * @deprecated DataSet API in Flink has been deprecated move over to bounded streams for a 1-to-1 replacement {@link #BOUNDED_STREAM_MAPPINGS}.
     */
    @Deprecated
    public static final Collection<Mapping> BASIC_MAPPINGS = Arrays.asList(
            new CartesianMapping(),
            new CoGroupMapping(),
            new CollectionSourceMapping(),
            new CountMapping(),
            new DistinctMapping(),
            new DoWhileMapping(),
            new FilterMapping(),
            new FlatMapMapping(),
            new GlobalMaterializedGroupMapping(),
            new GlobalReduceMapping(),
            new GroupByMapping(),
            new IntersectMapping(),
            new JoinMapping(),
            new LocalCallbackSinkMapping(),
            new LoopMapping(),
            new MapMapping(),
            new MapPartitionsMapping(),
            new MaterializedGroupByMapping(),
            new PageRankMapping(),
            new ReduceByMapping(),
            new RepeatMapping(),
            new SampleMapping(),
            new SortMapping(),
            new TextFileSinkMapping(),
            new ObjectFileSinkMapping(),
            new TextFileSourceMapping(),
            new ObjectFileSourceMapping(),
            new UnionAllMapping(),
            new ZipWithIdMapping()
    );

    public static final Collection<Mapping> BOUNDED_STREAM_MAPPINGS = Arrays.asList(
        new BoundedTextFileSourceMapping(),
        new StreamedMapMapping(),
        new StreamedJoinMapping(),
        new StreamedLocalCallbackSinkMapping()
    );
}


