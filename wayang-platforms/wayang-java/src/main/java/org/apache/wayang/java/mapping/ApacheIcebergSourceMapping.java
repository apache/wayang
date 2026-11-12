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
package org.apache.wayang.java.mapping;

import java.util.Collection;
import java.util.Collections;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.wayang.basic.operators.ApacheIcebergSource;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.java.operators.JavaApacheIcebergSource;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.iceberg.expressions.Expression;


/**
 * Mapping from {@link ApacheIcebergSource} to {@link JavaApacheIcebergSource}.
 */
public class ApacheIcebergSourceMapping implements Mapping {


    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        ApacheIcebergSource icebergSource = new ApacheIcebergSource((Catalog) null, (TableIdentifier) null, (Collection<Expression>) null, (Collection<String>) null);
        
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new ApacheIcebergSource(icebergSource), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ApacheIcebergSource>(
                (matchedOperator, epoch) -> new JavaApacheIcebergSource(matchedOperator).at(epoch)
        );
    }
    
}
