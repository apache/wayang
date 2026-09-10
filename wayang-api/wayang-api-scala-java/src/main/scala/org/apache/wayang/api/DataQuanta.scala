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

package org.apache.wayang.api

/**
 * TODO: add unitary test to the elements in the file org.apache.wayang.api.DataQuanta.scala
 * labels: unitary-test,todo
 */
import _root_.java.lang.{Iterable => JavaIterable}
import _root_.java.util.function.{Consumer, IntUnaryOperator, BiFunction => JavaBiFunction, Function => JavaFunction}
import _root_.java.util.{Collection => JavaCollection}
import org.apache.commons.lang3.Validate
import org.apache.wayang.basic.function.ProjectionDescriptor
import org.apache.wayang.basic.operators._
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction, SerializableIntUnaryOperator, SerializablePredicate}
import org.apache.wayang.core.function._
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator
import org.apache.wayang.core.plan.wayangplan._
import org.apache.wayang.core.api.spatial.{SpatialGeometry, SpatialPredicate}
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.core.util.{Tuple => WayangTuple}
import org.apache.wayang.basic.data.{Record, Tuple2 => WayangTuple2}
import org.apache.wayang.basic.model.{DLModel, LogisticRegressionModel,DecisionTreeRegressionModel}
import org.apache.wayang.commons.util.profiledb.model.Experiment
import com.google.protobuf.ByteString
import org.apache.wayang.api.python.function._
import org.tensorflow.ndarray.NdArray

import scala.collection.JavaConverters._
import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect._

import org.apache.iceberg.Schema
import org.apache.iceberg.FileFormat
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}


/**
  * Represents an intermediate result/data flow edge in a [[WayangPlan]].
  *
  * @param operator    a unary [[Operator]] that produces this instance
  * @param ev$1        the data type of the elements in this instance
  * @param planBuilder keeps track of the [[WayangPlan]] being build
  */
class DataQuanta[Out: ClassTag](val operator: ElementaryOperator, outputIndex: Int = 0)(implicit val planBuilder: PlanBuilder) {

  Validate.isTrue(operator.getNumOutputs > outputIndex)

  /**
    * This instance corresponds to an [[OutputSlot]] of a wrapped [[Operator]].
    *
    * @return the said [[OutputSlot]]
    */
  implicit def output = operator.getOutput(outputIndex).asInstanceOf[OutputSlot[Out]]

  /**
    * Feed this instance into a [[MapOperator]].
    *
    * @param udf     UDF for the [[MapOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def map[NewOut: ClassTag](udf: Out => NewOut,
                            udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] =
    mapJava(toSerializableFunction(udf), udfLoad)

  /**
    * Feed this instance into a [[MapOperator]].
    *
    * @param udf     a Java 8 lambda expression as UDF for the [[MapOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def mapJava[NewOut: ClassTag](udf: SerializableFunction[Out, NewOut],
                                udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] = {
    val mapOperator = new MapOperator(new TransformationDescriptor(
      udf, basicDataUnitType[Out], basicDataUnitType[NewOut], udfLoad
    ))
    this.connectTo(mapOperator, 0)
    mapOperator
  }

  /**
    * Feed this instance into a [[MapPartitionsOperator]].
    *
    * @param udf         UDF for the [[MapPartitionsOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[MapPartitionsOperator]]'s output
    */
  def mapPartitions[NewOut: ClassTag](udf: Iterable[Out] => Iterable[NewOut],
                                      selectivity: ProbabilisticDoubleInterval = null,
                                      udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] =
    mapPartitionsJava(toSerializablePartitionFunction(udf), selectivity, udfLoad)

  /**
   * Feed this instance into a [[LogisticRegressionOperator]].
   * Trains a logistic regression model using the provided feature and label data.
   *
   * @param labels DataQuanta containing the label values (0.0 or 1.0)
   * @param fitIntercept whether to fit an intercept term
   * @return a new [[DataQuanta]] instance containing the trained [[LogisticRegressionModel]]
   */
  def trainLogisticRegression(labels: DataQuanta[java.lang.Double], fitIntercept: Boolean): DataQuanta[LogisticRegressionModel] = {
    val operator = new LogisticRegressionOperator(fitIntercept)
    this.connectTo(operator, 0)
    labels.connectTo(operator, 1)
    operator
  }

  /**
   * Feed this instance into a [[DecisionTreeRegressionOperator]].
   * Trains a generic Decision Tree Regression model using feature vectors and label values.
   *
   * @param labels DataQuanta containing the target values (labels)
   * @param maxDepth the maximum depth of the decision tree
   * @param minInstances the minimum number of instances per node
   * @return a new [[DataQuanta]] instance containing the predicted values
   */
  def trainDecisionTreeRegression(
                                   labels: DataQuanta[java.lang.Double],
                                   maxDepth: Int,
                                   minInstances: Int
                                 ): DataQuanta[DecisionTreeRegressionModel] = {
    val operator = new DecisionTreeRegressionOperator(maxDepth, minInstances)
    this.connectTo(operator, 0)
    labels.connectTo(operator, 1)
    operator
  }

  /**
   * Feed this instance into a [[LinearSVCOperator]].
   * Trains a linear support vector classification (SVM) model using feature vectors and label values.
   *
   * @param labels    DataQuanta containing the target binary labels (0.0 or 1.0)
   * @param maxIter   the maximum number of iterations to optimize the linear SVM
   * @param regParam  the regularization parameter (L2 penalty term)
   * @return a new [[DataQuanta]] instance containing the trained [[SVMModel]]
   */
  def trainLinearSVC(
                      labels: DataQuanta[java.lang.Double],
                      maxIter: Int,
                      regParam: Double
                    ): DataQuanta[org.apache.wayang.basic.model.SVMModel] = {
    val operator = new LinearSVCOperator(maxIter, regParam)
    this.connectTo(operator, 0)
    labels.connectTo(operator, 1)
    operator
  }





  /**
    * Feed this instance into a [[MapPartitionsOperator]].
    *
    * @param udf         a Java 8 lambda expression as UDF for the [[MapPartitionsOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def mapPartitionsJava[NewOut: ClassTag](udf: SerializableFunction[JavaIterable[Out], JavaIterable[NewOut]],
                                          selectivity: ProbabilisticDoubleInterval = null,
                                          udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] = {
    val mapOperator = new MapPartitionsOperator(
      new MapPartitionsDescriptor(udf, basicDataUnitType[Out], basicDataUnitType[NewOut], selectivity, udfLoad)
    )
    this.connectTo(mapOperator, 0)
    mapOperator
  }

  /**
    * Feed this instance into a [[MapPartitionsOperator]].
    *
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def mapPartitionsPython[NewOut: ClassTag](
      udf: String,
      inputType: Class[_ <: Any],
      outputType: Class[_ <: Any]
    ): DataQuanta[NewOut] = {
    val descriptor = new WrappedMapPartitionsDescriptor(
        ByteString.copyFromUtf8(udf),
        inputType,
        outputType
    )
    val mapOperator = new MapPartitionsOperator(
      descriptor
    )
    this.connectTo(mapOperator, 0)
    mapOperator
  }


  /**
    * Feed this instance into a [[MapOperator]] with a [[ProjectionDescriptor]].
    *
    * @param fieldNames names of the fields to be projected
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def project[NewOut: ClassTag](fieldNames: Seq[String]): DataQuanta[NewOut] = {
    val projectionOperator = new MapOperator(
      new ProjectionDescriptor(basicDataUnitType[Out], basicDataUnitType[NewOut], fieldNames: _*)
    )
    this.connectTo(projectionOperator, 0)
    projectionOperator
  }

  /**
    * Connects the [[operator]] to a further [[Operator]].
    *
    * @param operator   the [[Operator]] to connect to
    * @param inputIndex the input index of the [[Operator]]s [[InputSlot]]
    */
  private[api] def connectTo(operator: Operator, inputIndex: Int): Unit =
    this.operator.connectTo(outputIndex, operator, inputIndex)


  /**
    * Feed this instance into a [[FilterOperator]].
    *
    * @param udf         UDF for the [[FilterOperator]]
    * @param sqlUdf      UDF as SQL `WHERE` clause
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @param selectivity selectivity of the UDF
    * @return a new instance representing the [[FilterOperator]]'s output
    */
  def filter(udf: Out => Boolean,
             sqlUdf: String = null,
             selectivity: ProbabilisticDoubleInterval = null,
             udfLoad: LoadProfileEstimator = null) =
    filterJava(toSerializablePredicate(udf), sqlUdf, selectivity, udfLoad)

  /**
    * Feed this instance into a [[FilterOperator]].
    *
    * @param udf         UDF for the [[FilterOperator]]
    * @param sqlUdf      UDF as SQL `WHERE` clause
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[FilterOperator]]'s output
    */
  def filterJava(udf: SerializablePredicate[Out],
                 sqlUdf: String = null,
                 selectivity: ProbabilisticDoubleInterval = null,
                 udfLoad: LoadProfileEstimator = null): DataQuanta[Out] = {
    val filterOperator = new FilterOperator(new PredicateDescriptor(
      udf, this.output.getType.getDataUnitType.toBasicDataUnitType, selectivity, udfLoad
    ).withSqlImplementation(sqlUdf))
    this.connectTo(filterOperator, 0)
    filterOperator
  }

  def filterPython(udf: String,
                 sqlUdf: String = null,
                 selectivity: ProbabilisticDoubleInterval = null,
                 udfLoad: LoadProfileEstimator = null): DataQuanta[Out] = {
    val filterOperator = new FilterOperator(
      new WrappedPredicateDescriptor(
        ByteString.copyFromUtf8(udf),
        this.output.getType.getDataUnitType.toBasicDataUnitType,
        selectivity,
        udfLoad
    ).withSqlImplementation(sqlUdf))
    this.connectTo(filterOperator, 0)
    filterOperator
  }

  /**
    * Feed this instance into a [[FlatMapOperator]].
    *
    * @param udf         UDF for the [[FlatMapOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def flatMap[NewOut: ClassTag](udf: Out => Iterable[NewOut],
                                selectivity: ProbabilisticDoubleInterval = null,
                                udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] =
    flatMapJava(toSerializableFlatteningFunction(udf), selectivity, udfLoad)

  /**
    * Feed this instance into a [[FlatMapOperator]].
    *
    * @param udf         a Java 8 lambda expression as UDF for the [[FlatMapOperator]]
    * @param selectivity selectivity of the UDF
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def flatMapJava[NewOut: ClassTag](udf: SerializableFunction[Out, JavaIterable[NewOut]],
                                    selectivity: ProbabilisticDoubleInterval = null,
                                    udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] = {
    val flatMapOperator = new FlatMapOperator(new FlatMapDescriptor(
      udf, basicDataUnitType[Out], basicDataUnitType[NewOut], selectivity, udfLoad
    ))
    this.connectTo(flatMapOperator, 0)
    flatMapOperator
  }

  def flatMapPython[NewOut: ClassTag](udf: String,
                                    selectivity: ProbabilisticDoubleInterval = null,
                                    udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] = {
    val flatMapOperator = new FlatMapOperator(
      new WrappedFlatMapDescriptor(
        ByteString.copyFromUtf8(udf),
        basicDataUnitType[Out],
        basicDataUnitType[NewOut],
        selectivity,
        udfLoad
      )
    )
    this.connectTo(flatMapOperator, 0)
    flatMapOperator
  }

  /**
    * Feed this instance into a [[SampleOperator]]. If this operation is inside of a loop, the sampling size
    * can be adjusted in each iteration.
    *
    * @param sampleSize   absolute size of the sample
    * @param datasetSize  optional size of the dataset to be sampled
    * @param seed         the seed for the random sample
    * @param sampleMethod the [[SampleOperator.Methods]] to use for sampling
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def sample(sampleSize: Int,
             datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
             seed: Option[Long] = None,
             sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): DataQuanta[Out] =
    this.sampleDynamic(_ => sampleSize, datasetSize, seed, sampleMethod)

  /**
    * Feed this instance into a [[SampleOperator]]. If this operation is inside of a loop, the sampling size
    * can be adjusted in each iteration.
    *
    * @param sampleSizeFunction absolute size of the sample as a function of the current iteration number
    * @param datasetSize        optional size of the dataset to be sampled
    * @param sampleMethod       the [[SampleOperator.Methods]] to use for sampling
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def sampleDynamic(sampleSizeFunction: Int => Int,
                    datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
                    seed: Option[Long] = None,
                    sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): DataQuanta[Out] =
    this.sampleDynamicJava(
      new SerializableIntUnaryOperator {
        override def applyAsInt(operand: Int): Int = sampleSizeFunction(operand)
      },
      datasetSize,
      seed,
      sampleMethod
    )


  /**
    * Feed this instance into a [[SampleOperator]].
    *
    * @param sampleSizeFunction absolute size of the sample as a function of the current iteration number
    * @param datasetSize        optional size of the dataset to be sampled
    * @param sampleMethod       the [[SampleOperator.Methods]] to use for sampling
    * @return a new instance representing the [[FlatMapOperator]]'s output
    */
  def sampleDynamicJava(sampleSizeFunction: SerializableIntUnaryOperator,
                        datasetSize: Long = SampleOperator.UNKNOWN_DATASET_SIZE,
                        seed: Option[Long] = None,
                        sampleMethod: SampleOperator.Methods = SampleOperator.Methods.ANY): DataQuanta[Out] = {
    if (seed.isEmpty) {
      val sampleOperator = new SampleOperator(
        sampleSizeFunction,
        dataSetType[Out],
        sampleMethod
      )
      sampleOperator.setDatasetSize(datasetSize)
      this.connectTo(sampleOperator, 0)
      sampleOperator
    }
    else {
      val sampleOperator = new SampleOperator(
        sampleSizeFunction,
        dataSetType[Out],
        sampleMethod,
        seed.get
      )
      sampleOperator.setDatasetSize(datasetSize)
      this.connectTo(sampleOperator, 0)
      sampleOperator
    }
  }

  /**
    * Assigns this instance a key extractor, which enables some key-based operations.
    *
    * @see KeyedDataQuanta
    * @param keyExtractor extracts the key from the [[DataQuanta]]
    * @return the [[KeyedDataQuanta]]
    */
  def keyBy[Key: ClassTag](keyExtractor: Out => Key) = new KeyedDataQuanta[Out, Key](this, keyExtractor)

  /**
    * Assigns this instance a key extractor, which enables some key-based operations.
    *
    * @see KeyedDataQuanta
    * @param keyExtractor extracts the key from the [[DataQuanta]]
    * @return the [[KeyedDataQuanta]]
    */
  def keyByJava[Key: ClassTag](keyExtractor: SerializableFunction[Out, Key]) = new KeyedDataQuanta[Out, Key](this, keyExtractor)

  /**
    * Feed this instance into a [[ReduceByOperator]].
    *
    * @param keyUdf  UDF to extract the grouping key from the data quanta
    * @param udf     aggregation UDF for the [[ReduceByOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[ReduceByOperator]]'s output
    */
  def reduceByKey[Key: ClassTag](keyUdf: Out => Key,
                                 udf: (Out, Out) => Out,
                                 udfLoad: LoadProfileEstimator = null): DataQuanta[Out] =
    reduceByKeyJava(toSerializableFunction(keyUdf), toSerializableBinaryOperator(udf), udfLoad)

  /**
    * Feed this instance into a [[ReduceByOperator]].
    *
    * @param keyUdf  UDF to extract the grouping key from the data quanta
    * @param udf     aggregation UDF for the [[ReduceByOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[ReduceByOperator]]'s output
    */
  def reduceByKeyJava[Key: ClassTag](keyUdf: SerializableFunction[Out, Key],
                                     udf: SerializableBinaryOperator[Out],
                                     udfLoad: LoadProfileEstimator = null)
  : DataQuanta[Out] = {
    val reduceByOperator = new ReduceByOperator(
      new TransformationDescriptor(keyUdf, basicDataUnitType[Out], basicDataUnitType[Key]),
      new ReduceDescriptor(udf, groupedDataUnitType[Out], basicDataUnitType[Out], udfLoad)
    )
    this.connectTo(reduceByOperator, 0)
    reduceByOperator
  }

  def reduceByKeyPython[Key: ClassTag](keyUdf: String,
                                     udf: String,
                                     udfLoad: LoadProfileEstimator = null)
  : DataQuanta[Out] = {
    val reduceByOperator = new ReduceByOperator(
      new WrappedTransformationDescriptor(
        ByteString.copyFromUtf8(keyUdf),
        basicDataUnitType[Out],
        basicDataUnitType[Key]
      ),
      new WrappedReduceDescriptor(
        ByteString.copyFromUtf8(udf),
        groupedDataUnitType[Out],
        basicDataUnitType[Out]
      )
    )
    this.connectTo(reduceByOperator, 0)
    reduceByOperator
  }

  /**
    * Feed this instance into a [[MaterializedGroupByOperator]].
    *
    * @param keyUdf     UDF to extract the grouping key from the data quanta
    * @param keyUdfLoad optional [[LoadProfileEstimator]] for the `keyUdf`
    * @return a new instance representing the [[MaterializedGroupByOperator]]'s output
    */
  def groupByKey[Key: ClassTag](keyUdf: Out => Key,
                                keyUdfLoad: LoadProfileEstimator = null): DataQuanta[java.lang.Iterable[Out]] =
    groupByKeyJava(toSerializableFunction(keyUdf), keyUdfLoad)

  /**
    * Feed this instance into a [[MaterializedGroupByOperator]].
    *
    * @param keyUdf     UDF to extract the grouping key from the data quanta
    * @param keyUdfLoad optional [[LoadProfileEstimator]] for the `keyUdf`
    * @return a new instance representing the [[MaterializedGroupByOperator]]'s output
    */
  def groupByKeyJava[Key: ClassTag](keyUdf: SerializableFunction[Out, Key],
                                    keyUdfLoad: LoadProfileEstimator = null): DataQuanta[java.lang.Iterable[Out]] = {
    val groupByOperator = new MaterializedGroupByOperator(
      new TransformationDescriptor(keyUdf, basicDataUnitType[Out], basicDataUnitType[Key], keyUdfLoad),
      dataSetType[Out],
      groupedDataSetType[Out]
    )
    this.connectTo(groupByOperator, 0)
    groupByOperator
  }

  /**
    * Feed this instance into a [[GlobalReduceOperator]].
    *
    * @param udf     aggregation UDF for the [[GlobalReduceOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[GlobalReduceOperator]]'s output
    */
  def reduce(udf: (Out, Out) => Out,
             udfLoad: LoadProfileEstimator = null): DataQuanta[Out] =
    reduceJava(toSerializableBinaryOperator(udf), udfLoad)

  /**
    * Feed this instance into a [[GlobalReduceOperator]].
    *
    * @param udf     aggregation UDF for the [[GlobalReduceOperator]]
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the [[GlobalReduceOperator]]'s output
    */
  def reduceJava(udf: SerializableBinaryOperator[Out],
                 udfLoad: LoadProfileEstimator = null): DataQuanta[Out] = {
    val globalReduceOperator = new GlobalReduceOperator(
      new ReduceDescriptor(udf, groupedDataUnitType[Out], basicDataUnitType[Out], udfLoad)
    )
    this.connectTo(globalReduceOperator, 0)
    globalReduceOperator
  }

  /**
    * Feed this instance into a [[GlobalMaterializedGroupOperator]].
    *
    * @return a new instance representing the [[GlobalMaterializedGroupOperator]]'s output
    */
  def group(): DataQuanta[java.lang.Iterable[Out]] = {
    val groupOperator = new GlobalMaterializedGroupOperator(dataSetType[Out], groupedDataSetType[Out])
    this.connectTo(groupOperator, 0)
    groupOperator
  }

  /**
    * Feed this instance and a further instance into a [[UnionAllOperator]].
    *
    * @param that the other instance to union with
    * @return a new instance representing the [[UnionAllOperator]]'s output
    */
  def union(that: DataQuanta[Out]): DataQuanta[Out] = {
    require(this.planBuilder eq that.planBuilder, s"$this and $that must use the same plan builders.")
    val unionAllOperator = new UnionAllOperator(dataSetType[Out])
    this.connectTo(unionAllOperator, 0)
    that.connectTo(unionAllOperator, 1)
    unionAllOperator
  }

  /**
    * Feed this instance and a further instance into a [[IntersectOperator]].
    *
    * @param that the other instance to intersect with
    * @return a new instance representing the [[IntersectOperator]]'s output
    */
  def intersect(that: DataQuanta[Out]): DataQuanta[Out] = {
    require(this.planBuilder eq that.planBuilder, s"$this and $that must use the same plan builders.")
    val intersectOperator = new IntersectOperator(dataSetType[Out])
    this.connectTo(intersectOperator, 0)
    that.connectTo(intersectOperator, 1)
    intersectOperator
  }

  /**
    * Feeds this and a further instance into a [[JoinOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[JoinOperator]]'s output
    */
  def join[ThatOut: ClassTag, Key: ClassTag]
  (thisKeyUdf: Out => Key,
   that: DataQuanta[ThatOut],
   thatKeyUdf: ThatOut => Key)
  : DataQuanta[WayangTuple2[Out, ThatOut]] =
    joinJava(toSerializableFunction(thisKeyUdf), that, toSerializableFunction(thatKeyUdf))

  /**
    * Feeds this and a further instance into a [[JoinOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[JoinOperator]]'s output
    */
  def joinJava[ThatOut: ClassTag, Key: ClassTag]
  (thisKeyUdf: SerializableFunction[Out, Key],
   that: DataQuanta[ThatOut],
   thatKeyUdf: SerializableFunction[ThatOut, Key])
  : DataQuanta[WayangTuple2[Out, ThatOut]] = {
    require(this.planBuilder eq that.planBuilder, s"$this and $that must use the same plan builders.")
    val joinOperator = new JoinOperator(
      new TransformationDescriptor(thisKeyUdf, basicDataUnitType[Out], basicDataUnitType[Key]),
      new TransformationDescriptor(thatKeyUdf, basicDataUnitType[ThatOut], basicDataUnitType[Key])
    )
    this.connectTo(joinOperator, 0)
    that.connectTo(joinOperator, 1)
    joinOperator
  }

  def joinPython[ThatOut: ClassTag, Key: ClassTag]
  (thisKeyUdf: String,
   that: DataQuanta[ThatOut],
   thatKeyUdf: String)
  : DataQuanta[WayangTuple2[Out, ThatOut]] = {
    require(this.planBuilder eq that.planBuilder, s"$this and $that must use the same plan builders.")
    val joinOperator = new JoinOperator(
      new WrappedTransformationDescriptor(
        ByteString.copyFromUtf8(thisKeyUdf),
        basicDataUnitType[Out],
        basicDataUnitType[Key]
      ),
      new WrappedTransformationDescriptor(
        ByteString.copyFromUtf8(thatKeyUdf),
        basicDataUnitType[ThatOut],
        basicDataUnitType[Key]
      )
    )
    this.connectTo(joinOperator, 0)
    that.connectTo(joinOperator, 1)
    joinOperator
  }

  def semanticFilterPrompt(prompt: String): DataQuanta[Out] = {
    val dataSetType = org.apache.wayang.core.types.DataSetType.createDefault(
      this.output.getType.getDataUnitType.toBasicDataUnitType
    )

    val filterOperator = new SemanticFilterOperator[Out](
      dataSetType,
      prompt
    )

    this.connectTo(filterOperator, 0)
    wrap[Out](filterOperator)
  }

  def semanticFilterPrompt(prompt: String, targetModels: AnyRef * ): DataQuanta[Out] = {
      val dataSetType = org.apache.wayang.core.types.DataSetType.createDefault(
        this.output.getType.getDataUnitType.toBasicDataUnitType
      )

      val targetModelsSet: java.util.Set[Object] = targetModels.toSet.asJava.asInstanceOf[java.util.Set[Object]]
      
      val filterOperator = new SemanticFilterOperator[Out](
        dataSetType,
        prompt,
        targetModelsSet
      )

      this.connectTo(filterOperator, 0)
      wrap[Out](filterOperator)
  }


  /**
    * Applies a spatial filter to this instance.
    *
    * @param keySelector    UDF to extract spatial geometry from data quanta
    * @param predicateType  the spatial predicate type
    * @param filterGeometry the geometry to filter against
    * @param columnName     optional SQL column name for database pushdown
    * @return a new instance representing the filtered output
    */
  def spatialFilter(keySelector: Out => SpatialGeometry,
                    predicateType: SpatialPredicate,
                    filterGeometry: SpatialGeometry,
                    columnName: String = null): DataQuanta[Out] =
    spatialFilterJava(toSerializableFunction(keySelector), predicateType, filterGeometry, columnName)

  /**
    * Applies a spatial filter to this instance.
    *
    * @param keySelector    UDF to extract spatial geometry from data quanta
    * @param predicateType  the spatial predicate type
    * @param filterGeometry the geometry to filter against
    * @param columnName     optional SQL column name for database pushdown
    * @return a new instance representing the filtered output
    */
  def spatialFilterJava(keySelector: SerializableFunction[Out, _ <: SpatialGeometry],
                        predicateType: SpatialPredicate,
                        filterGeometry: SpatialGeometry,
                        columnName: String = null): DataQuanta[Out] = {
    val op = new SpatialFilterOperator(predicateType, keySelector, dataSetType[Out], filterGeometry)
    if (columnName != null) op.getKeyDescriptor.withSqlImplementation(null, columnName)
    this.connectTo(op, 0)
    wrap[Out](op)
  }

  /**
    * Feeds this and a further instance into a [[SpatialJoinOperator]].
    *
    * @param thisKeyUdf    UDF to extract spatial geometry from this instance's elements
    * @param that          the other instance
    * @param thatKeyUdf    UDF to extract spatial geometry from `that` instance's elements
    * @param predicateType the spatial predicate type for the join
    * @return a new instance representing the SpatialJoinOperator's output
    */
  def spatialJoin[ThatOut: ClassTag](
      thisKeyUdf: Out => SpatialGeometry,
      that: DataQuanta[ThatOut],
      thatKeyUdf: ThatOut => SpatialGeometry,
      predicateType: SpatialPredicate): DataQuanta[WayangTuple2[Out, ThatOut]] =
    spatialJoinJava(toSerializableFunction(thisKeyUdf), that, toSerializableFunction(thatKeyUdf), predicateType)

  /**
    * Feeds this and a further instance into a [[SpatialJoinOperator]].
    *
    * @param thisKeyUdf    UDF to extract spatial geometry from this instance's elements
    * @param that          the other instance
    * @param thatKeyUdf    UDF to extract spatial geometry from `that` instance's elements
    * @param predicateType the spatial predicate type for the join
    * @return a new instance representing the SpatialJoinOperator's output
    */
  def spatialJoinJava[ThatOut: ClassTag](
      thisKeyUdf: SerializableFunction[Out, _ <: SpatialGeometry],
      that: DataQuanta[ThatOut],
      thatKeyUdf: SerializableFunction[ThatOut, _ <: SpatialGeometry],
      predicateType: SpatialPredicate): DataQuanta[WayangTuple2[Out, ThatOut]] = {
    require(this.planBuilder eq that.planBuilder, s"$this and $that must use the same plan builders.")
    val op = new SpatialJoinOperator(
      new TransformationDescriptor(thisKeyUdf.asInstanceOf[SerializableFunction[Out, SpatialGeometry]], basicDataUnitType[Out], basicDataUnitType[SpatialGeometry]),
      new TransformationDescriptor(thatKeyUdf.asInstanceOf[SerializableFunction[ThatOut, SpatialGeometry]], basicDataUnitType[ThatOut], basicDataUnitType[SpatialGeometry]),
      predicateType
    )
    this.connectTo(op, 0)
    that.connectTo(op, 1)
    wrap[WayangTuple2[Out, ThatOut]](op)
  }

  def predict[ThatOut: ClassTag](
    that: DataQuanta[ThatOut],
    inputType: Class[_ <: Any],
    outputType: Class[_ <: Any]
  ): DataQuanta[Out] =
    predictJava(that, inputType, outputType)

  def predictJava[ThatOut: ClassTag](
    that: DataQuanta[ThatOut],
    inputType: Class[_ <: Any],
    outputType: Class[_ <: Any]
  ): DataQuanta[Out] = {
    val predictOperator = new PredictOperator(
      inputType, outputType
    )
    this.connectTo(predictOperator, 0)
    that.connectTo(predictOperator, 1)
    predictOperator
  }

  def predictJava[ThatOut: ClassTag, Result: ClassTag](
    that: DataQuanta[ThatOut]
    ): DataQuanta[Result] = {
    val predictOperator = new PredictOperator(
      implicitly[ClassTag[ThatOut]].runtimeClass,
      implicitly[ClassTag[Result]].runtimeClass
    )
    this.connectTo(predictOperator, 0)
    that.connectTo(predictOperator, 1)
    predictOperator
  }

  def dlTraining[ThatOut: ClassTag](
    model: DLModel,
    option: DLTrainingOperator.Option,
    that: DataQuanta[ThatOut],
    xType: Class[_ <: Any],
    yType: Class[_ <: Any]
  ): DataQuanta[Out] =
    dlTrainingJava(model, option, that, xType, yType)

  def dlTrainingJava[ThatOut: ClassTag](
    model: DLModel,
    option: DLTrainingOperator.Option,
    that: DataQuanta[ThatOut],
    xType: Class[_ <: Any],
    yType: Class[_ <: Any]
  ): DataQuanta[Out] = {
    val dlTrainingOperator = new DLTrainingOperator(
      model,
      option,
      xType,
      yType
    )

    this.connectTo(dlTrainingOperator, 0)
    that.connectTo(dlTrainingOperator, 1)
    dlTrainingOperator
  }


  def dlTrainingJava[ThatOut: ClassTag](
    model: DLModel,
    option: DLTrainingOperator.Option,
    that: DataQuanta[ThatOut]
  ): DataQuanta[DLModel] = {
    val dlTrainingOperator = new DLTrainingOperator(
      model,
      option,
      implicitly[ClassTag[Out]].runtimeClass,
      implicitly[ClassTag[ThatOut]].runtimeClass
    )

    this.connectTo(dlTrainingOperator, 0)
    that.connectTo(dlTrainingOperator, 1)
    dlTrainingOperator
  }

  /**
    * Feeds this and a further instance into a [[CoGroupOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[CoGroupOperator]]'s output
    */
  def coGroup[ThatOut: ClassTag, Key: ClassTag]
  (thisKeyUdf: Out => Key,
   that: DataQuanta[ThatOut],
   thatKeyUdf: ThatOut => Key)
  : DataQuanta[WayangTuple2[java.lang.Iterable[Out], java.lang.Iterable[ThatOut]]] =
    coGroupJava(toSerializableFunction(thisKeyUdf), that, toSerializableFunction(thatKeyUdf))

  /**
    * Feeds this and a further instance into a [[CoGroupOperator]].
    *
    * @param thisKeyUdf UDF to extract keys from data quanta in this instance
    * @param that       the other instance
    * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
    * @return a new instance representing the [[CoGroupOperator]]'s output
    */
  def coGroupJava[ThatOut: ClassTag, Key: ClassTag]
  (thisKeyUdf: SerializableFunction[Out, Key],
   that: DataQuanta[ThatOut],
   thatKeyUdf: SerializableFunction[ThatOut, Key])
  : DataQuanta[WayangTuple2[java.lang.Iterable[Out], java.lang.Iterable[ThatOut]]] = {
    require(this.planBuilder eq that.planBuilder, s"$this and $that must use the same plan builders.")
    val coGroupOperator = new CoGroupOperator(
      new TransformationDescriptor(thisKeyUdf, basicDataUnitType[Out], basicDataUnitType[Key]),
      new TransformationDescriptor(thatKeyUdf, basicDataUnitType[ThatOut], basicDataUnitType[Key])
    )
    this.connectTo(coGroupOperator, 0)
    that.connectTo(coGroupOperator, 1)
    coGroupOperator
  }

  /**
    * Feeds this and a further instance into a [[SortOperator]].
    *
    * @param keyUdf UDF to extract key from data quanta in this instance
    * @return a new instance representing the [[SortOperator]]'s output
    */
  def sort[Key: ClassTag]
  (keyUdf: Out => Key)
  : DataQuanta[Out] =
    sortJava(toSerializableFunction(keyUdf))

  /**
    * Feeds this and a further instance into a [[SortOperator]].
    *
    * @param keyUdf UDF to extract key from data quanta in this instance
    * @return a new instance representing the [[SortOperator]]'s output
    */
  def sortJava[Key: ClassTag]
  (keyUdf: SerializableFunction[Out, Key])
  : DataQuanta[Out] = {
    val sortOperator = new SortOperator(new TransformationDescriptor(
      keyUdf, basicDataUnitType[Out], basicDataUnitType[Key]))
    this.connectTo(sortOperator, 0)
    sortOperator
  }



  /**
    * Feeds this and a further instance into a [[CartesianOperator]].
    *
    * @param that the other instance
    * @return a new instance representing the [[CartesianOperator]]'s output
    */
  def cartesian[ThatOut: ClassTag](that: DataQuanta[ThatOut])
  : DataQuanta[WayangTuple2[Out, ThatOut]] = {
    require(this.planBuilder eq that.planBuilder, s"$this and $that must use the same plan builders.")
    val cartesianOperator = new CartesianOperator(dataSetType[Out], dataSetType[ThatOut])
    this.connectTo(cartesianOperator, 0)
    that.connectTo(cartesianOperator, 1)
    cartesianOperator
  }

  /**
    * Feeds this instance into a [[ZipWithIdOperator]].
    *
    * @return a new instance representing the [[ZipWithIdOperator]]'s output
    */
  def zipWithId: DataQuanta[WayangTuple2[java.lang.Long, Out]] = {
    val zipWithIdOperator = new ZipWithIdOperator(dataSetType[Out])
    this.connectTo(zipWithIdOperator, 0)
    zipWithIdOperator
  }

  /**
    * Feeds this instance into a [[DistinctOperator]].
    *
    * @return a new instance representing the [[DistinctOperator]]'s output
    */
  def distinct: DataQuanta[Out] = {
    val distinctOperator = new DistinctOperator(dataSetType[Out])
    this.connectTo(distinctOperator, 0)
    distinctOperator
  }

  /**
    * Feeds this instance into a [[CountOperator]].
    *
    * @return a new instance representing the [[CountOperator]]'s output
    */
  def count: DataQuanta[java.lang.Long] = {
    val countOperator = new CountOperator(dataSetType[Out])
    this.connectTo(countOperator, 0)
    countOperator
  }


  /**
    * Feeds this instance into a do-while loop (guarded by a [[DoWhileOperator]].
    *
    * @param udf         condition to be evaluated after each iteration
    * @param bodyBuilder creates the loop body
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the final output of the [[DoWhileOperator]]
    */
  def doWhile[ConvOut: ClassTag](udf: Iterable[ConvOut] => Boolean,
                                 bodyBuilder: DataQuanta[Out] => (DataQuanta[Out], DataQuanta[ConvOut]),
                                 numExpectedIterations: Int = 20,
                                 udfLoad: LoadProfileEstimator = null) =
    doWhileJava(
      toSerializablePredicate((in: JavaCollection[ConvOut]) => udf(JavaConversions.collectionAsScalaIterable(in))),
      new JavaFunction[DataQuanta[Out], WayangTuple[DataQuanta[Out], DataQuanta[ConvOut]]] {
        override def apply(t: DataQuanta[Out]) = {
          val result = bodyBuilder(t)
          new WayangTuple(result._1, result._2)
        }
      },
      numExpectedIterations, udfLoad
    )

  /**
    * Feeds this instance into a do-while loop (guarded by a [[DoWhileOperator]].
    *
    * @param udf         condition to be evaluated after each iteration
    * @param bodyBuilder creates the loop body
    * @param udfLoad     optional [[LoadProfileEstimator]] for the `udf`
    * @return a new instance representing the final output of the [[DoWhileOperator]]
    */
  def doWhileJava[ConvOut: ClassTag](
                                      udf: SerializablePredicate[JavaCollection[ConvOut]],
                                      bodyBuilder: JavaFunction[DataQuanta[Out], WayangTuple[DataQuanta[Out], DataQuanta[ConvOut]]],
                                      numExpectedIterations: Int = 20,
                                      udfLoad: LoadProfileEstimator = null) = {
    // Create the DoWhileOperator.
    val doWhileOperator = new DoWhileOperator(
      dataSetType[Out],
      dataSetType[ConvOut],
      new PredicateDescriptor(udf, basicDataUnitType[JavaCollection[ConvOut]], null, udfLoad),
      numExpectedIterations
    )
    this.connectTo(doWhileOperator, DoWhileOperator.INITIAL_INPUT_INDEX)

    // Create and wire the loop body.
    val loopDataQuanta = new DataQuanta[Out](doWhileOperator, DoWhileOperator.ITERATION_OUTPUT_INDEX)
    val iterationResults = bodyBuilder.apply(loopDataQuanta)
    iterationResults.getField0.connectTo(doWhileOperator, DoWhileOperator.ITERATION_INPUT_INDEX)
    iterationResults.getField1.connectTo(doWhileOperator, DoWhileOperator.CONVERGENCE_INPUT_INDEX)

    // Return the iteration result.
    new DataQuanta[Out](doWhileOperator, DoWhileOperator.FINAL_OUTPUT_INDEX)
  }

  /**
    * Feeds this instance into a for-loop (guarded by a [[LoopOperator]].
    *
    * @param n           number of iterations
    * @param bodyBuilder creates the loop body
    * @return a new instance representing the final output of the [[LoopOperator]]
    */
  def repeat(n: Int, bodyBuilder: DataQuanta[Out] => DataQuanta[Out]) =
    repeatJava(n,
      new JavaFunction[DataQuanta[Out], DataQuanta[Out]] {
        override def apply(t: DataQuanta[Out]) = bodyBuilder(t)
      }
    )

  /**
    * Feeds this instance into a for-loop (guarded by a [[LoopOperator]].
    *
    * @param n           number of iterations
    * @param bodyBuilder creates the loop body
    * @return a new instance representing the final output of the [[LoopOperator]]
    */
  def repeatJava(n: Int, bodyBuilder: JavaFunction[DataQuanta[Out], DataQuanta[Out]]) = {
    // Create the RepeatOperator.
    val repeatOperator = new RepeatOperator(n, dataSetType[Out])
    this.connectTo(repeatOperator, RepeatOperator.INITIAL_INPUT_INDEX)

    // Create and wire the loop body.
    val loopDataQuanta = new DataQuanta[Out](repeatOperator, RepeatOperator.ITERATION_OUTPUT_INDEX)
    val iterationResult = bodyBuilder.apply(loopDataQuanta)
    iterationResult.connectTo(repeatOperator, RepeatOperator.ITERATION_INPUT_INDEX)

    // Return the iteration result.
    new DataQuanta[Out](repeatOperator, RepeatOperator.FINAL_OUTPUT_INDEX)
  }

  /**
    * Use a custom [[Operator]]. Note that only [[Operator]]s with a single [[InputSlot]] and [[OutputSlot]] are allowed.
    * Otherwise, use [[PlanBuilder.customOperator()]].
    *
    * @param operator the custom [[Operator]]
    * @tparam T the output type of the `operator`
    * @return a new instance representing the output of the custom [[Operator]]
    */
  def customOperator[T](operator: Operator): DataQuanta[T] = {
    Validate.isTrue(
      operator.getNumInputs == 1 && operator.getNumOutputs == 1,
      "customOperator() accepts only operators with a single input and output. Use PlanBuilder.customOperator(...)."
    )
    planBuilder.customOperator(operator, this)(0).asInstanceOf[DataQuanta[T]]
  }

  /**
    * Use a broadcast in the [[Operator]] that creates this instance.
    *
    * @param sender        provides the broadcast data quanta
    * @param broadcastName the name with that the broadcast will be registered
    * @return this instance
    */
  def withBroadcast(sender: DataQuanta[_], broadcastName: String) = {
    require(this.planBuilder eq sender.planBuilder, s"$this and $sender must use the same plan builders.")
    sender.broadcast(this, broadcastName)
    this
  }

  /**
    * Broadcasts the data quanta in this instance to a further instance.
    *
    * @param receiver      the instance that receives the broadcast
    * @param broadcastName the name with that the broadcast will be registered
    */
  private def broadcast(receiver: DataQuanta[_], broadcastName: String) =
    receiver.registerBroadcast(this.operator, this.outputIndex, broadcastName)

  /**
    * Register a further instance as broadcast.
    *
    * @param sender        provides the broadcast data quanta
    * @param outputIndex   identifies the output index of the sender
    * @param broadcastName the name with that the broadcast will be registered
    */
  private def registerBroadcast(sender: Operator, outputIndex: Int, broadcastName: String) =
    sender.broadcastTo(outputIndex, this.operator, broadcastName)


  /**
    * Perform a local action on each data quantum in this instance. Triggers execution.
    *
    * @param f the action to perform
    */
  def foreach(f: Out => _): Unit = foreachJava(toConsumer(f))

  /**
    * Perform a local action on each data quantum in this instance. Triggers execution.
    *
    * @param f the action to perform as Java 8 lambda expression
    */
  def foreachJava(f: Consumer[Out]): Unit = {
    val sink = new LocalCallbackSink(f, dataSetType[Out])
    sink.setName("foreach()")
    this.connectTo(sink, 0)
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()
  }

  /**
    * Collect the data quanta in this instance. Triggers execution.
    *
    * @return the data quanta
    */
  def collect(): Iterable[Out] = {
    // Set up the sink.
    val collector = new java.util.LinkedList[Out]()
    val sink = LocalCallbackSink.createCollectingSink(collector, dataSetType[Out])
    sink.setName("collect()")
    this.connectTo(sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()

    // Return the collected values.
    collector
  }

  def explain(toJson: Boolean = false): Unit = {
    // Set up the sink.
    val collector = new java.util.LinkedList[Out]()
    val sink = LocalCallbackSink.createCollectingSink(collector, dataSetType[Out])
    sink.setName("explain()")
    this.connectTo(sink, 0)

    // Do the explanation.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExplain(toJson)
  }

  /**
    * Write the data quanta in this instance to a text file. Triggers execution.
    *
    * @param url          URL to the text file
    * @param formatterUdf UDF to format data quanta to [[String]]s
    * @param udfLoad      optional [[LoadProfileEstimator]] for the `udf`
    */
  def writeTextFile(url: String,
                    formatterUdf: Out => String,
                    udfLoad: LoadProfileEstimator = null): Unit = {
    writeTextFileJava(url, toSerializableFunction(formatterUdf), udfLoad)
  }

  /**
    * Write the data quanta in this instance to a iceberg table. Triggers execution.
    *
    * @param catalog          Iceberg Catalog
    * @param schema           Iceberg Schema of the table to create
    * @param tableIdentifier  Iceberg Table Identifier of the table to create
    * @param outputFileFormat File format of the output data files
    */

  def writeIcebergTable(catalog: Catalog, 
                      schema: Schema, 
                      tableIdentifier: TableIdentifier,
                      outputFileFormat: FileFormat): Unit = {
    writeIcebergTableJava(catalog, schema, tableIdentifier, outputFileFormat)
  }

  /**
    * Write the data quanta in this instance to a iceberg table. Triggers execution.
    *
    * @param catalog          Iceberg Catalog
    * @param schema           Iceberg Schema of the table to create
    * @param tableIdentifier  Iceberg Table Identifier of the table to create
    * @param outputFileFormat File format of the output data files
    */
  def writeIcebergTableJava(
    catalog: Catalog, 
    schema: Schema, 
    tableIdentifier: TableIdentifier,
    outputFileFormat: FileFormat ): Unit = {
    
    val sink = new ApacheIcebergSink(catalog, schema, tableIdentifier, outputFileFormat)

    sink.setName(s"*#-> Write to Iceberg Table Sink ")
    this.connectTo(sink, 0)
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()
  }
  def writeParquet(url: String,
                   overwrite: Boolean = false,
                   preferDataset: Boolean = false)(implicit ev: Out =:= Record): Unit =
    writeParquetJava(url, overwrite, preferDataset)

 /**
  * Write the data quanta in this instance to a text file. Triggers execution.
  *
  * @param topicName    topicName to write into
  * @param formatterUdf UDF to format data quanta to [[String]]s
  * @param udfLoad      optional [[LoadProfileEstimator]] for the `udf`
  */
  def writeKafkaTopic(topicName: String,
                  formatterUdf: Out => String,
                  udfLoad: LoadProfileEstimator = null): Unit = {
     writeKafkaTopicJava(topicName, toSerializableFunction(formatterUdf), udfLoad)
  }

    /**
      * Write the data quanta in this instance to a text file. Triggers execution.
      *
      * @param url          URL to the text file
      * @param formatterUdf UDF to format data quanta to [[String]]s
      * @param udfLoad      optional [[LoadProfileEstimator]] for the `udf`
      */
    def writeKafkaTopicJava(topicName: String,
                          formatterUdf: SerializableFunction[Out, String],
                          udfLoad: LoadProfileEstimator = null): Unit = {

      val sink = new KafkaTopicSink[Out](
        topicName,
        new TransformationDescriptor(formatterUdf, basicDataUnitType[Out], basicDataUnitType[String], udfLoad)
      )
      sink.setName(s"*#-> Write to KafkaTopic $topicName")
      println(s"*#-> Write to KafkaTopic $topicName")
      this.connectTo(sink, 0)

      // Do the execution.
      this.planBuilder.sinks += sink
      this.planBuilder.buildAndExecute()
      this.planBuilder.sinks.clear()
    }

    /**
    * Write the data quanta in this instance to a database table. Triggers execution.
    *
    * @param tableName   name of the target table
    * @param mode        write mode (e.g., "overwrite" or "append")
    * @param columnNames names of the columns in the target table
    * @param props       database connection properties
    */
  def writeTable(tableName: String,
                 mode: String,
                 columnNames: Array[String],
                 props: java.util.Properties): Unit = {
    val sink = new TableSink[Out](props, mode, tableName, columnNames: _*)
    sink.setName(s"Write to table $tableName")
    this.connectTo(sink, 0)
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()
  }
  
  /**
    * Write the data quanta in this instance to a text file. Triggers execution.
    *
    * @param url          URL to the text file
    * @param formatterUdf UDF to format data quanta to [[String]]s
    * @param udfLoad      optional [[LoadProfileEstimator]] for the `udf`
    */
  def writeTextFileJava(url: String,
                        formatterUdf: SerializableFunction[Out, String],
                        udfLoad: LoadProfileEstimator = null): Unit = {
    val sink = new TextFileSink[Out](
      url,
      new TransformationDescriptor(formatterUdf, basicDataUnitType[Out], basicDataUnitType[String], udfLoad)
    )
    sink.setName(s"Write to $url")

    this.connectTo(sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()
  }


  private def writeParquetJava(url: String, overwrite: Boolean, preferDataset: Boolean)(implicit ev: Out =:= Record): Unit = {
    val _ = ev
    val sink = new ParquetSink(url, overwrite, preferDataset)
    sink.setName(s"Write parquet $url")
    this.connectTo(sink, 0)
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()
  }

  /**
   * Write the data quanta in this instance to a Object file. Triggers execution.
   *
   * @param url          URL to the text file
   */
  def writeObjectFile(url: String)(implicit classTag: ClassTag[Out]): Unit = {
    writeObjectFileJava(url, classTag)
  }

  /**
   * Write the data quanta in this instance to a Object file. Triggers execution.
   *
   * @param url          URL to the text file
   */
  def writeObjectFileJava(url: String, classTag: ClassTag[Out]): Unit ={
    val sink = new ObjectFileSink[Out](
      url,
      basicDataUnitType(classTag).getTypeClass
    )
    sink.setName(s"Write objects to $url")
    this.connectTo(sink, 0)

    // Do the execution.
    this.planBuilder.sinks += sink
    this.planBuilder.buildAndExecute()
    this.planBuilder.sinks.clear()
  }

  /**
    * Restrict the producing [[Operator]] to run on certain [[Platform]]s.
    *
    * @param platforms on that the [[Operator]] may be executed
    * @return this instance
    */
  def withTargetPlatforms(platforms: Platform*) = {
    platforms.foreach(this.operator.addTargetPlatform)
    this
  }

  /**
    * Set a name for the [[Operator]] that creates this instance.
    *
    * @param name the name
    * @return this instance
    */
  def withName(name: String) = {
    this.operator.setName(name)
    this
  }

  /**
    * Sets a [[CardinalityEstimator]] for the [[Operator]] that creates this instance.
    *
    * @param estimator that should be set
    * @return this instance
    */
  def withCardinalityEstimator(estimator: CardinalityEstimator) = {
    this.operator.setCardinalityEstimator(outputIndex, estimator)
    this
  }

  /**
    * Defines user-code JAR files that might be needed to transfer to execution platforms.
    *
    * @param paths paths to JAR files that should be transferred
    * @return this instance
    */
  def withUdfJars(paths: String*) = {
    this.planBuilder withUdfJars (paths: _*)
    this
  }


  /**
    * Defines the [[Experiment]] that should collects metrics of the [[WayangPlan]].
    *
    * @param experiment the [[Experiment]]
    * @return this instance
    */
  def withExperiment(experiment: Experiment) = {
    this.planBuilder withExperiment experiment
    this
  }

  /**
    * Defines user-code JAR files that might be needed to transfer to execution platforms.
    *
    * @param classes whose JAR files should be transferred
    * @return this instance
    */
  def withUdfJarsOf(classes: Class[_]*) = {
    this.planBuilder withUdfJarsOf (classes: _*)
    this
  }

  override def toString = s"DataQuanta[$output]"

}

/**
  * This class provides operations on [[DataQuanta]] with additional operations.
  */
class KeyedDataQuanta[Out: ClassTag, Key: ClassTag](val dataQuanta: DataQuanta[Out],
                                                    val keyExtractor: SerializableFunction[Out, Key]) {

  /**
    * Performs a join. The join fields are governed by the [[KeyedDataQuanta]]'s keys.
    *
    * @param that the other [[KeyedDataQuanta]] to join with
    * @return the join product [[DataQuanta]]
    */
  def join[ThatOut: ClassTag](that: KeyedDataQuanta[ThatOut, Key]):
  DataQuanta[WayangTuple2[Out, ThatOut]] =
    dataQuanta.joinJava[ThatOut, Key](this.keyExtractor, that.dataQuanta, that.keyExtractor)

  /**
    * Performs a co-group. The grouping fields are governed by the [[KeyedDataQuanta]]'s keys.
    *
    * @param that the other [[KeyedDataQuanta]] to co-group with
    * @return the co-grouped [[DataQuanta]]
    */
  def coGroup[ThatOut: ClassTag](that: KeyedDataQuanta[ThatOut, Key]):
  DataQuanta[WayangTuple2[java.lang.Iterable[Out], java.lang.Iterable[ThatOut]]] =
    dataQuanta.coGroupJava[ThatOut, Key](this.keyExtractor, that.dataQuanta, that.keyExtractor)

}

/**
  * This class amends joined [[DataQuanta]] with additional operations.
  */
class JoinedDataQuanta[Out0: ClassTag, Out1: ClassTag]
(val dataQuanta: DataQuanta[WayangTuple2[Out0, Out1]]) {

  /**
    * Assembles a new element from a join product tuple.
    *
    * @param udf     creates the output data quantum from two joinable data quanta
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return the join product [[DataQuanta]]
    */
  def assemble[NewOut: ClassTag](udf: (Out0, Out1) => NewOut,
                                 udfLoad: LoadProfileEstimator = null):
  DataQuanta[NewOut] =
    dataQuanta.map(joinTuple => udf.apply(joinTuple.field0, joinTuple.field1), udfLoad)

  /**
    * Assembles a new element from a join product tuple.
    *
    * @param assembler creates the output data quantum from two joinable data quanta
    * @param udfLoad optional [[LoadProfileEstimator]] for the `udf`
    * @return the join product [[DataQuanta]]
    */
  def assembleJava[NewOut: ClassTag](assembler: JavaBiFunction[Out0, Out1, NewOut],
                                     udfLoad: LoadProfileEstimator = null): DataQuanta[NewOut] =
    dataQuanta.map(join => assembler.apply(join.field0, join.field1), udfLoad)

}

/**
 * Companion object for [[DataQuanta]].
 *
 * Provides factory methods to create [[DataQuanta]] instances directly
 * from [[OutputSlot]]s.
 */
object DataQuanta {

  def create[T](output: OutputSlot[T])(implicit planBuilder: PlanBuilder): DataQuanta[_] =
    new DataQuanta(output.getOwner.asInstanceOf[ElementaryOperator], output.getIndex)(ClassTag(output.getType.getDataUnitType.getTypeClass), planBuilder)

}
