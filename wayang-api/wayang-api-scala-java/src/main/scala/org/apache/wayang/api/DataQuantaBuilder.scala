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
 * TODO: add unitary test to the elements in the file org.apache.wayang.api.DataQuantaBuilder.scala
 * labels: unitary-test,todo
 */
import java.util.function.{Consumer, IntUnaryOperator, Function => JavaFunction}
import java.util.{Collection => JavaCollection}
import org.apache.wayang.api.graph.{Edge, EdgeDataQuantaBuilder, EdgeDataQuantaBuilderDecorator}
import org.apache.wayang.api.util.{DataQuantaBuilderCache, TypeTrap}
import org.apache.wayang.basic.data.{Record, Tuple2 => RT2}
import org.apache.wayang.basic.model.{DLModel, Model, LogisticRegressionModel,DecisionTreeRegressionModel}
import org.apache.wayang.basic.operators.{DLTrainingOperator, GlobalReduceOperator, JoinOperator, LocalCallbackSink, MapOperator, ReduceByOperator, SampleOperator, SortOperator, LogisticRegressionOperator,DecisionTreeRegressionOperator, LinearSVCOperator}
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.spatial.{SpatialGeometry, SpatialPredicate}
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableBiFunction, SerializableBinaryOperator, SerializableFunction, SerializableIntUnaryOperator, SerializablePredicate}
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator
import org.apache.wayang.core.optimizer.costs.{LoadEstimator, LoadProfile, LoadProfileEstimator}
import org.apache.wayang.core.plan.wayangplan.{Operator, OutputSlot, UnarySource, WayangPlan}
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.core.types.DataSetType
import org.apache.wayang.core.util.{Logging, ReflectionUtils, WayangCollections, Tuple => WayangTuple}
import org.apache.wayang.core.plan.wayangplan.OutputSlot

import org.apache.iceberg.Schema
import org.apache.iceberg.FileFormat
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.annotation.varargs

/**
  * Trait/interface for builders of [[DataQuanta]]. The purpose of the builders is to provide a convenient
  * Java API for Wayang that compensates for lacking default and named arguments.
  */
trait DataQuantaBuilder[+This <: DataQuantaBuilder[_, Out], Out] extends Logging {

  /**
    * The type of the [[DataQuanta]] to be built.
    */
  protected[api] def outputTypeTrap: TypeTrap

  /**
    * Provide a [[JavaPlanBuilder]] to which this instance is associated.
    */
  protected[api] implicit def javaPlanBuilder: JavaPlanBuilder

  /**
    * Set a name for the [[DataQuanta]] and its associated [[org.apache.wayang.core.plan.wayangplan.Operator]]s.
    *
    * @param name the name
    * @return this instance
    */
  def withName(name: String): This

  /**
    * Set an [[Experiment]] for the currently built [[org.apache.wayang.core.api.Job]].
    *
    * @param experiment the [[Experiment]]
    * @return this instance
    */
  def withExperiment(experiment: Experiment): This

  /**
    * Explicitly set an output [[DataSetType]] for the currently built [[DataQuanta]]. Note that it is not
    * always necessary to set it and that it can be inferred in some situations.
    *
    * @param outputType the output [[DataSetType]]
    * @return this instance
    */
  def withOutputType(outputType: DataSetType[Out]): This

  /**
    * Explicitly set an output [[Class]] for the currently built [[DataQuanta]]. Note that it is not
    * always necessary to set it and that it can be inferred in some situations.
    *
    * @param cls the output [[Class]]
    * @return this instance
    */
  def withOutputClass(cls: Class[Out]): This

  /**
    * Register a broadcast with the [[DataQuanta]] to be built
    *
    * @param sender        a [[DataQuantaBuilder]] constructing the broadcasted [[DataQuanta]]
    * @param broadcastName the name of the broadcast
    * @return this instance
    */
  def withBroadcast[Sender <: DataQuantaBuilder[_, _]](sender: Sender, broadcastName: String): This

  /**
    * Set a [[CardinalityEstimator]] for the currently built [[DataQuanta]].
    *
    * @param cardinalityEstimator the [[CardinalityEstimator]]
    * @return this instance
    */
  def withCardinalityEstimator(cardinalityEstimator: CardinalityEstimator): This

  /**
    * Add a target [[Platform]] on which the currently built [[DataQuanta]] should be calculated. Can be invoked
    * multiple times to set multiple possilbe target [[Platform]]s or not at all to impose no restrictions.
    *
    * @param platform the [[CardinalityEstimator]]
    * @return this instance
    */
  def withTargetPlatform(platform: Platform): This

  /**
    * Register the JAR file containing the given [[Class]] with the currently built [[org.apache.wayang.core.api.Job]].
    *
    * @param cls the [[Class]]
    * @return this instance
    */
  def withUdfJarOf(cls: Class[_]): This

  /**
    * Register a JAR file with the currently built [[org.apache.wayang.core.api.Job]].
    *
    * @param path the path of the JAR file
    * @return this instance
    */
  def withUdfJar(path: String): This

  /**
    * Provide a [[ClassTag]] for the constructed [[DataQuanta]].
    *
    * @return the [[ClassTag]]
    */
  protected[api] implicit def classTag: ClassTag[Out] = ClassTag(outputTypeTrap.typeClass)

  /**
    * Feed the built [[DataQuanta]] into a [[MapOperator]].
    *
    * @param udf the UDF for the [[MapOperator]]
    * @return a [[MapDataQuantaBuilder]]
    */
  def map[NewOut](udf: SerializableFunction[Out, NewOut]) = new MapDataQuantaBuilder(this, udf)

  /**
    * Feed the built [[DataQuanta]] into a [[MapOperator]] with a [[org.apache.wayang.basic.function.ProjectionDescriptor]].
    *
    * @param fieldNames field names for the [[org.apache.wayang.basic.function.ProjectionDescriptor]]
    * @return a [[MapDataQuantaBuilder]]
    */
  def project[NewOut](fieldNames: Array[String]) = new ProjectionDataQuantaBuilder(this, fieldNames)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.FilterOperator]].
    *
    * @param udf filter UDF
    * @return a [[FilterDataQuantaBuilder]]
    */
  def filter(udf: SerializablePredicate[Out]) = new FilterDataQuantaBuilder(this, udf)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.FlatMapOperator]].
    *
    * @param udf the UDF for the [[org.apache.wayang.basic.operators.FlatMapOperator]]
    * @return a [[FlatMapDataQuantaBuilder]]
    */
  def flatMap[NewOut](udf: SerializableFunction[Out, java.lang.Iterable[NewOut]]) = new FlatMapDataQuantaBuilder(this, udf)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.MapPartitionsOperator]].
    *
    * @param udf the UDF for the [[org.apache.wayang.basic.operators.MapPartitionsOperator]]
    * @return a [[MapPartitionsDataQuantaBuilder]]
    */
  def mapPartitions[NewOut](udf: SerializableFunction[java.lang.Iterable[Out], java.lang.Iterable[NewOut]]) =
    new MapPartitionsDataQuantaBuilder(this, udf)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.SampleOperator]].
    *
    * @param sampleSize the absolute size of the sample
    * @return a [[SampleDataQuantaBuilder]]
    */
  def sample(sampleSize: Int): SampleDataQuantaBuilder[Out] = this.sample(new SerializableIntUnaryOperator {
    override def applyAsInt(operand: Int): Int = sampleSize
  })


  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.SampleOperator]].
    *
    * @param sampleSizeFunction the absolute size of the sample as a function of the current iteration number
    * @return a [[SampleDataQuantaBuilder]]
    */
  def sample(sampleSizeFunction: SerializableIntUnaryOperator) = new SampleDataQuantaBuilder[Out](this, sampleSizeFunction)

  /**
    * Annotates a key to this instance.
    * @param keyExtractor extracts the key from the data quanta
    * @return a [[KeyedDataQuantaBuilder]]
    */
  def keyBy[Key](keyExtractor: SerializableFunction[Out, Key]) = new KeyedDataQuantaBuilder[Out, Key](this, keyExtractor)

  /**
    * Feed the built [[DataQuanta]] into a [[GlobalReduceOperator]].
    *
    * @param udf the UDF for the [[GlobalReduceOperator]]
    * @return a [[GlobalReduceDataQuantaBuilder]]
    */
  def reduce(udf: SerializableBinaryOperator[Out]) = new GlobalReduceDataQuantaBuilder(this, udf)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.ReduceByOperator]].
    *
    * @param keyUdf the key UDF for the [[org.apache.wayang.basic.operators.ReduceByOperator]]
    * @param udf    the UDF for the [[org.apache.wayang.basic.operators.ReduceByOperator]]
    * @return a [[ReduceByDataQuantaBuilder]]
    */
  def reduceByKey[Key](keyUdf: SerializableFunction[Out, Key], udf: SerializableBinaryOperator[Out]) =
    new ReduceByDataQuantaBuilder(this, keyUdf, udf)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.MaterializedGroupByOperator]].
    *
    * @param keyUdf the key UDF for the [[org.apache.wayang.basic.operators.MaterializedGroupByOperator]]
    * @return a [[GroupByDataQuantaBuilder]]
    */
  def groupByKey[Key](keyUdf: SerializableFunction[Out, Key]) =
    new GroupByDataQuantaBuilder(this, keyUdf)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.GlobalMaterializedGroupOperator]].
    *
    * @return a [[GlobalGroupDataQuantaBuilder]]
    */
  def group() = new GlobalGroupDataQuantaBuilder(this)

  /**
    * Feed the built [[DataQuanta]] of this and the given instance into a
    * [[org.apache.wayang.basic.operators.UnionAllOperator]].
    *
    * @param that the other [[DataQuantaBuilder]] to union with
    * @return a [[UnionDataQuantaBuilder]]
    */
  def union(that: DataQuantaBuilder[_, Out]) = new UnionDataQuantaBuilder(this, that)

  /**
    * Feed the built [[DataQuanta]] of this and the given instance into a
    * [[org.apache.wayang.basic.operators.IntersectOperator]].
    *
    * @param that the other [[DataQuantaBuilder]] to intersect with
    * @return an [[IntersectDataQuantaBuilder]]
    */
  def intersect(that: DataQuantaBuilder[_, Out]) = new IntersectDataQuantaBuilder(this, that)

  /**
    * Feed the built [[DataQuanta]] of this and the given instance into a
    * [[org.apache.wayang.basic.operators.JoinOperator]].
    *
    * @param thisKeyUdf the key extraction UDF for this instance
    * @param that       the other [[DataQuantaBuilder]] to join with
    * @param thatKeyUdf the key extraction UDF for `that` instance
    * @return a [[JoinDataQuantaBuilder]]
    */
  def join[ThatOut, Key](thisKeyUdf: SerializableFunction[Out, Key],
                         that: DataQuantaBuilder[_, ThatOut],
                         thatKeyUdf: SerializableFunction[ThatOut, Key]) =
    new JoinDataQuantaBuilder(this, that, thisKeyUdf, thatKeyUdf)

  def semanticFilter(prompt: String) = 
    new SemanticFilterDataQuantaBuilder[Out](this, prompt)

  /**
    * Feed the built [[DataQuanta]] into a spatial filter operator.
    * Requires the wayang-spatial plugin to be loaded.
    *
    * @param keyUdf          function to extract geometry from elements
    * @param predicate       the spatial predicate type
    * @param filterGeometry  the geometry to filter against
    * @return a [[DataQuantaBuilder]] representing the filtered output
    */
  def spatialFilter(
      keyUdf: SerializableFunction[Out, _ <: SpatialGeometry],
      predicate: SpatialPredicate,
      filterGeometry: SpatialGeometry
  ): SpatialFilterDataQuantaBuilder[Out] =
    new SpatialFilterDataQuantaBuilder(this, keyUdf, predicate, filterGeometry)

  /**
    * Feed the built [[DataQuanta]] into a spatial filter operator with SQL pushdown support.
    *
    * @param keyUdf              function to extract geometry from elements
    * @param predicate           the spatial predicate type
    * @param filterGeometry      the geometry to filter against
    * @param sqlGeometryColumn   the name of the geometry column in the database for SQL pushdown
    * @return a [[SpatialFilterDataQuantaBuilder]] representing the filtered output
    */
  def spatialFilter(
      keyUdf: SerializableFunction[Out, _ <: SpatialGeometry],
      predicate: SpatialPredicate,
      filterGeometry: SpatialGeometry,
      sqlGeometryColumn: String
  ): SpatialFilterDataQuantaBuilder[Out] =
    new SpatialFilterDataQuantaBuilder(this, keyUdf, predicate, filterGeometry)
      .withSqlGeometryColumnName(sqlGeometryColumn)

  /**
    * Feed the built [[DataQuanta]] of this and the given instance into a spatial join operator.
    *
    * @param thisKeyUdf  function to extract geometry from this instance's elements
    * @param that        the other [[DataQuantaBuilder]] to join with
    * @param thatKeyUdf  function to extract geometry from `that` instance's elements
    * @param predicate   the spatial predicate type
    * @return a [[SpatialJoinDataQuantaBuilder]] representing the joined output as Tuple2
    */
  def spatialJoin[ThatOut](
      thisKeyUdf: SerializableFunction[Out, _ <: SpatialGeometry],
      that: DataQuantaBuilder[_, ThatOut],
      thatKeyUdf: SerializableFunction[ThatOut, _ <: SpatialGeometry],
      predicate: SpatialPredicate
  ): SpatialJoinDataQuantaBuilder[Out, ThatOut] =
    new SpatialJoinDataQuantaBuilder(this, that, thisKeyUdf, thatKeyUdf, predicate)

  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[org.apache.wayang.basic.operators.DLTrainingOperator]].
   *
   * @param that       the other [[DataQuantaBuilder]] to join with
   * @param model      model for the [[org.apache.wayang.basic.operators.DLTrainingOperator]]
   * @param option     option for the [[org.apache.wayang.basic.operators.DLTrainingOperator]]
   * @return a [[DLTrainingDataQuantaBuilder]]
   */
  def dlTraining[ThatOut](that: DataQuantaBuilder[_, ThatOut],
                          model: DLModel,
                          option: DLTrainingOperator.Option) =
    new DLTrainingDataQuantaBuilder(this, that, model, option)


  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[org.apache.wayang.basic.operators.LogisticRegressionOperator]].
   * This operator trains a logistic regression model using the provided features and labels.
   *
   * @param that          the [[DataQuantaBuilder]] containing the label values (0.0 or 1.0)
   * @param fitIntercept  whether to include an intercept term in the model
   * @return a [[LogisticRegressionDataQuantaBuilder]] for the trained [[LogisticRegressionModel]]
   */
  def trainLogisticRegression(that: DataQuantaBuilder[_, java.lang.Double], fitIntercept: Boolean = true): LogisticRegressionDataQuantaBuilder =
    new LogisticRegressionDataQuantaBuilder(this.asInstanceOf[DataQuantaBuilder[_, Array[Double]]], that, fitIntercept)


  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[DecisionTreeRegressionOperator]].
   * This operator trains a generic Decision Tree Regression model using input features and labels.
   *
   * @param that         the [[DataQuantaBuilder]] containing the label values
   * @param maxDepth     the maximum depth of the decision tree
   * @param minInstances the minimum number of instances per node in the tree
   * @return a [[DataQuantaBuilder]] containing the predicted output values
   */
  def trainDecisionTreeRegression(
                                   that: DataQuantaBuilder[_, java.lang.Double],
                                   maxDepth: Int,
                                   minInstances: Int
                                 ): DataQuantaBuilder[_, DecisionTreeRegressionModel] =
    new CustomOperatorDataQuantaBuilder[DecisionTreeRegressionModel](
      new DecisionTreeRegressionOperator(maxDepth, minInstances),
      0,
      new DataQuantaBuilderCache,
      this,
      that
    )

  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a [[LinearSVCOperator]].
   * This operator trains a linear support vector classification (SVM) model using input features and binary labels.
   *
   * @param that      the [[DataQuantaBuilder]] containing the label values
   * @param maxIter   the maximum number of optimization iterations
   * @param regParam  the regularization strength for the SVM model
   * @return a [[DataQuantaBuilder]] containing the trained [[SVMModel]]
   */
  def trainLinearSVC(
                      that: DataQuantaBuilder[_, java.lang.Double],
                      maxIter: Int,
                      regParam: Double
                    ): DataQuantaBuilder[_, org.apache.wayang.basic.model.SVMModel] =
    new CustomOperatorDataQuantaBuilder[org.apache.wayang.basic.model.SVMModel](
      new LinearSVCOperator(maxIter, regParam),
      0,
      new DataQuantaBuilderCache,
      this,
      that
    )







  /**
   * Feed the built [[DataQuanta]] of this and the given instance into a
   * [[org.apache.wayang.basic.operators.PredictOperator]].
   *
   * @param that       the other [[DataQuantaBuilder]] to join with
   * @return a [[PredictDataQuantaBuilder]]
   */
  def predict[ThatOut, Result](that: DataQuantaBuilder[_, ThatOut], resultType: Class[Result]) =
    new PredictDataQuantaBuilder(this.asInstanceOf[DataQuantaBuilder[_, Model]], that, resultType)



  /**
    * Feed the built [[DataQuanta]] of this and the given instance into a
    * [[org.apache.wayang.basic.operators.CoGroupOperator]].
    *
    * @param thisKeyUdf the key extraction UDF for this instance
    * @param that       the other [[DataQuantaBuilder]] to join with
    * @param thatKeyUdf the key extraction UDF for `that` instance
    * @return a [[CoGroupDataQuantaBuilder]]
    */
  def coGroup[ThatOut, Key](thisKeyUdf: SerializableFunction[Out, Key],
                         that: DataQuantaBuilder[_, ThatOut],
                         thatKeyUdf: SerializableFunction[ThatOut, Key]) =
    new CoGroupDataQuantaBuilder(this, that, thisKeyUdf, thatKeyUdf)


  /**
    * Feed the built [[DataQuanta]] of this and the given instance into a
    * [[org.apache.wayang.basic.operators.SortOperator]].
    *
    * @param keyUdf the key extraction UDF for this instance
    * @return a [[SortDataQuantaBuilder]]
    */
  def sort[Key](keyUdf: SerializableFunction[Out, Key]) =
    new SortDataQuantaBuilder(this, keyUdf)

  /**
    * Feed the built [[DataQuanta]] of this and the given instance into a
    * [[org.apache.wayang.basic.operators.CartesianOperator]].
    *
    * @return a [[CartesianDataQuantaBuilder]]
    */
  def cartesian[ThatOut](that: DataQuantaBuilder[_, ThatOut]) = new CartesianDataQuantaBuilder(this, that)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.ZipWithIdOperator]].
    *
    * @return a [[ZipWithIdDataQuantaBuilder]] representing the [[org.apache.wayang.basic.operators.ZipWithIdOperator]]'s output
    */
  def zipWithId = new ZipWithIdDataQuantaBuilder(this)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.DistinctOperator]].
    *
    * @return a [[DistinctDataQuantaBuilder]] representing the [[org.apache.wayang.basic.operators.DistinctOperator]]'s output
    */
  def distinct = new DistinctDataQuantaBuilder(this)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.CountOperator]].
    *
    * @return a [[CountDataQuantaBuilder]] representing the [[org.apache.wayang.basic.operators.CountOperator]]'s output
    */
  def count = new CountDataQuantaBuilder(this)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.DoWhileOperator]].
    *
    * @return a [[DoWhileDataQuantaBuilder]]
    */
  def doWhile[Conv](conditionUdf: SerializablePredicate[JavaCollection[Conv]],
                    bodyBuilder: JavaFunction[DataQuantaBuilder[_, Out], WayangTuple[DataQuantaBuilder[_, Out], DataQuantaBuilder[_, Conv]]]) =
    new DoWhileDataQuantaBuilder(this, conditionUdf.asInstanceOf[SerializablePredicate[JavaCollection[Conv]]], bodyBuilder)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.RepeatOperator]].
    *
    * @return a [[DoWhileDataQuantaBuilder]]
    */
  def repeat(numRepetitions: Int, bodyBuilder: JavaFunction[DataQuantaBuilder[_, Out], DataQuantaBuilder[_, Out]]) =
    new RepeatDataQuantaBuilder(this, numRepetitions, bodyBuilder)

  /**
    * Feed the built [[DataQuanta]] into a custom [[Operator]] with a single [[org.apache.wayang.core.plan.wayangplan.InputSlot]]
    * and a single [[OutputSlot]].
    *
    * @param operator the custom [[Operator]]
    * @tparam T the type of the output [[DataQuanta]]
    * @return a [[CustomOperatorDataQuantaBuilder]]
    */
  def customOperator[T](operator: Operator) = {
    assert(operator.getNumInputs == 1, "customOperator(...) only allows for operators with a single input.")
    assert(operator.getNumOutputs == 1, "customOperator(...) only allows for operators with a single output.")
    new CustomOperatorDataQuantaBuilder[T](operator, 0, new DataQuantaBuilderCache, this)
  }

  /**
    * Feed the built [[DataQuanta]] into a [[LocalCallbackSink]] that collects all data quanta locally. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @return the collected data quanta
    */
  def collect(): JavaCollection[Out] = {
    import scala.collection.JavaConversions._
    this.dataQuanta().collect()
  }

  /**
    * Feed the built [[DataQuanta]] into a [[JavaFunction]] that runs locally. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @param f the [[JavaFunction]]
    * @return the collected data quanta
    */
  def forEach(f: Consumer[Out]): Unit = this.dataQuanta().foreachJava(f)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.TextFileSink]]. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @param url     the URL of the file to be written
    * @param jobName optional name for the [[WayangPlan]]
    * @return the collected data quanta
    */
  def writeTextFile(url: String, formatterUdf: SerializableFunction[Out, String], jobName: String): Unit =
    this.writeTextFile(url, formatterUdf, jobName, null)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.TextFileSink]]. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @param url the URL of the file to be written
    * @return the collected data quanta
    */
  def writeTextFile(url: String,
                    formatterUdf: SerializableFunction[Out, String],
                    jobName: String,
                    udfLoadProfileEstimator: LoadProfileEstimator): Unit = {
    this.javaPlanBuilder.withJobName(jobName)
    this.dataQuanta().writeTextFileJava(url, formatterUdf, udfLoadProfileEstimator)
  }

    /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.IcebergTableSink]]. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @param catalog          Iceberg Catalog
    * @param schema           Iceberg Schema of the table to create
    * @param tableIdentifier  Iceberg Table Identifier of the table to create
    * @param outputFileFormat File format of the output data files
    * @return the collected data quanta
    */

  def writeIcebergTable(catalog: Catalog,
                      schema: Schema,
                      tableIdentifier: TableIdentifier,
                      outputFileFormat: FileFormat,
                      jobName: String): Unit = {
    this.javaPlanBuilder.withJobName(jobName)
    this.dataQuanta().writeIcebergTableJava(catalog, schema, tableIdentifier, outputFileFormat)

  }

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.KafkaTopicSink]]. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @param topicName of the Kafka topic to be written
    * @return the collected data quanta
    */
  def writeKafkaTopic(topicName: String,
                    formatterUdf: Out => String,
                    jobName: String,
                    udfLoadProfileEstimator: LoadProfileEstimator): Unit = {
    this.javaPlanBuilder.withJobName(jobName)
    this.dataQuanta().writeKafkaTopic(topicName, formatterUdf, udfLoadProfileEstimator)
  }

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.ParquetSink]]. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @param url            URL of the Parquet file to be written
    * @param overwrite      whether to overwrite existing files
    * @param preferDataset  whether to prefer Spark Dataset over RDD
    */
  def writeParquet(url: String,
                   overwrite: Boolean,
                   preferDataset: Boolean): Unit =
    this.writeParquet(url, overwrite, preferDataset, null)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.ParquetSink]]. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @param url            URL of the Parquet file to be written
    * @param overwrite      whether to overwrite existing files
    * @param preferDataset  whether to prefer Spark Dataset over RDD
    * @param jobName        optional name for the [[WayangPlan]]
    */
  def writeParquet(url: String,
                   overwrite: Boolean,
                   preferDataset: Boolean,
                   jobName: String): Unit = {
    if (jobName != null) this.javaPlanBuilder.withJobName(jobName)
    this.dataQuanta().asInstanceOf[DataQuanta[Record]].writeParquet(url, overwrite, preferDataset)
  }

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.TableSink]]. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @param tableName   name of the target table
    * @param mode        write mode (e.g., "overwrite" or "append")
    * @param columnNames names of the columns in the target table
    * @param props       database connection properties
    */
  def writeTable(tableName: String,
                 mode: String,
                 columnNames: Array[String],
                 props: java.util.Properties): Unit =
    this.writeTable(tableName, mode, columnNames, props, null)

  /**
    * Feed the built [[DataQuanta]] into a [[org.apache.wayang.basic.operators.TableSink]]. This triggers
    * execution of the constructed [[WayangPlan]].
    *
    * @param tableName   name of the target table
    * @param mode        write mode (e.g., "overwrite" or "append")
    * @param columnNames names of the columns in the target table
    * @param props       database connection properties
    * @param jobName     optional name for the [[WayangPlan]]
    */
  def writeTable(tableName: String,
                 mode: String,
                 columnNames: Array[String],
                 props: java.util.Properties,
                 jobName: String): Unit = {
    if (jobName != null) this.javaPlanBuilder.withJobName(jobName)
    this.dataQuanta().writeTable(tableName, mode, columnNames, props)
  }

  /**
    * Enriches the set of operations to [[Record]]-based ones. This instances must deal with data quanta of
    * type [[Record]], though. Because of Java's type erasure, we need to leave it up to you whether this
    * operation is applicable.
    *
    * @return a [[RecordDataQuantaBuilder]]
    */
  def asRecords[T <: RecordDataQuantaBuilder[T]]: RecordDataQuantaBuilder[T] = {
    this match {
      case records: RecordDataQuantaBuilder[_] => records.asInstanceOf[RecordDataQuantaBuilder[T]]
      case _ => new RecordDataQuantaBuilderDecorator(this.asInstanceOf[DataQuantaBuilder[_, Record]])
    }
  }

  /**
    * Enriches the set of operations to [[Edge]]-based ones. This instances must deal with data quanta of
    * type [[Edge]], though. Because of Java's type erasure, we need to leave it up to you whether this
    * operation is applicable.
    *
    * @return a [[EdgeDataQuantaBuilder]]
    */
  def asEdges[T <: EdgeDataQuantaBuilder[T]]: EdgeDataQuantaBuilder[T] = {
    this match {
      case edges: RecordDataQuantaBuilder[_] => edges.asInstanceOf[EdgeDataQuantaBuilder[T]]
      case _ => new EdgeDataQuantaBuilderDecorator(this.asInstanceOf[DataQuantaBuilder[_, Edge]])
    }
  }

  /**
    * Get or create the [[DataQuanta]] built by this instance.
    *
    * @return the [[DataQuanta]]
    */
  protected[api] def dataQuanta(): DataQuanta[Out]

}

/**
  * Abstract base class for builders of [[DataQuanta]]. The purpose of the builders is to provide a convenient
  * Java API for Wayang that compensates for lacking default and named arguments.
  */
abstract class BasicDataQuantaBuilder[This <: DataQuantaBuilder[_, Out], Out](implicit _javaPlanBuilder: JavaPlanBuilder)
  extends Logging with DataQuantaBuilder[This, Out] {

  /**
    * Lazy-initialized. The [[DataQuanta]] product of this builder.
    */
  private var result: DataQuanta[Out] = _

  /**
    * A name for the [[DataQuanta]] to be built.
    */
  private var name: String = _

  /**
    * An [[Experiment]] for the [[DataQuanta]] to be built.
    */
  private var experiment: Experiment = _

  /**
    * Broadcasts for the [[DataQuanta]] to be built.
    */
  private val broadcasts: ListBuffer[(String, DataQuantaBuilder[_, _])] = ListBuffer()

  /**
    * [[CardinalityEstimator]] for the [[DataQuanta]] to be built.
    */
  private var cardinalityEstimator: CardinalityEstimator = _

  /**
    * Target [[Platform]]s for the [[DataQuanta]] to be built.
    */
  private val targetPlatforms: ListBuffer[Platform] = ListBuffer()

  /**
    * Paths of UDF JAR files for the [[DataQuanta]] to be built.
    */
  private val udfJars: ListBuffer[String] = ListBuffer()

  /**
    * The type of the [[DataQuanta]] to be built.
    */
  protected[api] val outputTypeTrap = getOutputTypeTrap

  /**
    * Retrieve an intialization value for [[outputTypeTrap]].
    *
    * @return the [[TypeTrap]]
    */
  protected def getOutputTypeTrap = new TypeTrap

  override protected[api] implicit def javaPlanBuilder = _javaPlanBuilder

  override def withName(name: String): This = {
    this.name = name
    this.asInstanceOf[This]
  }

  override def withExperiment(experiment: Experiment): This = {
    this.experiment = experiment
    this.asInstanceOf[This]
  }

  override def withOutputType(outputType: DataSetType[Out]): This = {
    this.outputTypeTrap.dataSetType = outputType
    this.asInstanceOf[This]
  }

  override def withOutputClass(cls: Class[Out]): This = this.withOutputType(DataSetType.createDefault(cls))

  override def withBroadcast[Sender <: DataQuantaBuilder[_, _]](sender: Sender, broadcastName: String): This = {
    this.broadcasts += Tuple2(broadcastName, sender)
    this.asInstanceOf[This]
  }

  override def withCardinalityEstimator(cardinalityEstimator: CardinalityEstimator): This = {
    this.cardinalityEstimator = cardinalityEstimator
    this.asInstanceOf[This]
  }

  override def withTargetPlatform(platform: Platform): This = {
    this.targetPlatforms += platform
    this.asInstanceOf[This] 
  }

  def getTargetPlatforms(): ListBuffer[Platform] = this.targetPlatforms

  def applyTargetPlatforms(op: DataQuanta[Out], targetPlatforms: ListBuffer[Platform]) = {
    targetPlatforms.foreach(platform => op.withTargetPlatforms(platform)) 
    op
  }

  def withUdfJarOf(cls: Class[_]): This = this.withUdfJar(ReflectionUtils.getDeclaringJar(cls))

  override def withUdfJar(path: String): This = {
    this.udfJars += path
    this.asInstanceOf[This]
  }

  override protected[api] implicit def classTag: ClassTag[Out] = ClassTag(outputTypeTrap.typeClass)

  override protected[api] def dataQuanta(): DataQuanta[Out] = {
    if (this.result == null) {
      this.result = this.build
      if (this.name != null) this.result.withName(this.name)
      if (this.cardinalityEstimator != null) this.result.withCardinalityEstimator(this.cardinalityEstimator)
      if (this.experiment != null) this.result.withExperiment(experiment)
      this.result.withUdfJars(this.udfJars: _*)
      this.result.withTargetPlatforms(this.targetPlatforms: _*)
      this.broadcasts.foreach {
        case (broadcastName, senderBuilder) => this.result.withBroadcast(senderBuilder.dataQuanta(), broadcastName)
      }
    }
    this.result
  }

  /**
   * Syntax sugar to avoid calling build().explain() on [[DataQuantaBuilder]]
   */
  def explain(): Unit = {
    this.dataQuanta().explain(false)
  }

  /**
    * Create the [[DataQuanta]] built by this instance. Note the configuration being done in [[dataQuanta()]].
    *
    * @return the created and partially configured [[DataQuanta]]
    */
  protected def build: DataQuanta[Out]


}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.core.plan.wayangplan.UnarySource]]s.
  *
  * @param source          the [[UnarySource]]
  * @param javaPlanBuilder the [[JavaPlanBuilder]]
  */
class UnarySourceDataQuantaBuilder[This <: DataQuantaBuilder[_, Out], Out](source: UnarySource[Out])
                                                                          (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[This, Out] {

  override protected def build: DataQuanta[Out] = applyTargetPlatforms(javaPlanBuilder.planBuilder.load(source)(this.classTag), this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.CollectionSource]]s.
  *
  * @param collection      the [[JavaCollection]] to be loaded
  * @param javaPlanBuilder the [[JavaPlanBuilder]]
  */
class LoadCollectionDataQuantaBuilder[Out](collection: JavaCollection[Out])(implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[LoadCollectionDataQuantaBuilder[Out], Out] {

  // Try to infer the type class from the collection.
  locally {
    if (!collection.isEmpty) {
      val any = WayangCollections.getAny(collection)
      if (any != null) {
        this.outputTypeTrap.dataSetType = DataSetType.createDefault(any.getClass)
      }
    }
  }

  override protected def build: DataQuanta[Out] = javaPlanBuilder.planBuilder.loadCollection(collection)(this.classTag)

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.MapOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param udf             UDF for the [[MapOperator]]
  */
class MapDataQuantaBuilder[In, Out](inputDataQuanta: DataQuantaBuilder[_, In],
                                    udf: SerializableFunction[In, Out])
                                   (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[MapDataQuantaBuilder[In, Out], Out] {

  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[udf]]. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  // Try to infer the type classes from the udf.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In] => inputDataQuanta.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
    parameters.get("Output") match {
      case cls: Class[Out] => this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
  }

  /**
    * Set a [[LoadProfileEstimator]] for the load of the UDF.
    *
    * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
    * @return this instance
    */
  def withUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.udfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().mapJava(udf, this.udfLoadProfileEstimator), this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.MapOperator]]s with
  * [[org.apache.wayang.basic.function.ProjectionDescriptor]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param fieldNames      field names for the [[org.apache.wayang.basic.function.ProjectionDescriptor]]
  */
class ProjectionDataQuantaBuilder[In, Out](inputDataQuanta: DataQuantaBuilder[_, In], fieldNames: Array[String])
                                          (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[ProjectionDataQuantaBuilder[In, Out], Out] {

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().project(fieldNames.toSeq), this.getTargetPlatforms())

}


/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.MapOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param udf             UDF for the [[MapOperator]]
  */
class FilterDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T], udf: SerializablePredicate[T])
                                (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[FilterDataQuantaBuilder[T], T] {

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[udf]]. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  /** Selectivity of the filter predicate. */
  private var selectivity: ProbabilisticDoubleInterval = _

  /** SQL UDF implementing the filter predicate. */
  private var sqlUdf: String = _

  // Try to infer the type classes from the udf.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[T] => this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
  }


  /**
    * Set a [[LoadProfileEstimator]] for the load of the UDF.
    *
    * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
    * @return this instance
    */
  def withUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.udfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  /**
    * Add a SQL implementation of the UDF.
    *
    * @param sqlUdf a SQL condition that can be plugged into a `WHERE` clause
    * @return this instance
    */
  def withSqlUdf(sqlUdf: String) = {
    this.sqlUdf = sqlUdf
    this
  }

  /**
    * Specify the selectivity of the UDF.
    *
    * @param lowerEstimate the lower bound of the expected selectivity
    * @param upperEstimate the upper bound of the expected selectivity
    * @param confidence    the probability of the actual selectivity being within these bounds
    * @return this instance
    */
  def withSelectivity(lowerEstimate: Double, upperEstimate: Double, confidence: Double) = {
    this.selectivity = new ProbabilisticDoubleInterval(lowerEstimate, upperEstimate, confidence)
    this
  }

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().filterJava(
    udf, this.sqlUdf, this.selectivity, this.udfLoadProfileEstimator
  ), this.getTargetPlatforms())

}


/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.SortOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param keyUdf             UDF for the [[org.apache.wayang.basic.operators.SortOperator]]
  */
class SortDataQuantaBuilder[T, Key](inputDataQuanta: DataQuantaBuilder[_, T],
                                    keyUdf: SerializableFunction[T, Key])
                                   (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[SortDataQuantaBuilder[T, Key], T] {

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  /** [[ClassTag]] or surrogate of [[Key]] */
  implicit var keyTag: ClassTag[Key] = _

  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf]]. */
  private var keyUdfCpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf]]. */
  private var keyUdfRamEstimator: LoadEstimator = _

  /** SQL column and direction implementing the sort key. */
  private var sqlColumnName: String = _
  private var sqlDirection: String = _


  // Try to infer the type classes from the UDFs.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(keyUdf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[T] => inputDataQuanta.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf)
    }

    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }


  /**
    * Set a [[LoadEstimator]] for the CPU load of the first key extraction UDF. Currently effectless.
    *
    * @param udfCpuEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThisKeyUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.keyUdfCpuEstimator = udfCpuEstimator
    this
  }

  /**
    * Set a [[LoadEstimator]] for the RAM load of first the key extraction UDF. Currently effectless.
    *
    * @param udfRamEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThisKeyUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.keyUdfRamEstimator = udfRamEstimator
    this
  }

  /**
    * Add a SQL implementation of the sort key.
    *
    * @param columnName SQL column to sort by
    * @param direction SQL sort direction, e.g. `ASC` or `DESC`
    * @return this instance
    */
  def withSqlUdf(columnName: String, direction: String) = {
    this.sqlColumnName = columnName
    this.sqlDirection = direction
    this
  }

  override protected def build = {
    val result = inputDataQuanta.dataQuanta().sortJava(keyUdf)(this.keyTag)
    if (this.sqlColumnName != null) {
      result.operator.asInstanceOf[SortOperator[T, Key]]
        .getKeyDescriptor.withSqlImplementation(this.sqlColumnName, this.sqlDirection)
    }
    applyTargetPlatforms(result, this.getTargetPlatforms())
  }

}


/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.FlatMapOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param udf             UDF for the [[org.apache.wayang.basic.operators.FlatMapOperator]]
  */
class FlatMapDataQuantaBuilder[In, Out](inputDataQuanta: DataQuantaBuilder[_, In],
                                        udf: SerializableFunction[In, java.lang.Iterable[Out]])
                                       (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[FlatMapDataQuantaBuilder[In, Out], Out] {


  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[udf]]. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  /** Selectivity of the filter predicate. */
  private var selectivity: ProbabilisticDoubleInterval = _

  // Try to infer the type classes from the udf.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In] => inputDataQuanta.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
    val originalClass = ReflectionUtils.getWrapperClass(parameters.get("Output"), 0)
    originalClass match {
      case cls: Class[Out] => {
        this.outputTypeTrap.dataSetType= DataSetType.createDefault(cls)
      }
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
  }

  /**
    * Set a [[LoadProfileEstimator]] for the load of the UDF.
    *
    * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
    * @return this instance
    */
  def withUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.udfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  /**
    * Specify the selectivity of the UDF.
    *
    * @param lowerEstimate the lower bound of the expected selectivity
    * @param upperEstimate the upper bound of the expected selectivity
    * @param confidence    the probability of the actual selectivity being within these bounds
    * @return this instance
    */
  def withSelectivity(lowerEstimate: Double, upperEstimate: Double, confidence: Double) = {
    this.selectivity = new ProbabilisticDoubleInterval(lowerEstimate, upperEstimate, confidence)
    this
  }

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().flatMapJava(
    udf, this.selectivity, this.udfLoadProfileEstimator
  ), this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.MapPartitionsOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param udf             UDF for the [[org.apache.wayang.basic.operators.MapPartitionsOperator]]
  */
class MapPartitionsDataQuantaBuilder[In, Out](inputDataQuanta: DataQuantaBuilder[_, In],
                                              udf: SerializableFunction[java.lang.Iterable[In], java.lang.Iterable[Out]])
                                             (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[MapPartitionsDataQuantaBuilder[In, Out], Out] {

  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[udf]]. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  /** Selectivity of the filter predicate. */
  private var selectivity: ProbabilisticDoubleInterval = _

  // Try to infer the type classes from the udf.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In] => {
        inputDataQuanta.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      }
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
    val originalClass = ReflectionUtils.getWrapperClass(parameters.get("Output"), 0)
    originalClass match {
      case cls: Class[Out] => {
        this.outputTypeTrap.dataSetType= DataSetType.createDefault(cls)
      }
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
  }


  /**
    * Set a [[LoadProfileEstimator]] for the load of the UDF.
    *
    * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
    * @return this instance
    */
  def withUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.udfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  /**
    * Specify the selectivity of the UDF.
    *
    * @param lowerEstimate the lower bound of the expected selectivity
    * @param upperEstimate the upper bound of the expected selectivity
    * @param confidence    the probability of the actual selectivity being within these bounds
    * @return this instance
    */
  def withSelectivity(lowerEstimate: Double, upperEstimate: Double, confidence: Double) = {
    this.selectivity = new ProbabilisticDoubleInterval(lowerEstimate, upperEstimate, confidence)
    this
  }

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().mapPartitionsJava(
    udf, this.selectivity, this.udfLoadProfileEstimator
  ), this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.SampleOperator]]s.
  *
  * @param inputDataQuanta    [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param sampleSizeFunction the absolute size of the sample as a function of the current iteration number
  */
class SampleDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T], sampleSizeFunction: SerializableIntUnaryOperator)
                                (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[SampleDataQuantaBuilder[T], T] {

  /**
    * Size of the dataset to be sampled.
    */
  private var datasetSize = SampleOperator.UNKNOWN_DATASET_SIZE

  /**
    * Sampling method to use.
    */
  private var sampleMethod = SampleOperator.Methods.ANY

  /**
    * Seed to use.
    */
  private var seed: Option[Long] = None

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  /**
    * Set the size of the dataset that should be sampled.
    *
    * @param datasetSize the size of the dataset
    * @return this instance
    */
  def withDatasetSize(datasetSize: Long) = {
    this.datasetSize = datasetSize
    this
  }

  /**
    * Set the sample method to be used.
    *
    * @param sampleMethod the sample method
    * @return this instance
    */
  def withSampleMethod(sampleMethod: SampleOperator.Methods) = {
    this.sampleMethod = sampleMethod
    this
  }

  /**
    * Set the sample method to be used.
    *
    * @param seed
    * @return this instance
    */
  def withSeed(seed: Long) = {
    this.seed = Some(seed)
    this
  }

  override protected def build =
    applyTargetPlatforms(inputDataQuanta.dataQuanta().sampleDynamicJava(sampleSizeFunction, this.datasetSize, this.seed, this.sampleMethod), this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.ReduceByOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param udf             UDF for the [[org.apache.wayang.basic.operators.ReduceByOperator]]
  * @param keyUdf          key extraction UDF for the [[org.apache.wayang.basic.operators.ReduceByOperator]]
  */
class ReduceByDataQuantaBuilder[Key, T](inputDataQuanta: DataQuantaBuilder[_, T],
                                        keyUdf: SerializableFunction[T, Key],
                                        udf: SerializableBinaryOperator[T])
                                       (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[ReduceByDataQuantaBuilder[Key, T], T] {

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  implicit var keyTag: ClassTag[Key] = _

  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[udf]]. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  /** SQL implementations of the grouping key and reduction. */
  private var keySqlUdf: String = _
  private var reduceSqlUdf: String = _

  // TODO: Add these estimators.
  //  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf]]. */
  //  private var keyUdfCpuEstimator: LoadEstimator = _
  //
  //  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf]]. */
  //  private var keyUdfRamEstimator: LoadEstimator = _

  // Try to infer the type classes from the UDFs.
  locally {
    var parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableBinaryOperator[_]])
    parameters.get("Type") match {
      case cls: Class[T] => this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }

    parameters = ReflectionUtils.getTypeParameters(keyUdf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[T] => this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf)
    }
    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }

  /**
    * Set a [[LoadProfileEstimator]] for the load of the UDF.
    *
    * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
    * @return this instance
    */
  def withUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.udfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  /**
    * Add SQL implementations of the grouping key and reduction.
    *
    * @param keySqlUdf SQL grouping column
    * @param reduceSqlUdf SQL aggregate expression
    * @return this instance
    */
  def withSqlUdfs(keySqlUdf: String, reduceSqlUdf: String) = {
    this.keySqlUdf = keySqlUdf
    this.reduceSqlUdf = reduceSqlUdf
    this
  }

  override protected def build = {
    val result = inputDataQuanta.dataQuanta()
      .reduceByKeyJava(keyUdf, udf, this.udfLoadProfileEstimator)
    if (this.keySqlUdf != null) {
      val operator = result.operator.asInstanceOf[ReduceByOperator[T, Key]]
      operator.getKeyDescriptor.withSqlImplementation(this.keySqlUdf, this.keySqlUdf)
      operator.getReduceDescriptor.withSqlImplementation(this.reduceSqlUdf)
    }
    applyTargetPlatforms(result, this.getTargetPlatforms())
  }
}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.MaterializedGroupByOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param keyUdf          key extraction UDF for the [[org.apache.wayang.basic.operators.MaterializedGroupByOperator]]
  */
class GroupByDataQuantaBuilder[Key, T](inputDataQuanta: DataQuantaBuilder[_, T], keyUdf: SerializableFunction[T, Key])
                                      (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[GroupByDataQuantaBuilder[Key, T], java.lang.Iterable[T]] {

  implicit var keyTag: ClassTag[Key] = _


  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[keyUdf]]. */
  private var keyUdfLoadProfileEstimator: LoadProfileEstimator = _

  // Try to infer the type classes from the UDFs.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(keyUdf.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[T] => this.outputTypeTrap.dataSetType = DataSetType.createGrouped(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf)
    }

    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }

  /**
    * Set a [[LoadProfileEstimator]] for the load of the UDF.
    *
    * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
    * @return this instance
    */
  def withKeyUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.keyUdfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().groupByKeyJava(keyUdf, this.keyUdfLoadProfileEstimator), this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.GlobalMaterializedGroupOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  */
class GlobalGroupDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T])(implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[GlobalGroupDataQuantaBuilder[T], java.lang.Iterable[T]] {

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().group(), this.getTargetPlatforms())

}


/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.GlobalReduceOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param udf             UDF for the [[org.apache.wayang.basic.operators.GlobalReduceOperator]]
  */
class GlobalReduceDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T],
                                       udf: SerializableBinaryOperator[T])
                                      (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[GlobalReduceDataQuantaBuilder[T], T] {

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[udf]]. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  /** SQL implementation of the reduction. */
  private var sqlUdf: String = _

  // Try to infer the type classes from the udf.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(udf.getClass, classOf[SerializableBinaryOperator[_]])
    parameters.get("Type") match {
      case cls: Class[T] => this.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", udf)
    }
  }

  /**
    * Set a [[LoadProfileEstimator]] for the load of the UDF.
    *
    * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
    * @return this instance
    */
  def withUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.udfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  /**
    * Add a SQL implementation of the reduction.
    *
    * @param sqlUdf SQL aggregate expression
    * @return this instance
    */
  def withSqlUdf(sqlUdf: String) = {
    this.sqlUdf = sqlUdf
    this
  }

  override protected def build = {
    val result = inputDataQuanta.dataQuanta().reduceJava(udf, this.udfLoadProfileEstimator)
    if (this.sqlUdf != null) {
      result.operator.asInstanceOf[GlobalReduceOperator[T]]
        .getReduceDescriptor.withSqlImplementation(this.sqlUdf)
    }
    applyTargetPlatforms(result, this.getTargetPlatforms())
  }

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.UnionAllOperator]]s.
  *
  * @param inputDataQuanta0 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  * @param inputDataQuanta1 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  */
class UnionDataQuantaBuilder[T](inputDataQuanta0: DataQuantaBuilder[_, T],
                                inputDataQuanta1: DataQuantaBuilder[_, T])
                               (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[UnionDataQuantaBuilder[T], T] {

  override def getOutputTypeTrap = inputDataQuanta0.outputTypeTrap

  override protected def build = applyTargetPlatforms(inputDataQuanta0.dataQuanta().union(inputDataQuanta1.dataQuanta()), this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.IntersectOperator]]s.
  *
  * @param inputDataQuanta0 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  * @param inputDataQuanta1 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  */
class IntersectDataQuantaBuilder[T](inputDataQuanta0: DataQuantaBuilder[_, T],
                                    inputDataQuanta1: DataQuantaBuilder[_, T])
                                   (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[IntersectDataQuantaBuilder[T], T] {

  override def getOutputTypeTrap = inputDataQuanta0.outputTypeTrap

  override protected def build = applyTargetPlatforms(inputDataQuanta0.dataQuanta().intersect(inputDataQuanta1.dataQuanta()), this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.JoinOperator]]s.
  *
  * @param inputDataQuanta0 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  * @param inputDataQuanta1 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  * @param keyUdf0          first key extraction UDF for the [[org.apache.wayang.basic.operators.JoinOperator]]
  * @param keyUdf1          first key extraction UDF for the [[org.apache.wayang.basic.operators.JoinOperator]]
  */
class JoinDataQuantaBuilder[In0, In1, Key](inputDataQuanta0: DataQuantaBuilder[_, In0],
                                           inputDataQuanta1: DataQuantaBuilder[_, In1],
                                           keyUdf0: SerializableFunction[In0, Key],
                                           keyUdf1: SerializableFunction[In1, Key])
                                          (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[JoinDataQuantaBuilder[In0, In1, Key], RT2[In0, In1]] {

  /** [[ClassTag]] or surrogate of [[Key]] */
  implicit var keyTag: ClassTag[Key] = _

  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf0]]. */
  private var keyUdf0CpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf0]]. */
  private var keyUdf0RamEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf1]]. */
  private var keyUdf1CpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf1]]. */
  private var keyUdf1RamEstimator: LoadEstimator = _

  /** SQL implementations of both join keys. */
  private var keyUdf0TableName: String = _
  private var keyUdf0SqlUdf: String = _
  private var keyUdf1TableName: String = _
  private var keyUdf1SqlUdf: String = _

  // Try to infer the type classes from the UDFs.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(keyUdf0.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In0] => inputDataQuanta0.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf0)
    }

    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf0)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }
  locally {
    val parameters = ReflectionUtils.getTypeParameters(keyUdf1.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In1] => inputDataQuanta1.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf0)
    }

    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf0)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }
  // Since we are currently not looking at type parameters, we can statically determine the output type.
  locally {
    this.outputTypeTrap.dataSetType = dataSetType[RT2[_, _]]
  }

  /**
    * Set a [[LoadEstimator]] for the CPU load of the first key extraction UDF. Currently effectless.
    *
    * @param udfCpuEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThisKeyUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.keyUdf0CpuEstimator = udfCpuEstimator
    this
  }

  /**
    * Set a [[LoadEstimator]] for the RAM load of first the key extraction UDF. Currently effectless.
    *
    * @param udfRamEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThisKeyUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.keyUdf0RamEstimator = udfRamEstimator
    this
  }

  /**
    * Set a [[LoadEstimator]] for the CPU load of the second key extraction UDF. Currently effectless.
    *
    * @param udfCpuEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThatKeyUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.keyUdf1CpuEstimator = udfCpuEstimator
    this
  }

  /**
    * Set a [[LoadEstimator]] for the RAM load of the second key extraction UDF. Currently effectless.
    *
    * @param udfRamEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThatKeyUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.keyUdf1RamEstimator = udfRamEstimator
    this
  }

  /**
    * Add SQL implementations of both join keys.
    *
    * @return this instance
    */
  def withSqlUdfs(thisTableName: String,
                  thisKeySqlUdf: String,
                  thatTableName: String,
                  thatKeySqlUdf: String) = {
    this.keyUdf0TableName = thisTableName
    this.keyUdf0SqlUdf = thisKeySqlUdf
    this.keyUdf1TableName = thatTableName
    this.keyUdf1SqlUdf = thatKeySqlUdf
    this
  }

  /**
    * Assemble the joined elements to new elements.
    *
    * @param udf produces a joined element from two joinable elements
    * @return a new [[DataQuantaBuilder]] representing the assembled join product
    */
  def assemble[NewOut](udf: SerializableBiFunction[In0, In1, NewOut]) =
    this.map(new SerializableFunction[RT2[In0, In1], NewOut] {
      override def apply(joinTuple: RT2[In0, In1]): NewOut = udf.apply(joinTuple.field0, joinTuple.field1)
    })

  override protected def build = {
    val result = inputDataQuanta0.dataQuanta()
      .joinJava(keyUdf0, inputDataQuanta1.dataQuanta(), keyUdf1)(inputDataQuanta1.classTag, this.keyTag)
    if (this.keyUdf0SqlUdf != null) {
      val operator = result.operator.asInstanceOf[JoinOperator[In0, In1, Key]]
      operator.getKeyDescriptor0.withSqlImplementation(this.keyUdf0TableName, this.keyUdf0SqlUdf)
      operator.getKeyDescriptor1.withSqlImplementation(this.keyUdf1TableName, this.keyUdf1SqlUdf)
    }
    applyTargetPlatforms(result, this.getTargetPlatforms())
  }

}

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.DLTrainingOperator]]s.
 *
 * @param inputDataQuanta0 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
 * @param inputDataQuanta1 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
 * @param model            model for the [[org.apache.wayang.basic.operators.DLTrainingOperator]]
 * @param option           option for the [[org.apache.wayang.basic.operators.DLTrainingOperator]]
 */
class DLTrainingDataQuantaBuilder[In0, In1](inputDataQuanta0: DataQuantaBuilder[_, In0],
                                        inputDataQuanta1: DataQuantaBuilder[_, In1],
                                        model: DLModel,
                                        option: DLTrainingOperator.Option)
                                          (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[DLTrainingDataQuantaBuilder[In0, In1], DLModel] {

  // Since we are currently not looking at type parameters, we can statically determine the output type.
  locally {
    this.outputTypeTrap.dataSetType = dataSetType[DLModel]
  }

  override protected def build =
    applyTargetPlatforms(inputDataQuanta0.dataQuanta()
      .dlTrainingJava(model, option, inputDataQuanta1.dataQuanta())(inputDataQuanta1.classTag), this.getTargetPlatforms())
}

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.PredictOperator]]s.
 *
 * @param inputDataQuanta0 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
 * @param inputDataQuanta1 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
 */
class PredictDataQuantaBuilder[In1, Out](inputDataQuanta0: DataQuantaBuilder[_, Model],
                                         inputDataQuanta1: DataQuantaBuilder[_, In1],
                                         outClass: Class[Out])
                                         (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[PredictDataQuantaBuilder[In1, Out], Out] {

  // Since we are currently not looking at type parameters, we can statically determine the output type.
  locally {
    this.outputTypeTrap.dataSetType = dataSetType[Out]
  }

  override protected def build =
    applyTargetPlatforms(
    inputDataQuanta0.dataQuanta().
      predictJava(inputDataQuanta1.dataQuanta())(inputDataQuanta1.classTag, ClassTag.apply(outClass)), this.getTargetPlatforms())
}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.CoGroupOperator]]s.
  *
  * @param inputDataQuanta0 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  * @param inputDataQuanta1 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  * @param keyUdf0          first key extraction UDF for the [[org.apache.wayang.basic.operators.CoGroupOperator]]
  * @param keyUdf1          first key extraction UDF for the [[org.apache.wayang.basic.operators.CoGroupOperator]]
  */
class CoGroupDataQuantaBuilder[In0, In1, Key](inputDataQuanta0: DataQuantaBuilder[_, In0],
                                           inputDataQuanta1: DataQuantaBuilder[_, In1],
                                           keyUdf0: SerializableFunction[In0, Key],
                                           keyUdf1: SerializableFunction[In1, Key])
                                          (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[CoGroupDataQuantaBuilder[In0, In1, Key], RT2[java.lang.Iterable[In0], java.lang.Iterable[In1]]] {

  /** [[ClassTag]] or surrogate of [[Key]] */
  implicit var keyTag: ClassTag[Key] = _

  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf0]]. */
  private var keyUdf0CpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf0]]. */
  private var keyUdf0RamEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the CPU load of the [[keyUdf1]]. */
  private var keyUdf1CpuEstimator: LoadEstimator = _

  /** [[LoadEstimator]] to estimate the RAM load of the [[keyUdf1]]. */
  private var keyUdf1RamEstimator: LoadEstimator = _

  // Try to infer the type classes from the UDFs.
  locally {
    val parameters = ReflectionUtils.getTypeParameters(keyUdf0.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In0] => inputDataQuanta0.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf0)
    }

    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf0)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }
  locally {
    val parameters = ReflectionUtils.getTypeParameters(keyUdf1.getClass, classOf[SerializableFunction[_, _]])
    parameters.get("Input") match {
      case cls: Class[In1] => inputDataQuanta1.outputTypeTrap.dataSetType = DataSetType.createDefault(cls)
      case _ => logger.warn("Could not infer types from {}.", keyUdf0)
    }

    this.keyTag = parameters.get("Output") match {
      case cls: Class[Key] => ClassTag(cls)
      case _ =>
        logger.warn("Could not infer types from {}.", keyUdf0)
        ClassTag(DataSetType.none.getDataUnitType.getTypeClass)
    }
  }
  // Since we are currently not looking at type parameters, we can statically determine the output type.
  locally {
    this.outputTypeTrap.dataSetType = dataSetType[RT2[_, _]]
  }

  /**
    * Set a [[LoadEstimator]] for the CPU load of the first key extraction UDF. Currently effectless.
    *
    * @param udfCpuEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThisKeyUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.keyUdf0CpuEstimator = udfCpuEstimator
    this
  }

  /**
    * Set a [[LoadEstimator]] for the RAM load of first the key extraction UDF. Currently effectless.
    *
    * @param udfRamEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThisKeyUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.keyUdf0RamEstimator = udfRamEstimator
    this
  }

  /**
    * Set a [[LoadEstimator]] for the CPU load of the second key extraction UDF. Currently effectless.
    *
    * @param udfCpuEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThatKeyUdfCpuEstimator(udfCpuEstimator: LoadEstimator) = {
    this.keyUdf1CpuEstimator = udfCpuEstimator
    this
  }

  /**
    * Set a [[LoadEstimator]] for the RAM load of the second key extraction UDF. Currently effectless.
    *
    * @param udfRamEstimator the [[LoadEstimator]]
    * @return this instance
    */
  def withThatKeyUdfRamEstimator(udfRamEstimator: LoadEstimator) = {
    this.keyUdf1RamEstimator = udfRamEstimator
    this
  }

  override protected def build =
    applyTargetPlatforms(inputDataQuanta0.dataQuanta().coGroupJava(keyUdf0, inputDataQuanta1.dataQuanta(), keyUdf1)(inputDataQuanta1.classTag, this.keyTag), this.getTargetPlatforms())

}


/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.CartesianOperator]]s.
  *
  * @param inputDataQuanta0 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  * @param inputDataQuanta1 [[DataQuantaBuilder]] for the first input [[DataQuanta]]
  */
class CartesianDataQuantaBuilder[In0, In1](inputDataQuanta0: DataQuantaBuilder[_, In0],
                                           inputDataQuanta1: DataQuantaBuilder[_, In1])
                                          (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[CartesianDataQuantaBuilder[In0, In1], RT2[In0, In1]] {

  // Since we are currently not looking at type parameters, we can statically determine the output type.
  locally {
    this.outputTypeTrap.dataSetType = dataSetType[RT2[_, _]]
  }

  override protected def build =
    applyTargetPlatforms(inputDataQuanta0.dataQuanta().cartesian(inputDataQuanta1.dataQuanta())(inputDataQuanta1.classTag), this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.ZipWithIdOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  */
class ZipWithIdDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T])
                                   (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[ZipWithIdDataQuantaBuilder[T], RT2[java.lang.Long, T]] {

  // Since we are currently not looking at type parameters, we can statically determine the output type.
  locally {
    this.outputTypeTrap.dataSetType = dataSetType[RT2[_, _]]
  }

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().zipWithId, this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.DistinctOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  */
class DistinctDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T])
                                  (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[DistinctDataQuantaBuilder[T], T] {

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().distinct, this.getTargetPlatforms())

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.CountOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  */
class CountDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T])
                               (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[CountDataQuantaBuilder[T], java.lang.Long] {

  // We can statically determine the output type.
  locally {
    this.outputTypeTrap.dataSetType = dataSetType[java.lang.Long]
  }

  override protected def build = applyTargetPlatforms(inputDataQuanta.dataQuanta().count, this.getTargetPlatforms())

}


/**
  * [[DataQuantaBuilder]] implementation for any [[org.apache.wayang.core.plan.wayangplan.Operator]]s. Does not offer
  * any convenience methods, though.
  *
  * @param operator        the custom [[org.apache.wayang.core.plan.wayangplan.Operator]]
  * @param outputIndex     index of the [[OutputSlot]] addressed by the new instance
  * @param buildCache      a [[DataQuantaBuilderCache]] that must be shared across instances addressing the same [[Operator]]
  * @param inputDataQuanta [[DataQuantaBuilder]]s for the input [[DataQuanta]]
  * @param javaPlanBuilder the [[JavaPlanBuilder]] used to construct the current [[WayangPlan]]
  */
class CustomOperatorDataQuantaBuilder[T](operator: Operator,
                                         outputIndex: Int,
                                         buildCache: DataQuantaBuilderCache,
                                         inputDataQuanta: DataQuantaBuilder[_, _]*)
                                        (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[DataQuantaBuilder[_, T], T] {

  override protected def build = {
    // If the [[operator]] has multiple [[OutputSlot]]s, make sure that we only execute the build once.
    if (!buildCache.hasCached) {
      val dataQuanta = javaPlanBuilder.planBuilder.customOperator(operator, inputDataQuanta.map(_.dataQuanta()): _*)
      buildCache.cache(dataQuanta)
    }

    applyTargetPlatforms(buildCache(outputIndex), this.getTargetPlatforms())
  }

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.DoWhileOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param conditionUdf    UDF for the looping condition
  * @param bodyBuilder     builds the loop body
  */
class DoWhileDataQuantaBuilder[T, ConvOut](inputDataQuanta: DataQuantaBuilder[_, T],
                                           conditionUdf: SerializablePredicate[JavaCollection[ConvOut]],
                                           bodyBuilder: JavaFunction[DataQuantaBuilder[_, T], WayangTuple[DataQuantaBuilder[_, T], DataQuantaBuilder[_, ConvOut]]])
                                          (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[DoWhileDataQuantaBuilder[T, ConvOut], T] {

  // TODO: Get the ClassTag right.
  implicit private var convOutClassTag: ClassTag[ConvOut] = ClassTag(DataSetType.none.getDataUnitType.getTypeClass)

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  // TODO: We could improve by combining the TypeTraps in the body loop.

  /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the UDF. */
  private var udfLoadProfileEstimator: LoadProfileEstimator = _

  /** Number of expected iterations. */
  private var numExpectedIterations = 20

  /**
    * Set a [[LoadProfileEstimator]] for the load of the UDF.
    *
    * @param udfLoadProfileEstimator the [[LoadProfileEstimator]]
    * @return this instance
    */
  def withUdfLoad(udfLoadProfileEstimator: LoadProfileEstimator) = {
    this.udfLoadProfileEstimator = udfLoadProfileEstimator
    this
  }

  /**
    * Explicitly set the [[DataSetType]] for the condition [[DataQuanta]]. Note that it is not
    * always necessary to set it and that it can be inferred in some situations.
    *
    * @param outputType the output [[DataSetType]]
    * @return this instance
    */
  def withConditionType(outputType: DataSetType[ConvOut]) = {
    this.convOutClassTag = ClassTag(outputType.getDataUnitType.getTypeClass)
    this
  }

  /**
    * Explicitly set the [[Class]] for the condition [[DataQuanta]]. Note that it is not
    * always necessary to set it and that it can be inferred in some situations.
    *
    * @param cls the output [[Class]]
    * @return this instance
    */
  def withConditionClass(cls: Class[ConvOut]) = {
    this.convOutClassTag = ClassTag(cls)
    this
  }

  /**
    * Set the number of expected iterations for the built [[org.apache.wayang.basic.operators.DoWhileOperator]].
    *
    * @param numExpectedIterations the expected number of iterations
    * @return this instance
    */
  def withExpectedNumberOfIterations(numExpectedIterations: Int) = {
    this.numExpectedIterations = numExpectedIterations
    this
  }

  override protected def build = applyTargetPlatforms(
    inputDataQuanta.dataQuanta().doWhileJava[ConvOut](
      conditionUdf, dataQuantaBodyBuilder, this.numExpectedIterations, this.udfLoadProfileEstimator
    )(this.convOutClassTag), this.getTargetPlatforms())


  /**
    * Create a loop body builder that is based on [[DataQuanta]].
    *
    * @return the loop body builder
    */
  private def dataQuantaBodyBuilder =
    new JavaFunction[DataQuanta[T], WayangTuple[DataQuanta[T], DataQuanta[ConvOut]]] {
      override def apply(loopStart: DataQuanta[T]) = {
        val loopStartBuilder = new FakeDataQuantaBuilder(loopStart)
        val loopEndBuilders = bodyBuilder(loopStartBuilder)
        new WayangTuple(loopEndBuilders.field0.dataQuanta(), loopEndBuilders.field1.dataQuanta())
      }
    }

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.DoWhileOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param numRepetitions  number of repetitions of the loop
  * @param bodyBuilder     builds the loop body
  */
class RepeatDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T],
                                 numRepetitions: Int,
                                 bodyBuilder: JavaFunction[DataQuantaBuilder[_, T], DataQuantaBuilder[_, T]])
                                (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[RepeatDataQuantaBuilder[T], T] {

  // Reuse the input TypeTrap to enforce type equality between input and output.
  override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

  // TODO: We could improve by combining the TypeTraps in the body loop.

  override protected def build =
    applyTargetPlatforms(
    inputDataQuanta.dataQuanta().repeat(numRepetitions, startDataQuanta => {
      val loopStartbuilder = new FakeDataQuantaBuilder(startDataQuanta)
      bodyBuilder(loopStartbuilder).dataQuanta()
    }), this.getTargetPlatforms())

}

/**
  * Wraps [[DataQuanta]] and exposes them as [[DataQuantaBuilder]], i.e., this is an adapter.
  *
  * @param _dataQuanta the wrapped [[DataQuanta]]
  */
class FakeDataQuantaBuilder[T](_dataQuanta: DataQuanta[T])(implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[FakeDataQuantaBuilder[T], T] {

  override implicit def classTag = ClassTag(_dataQuanta.output.getType.getDataUnitType.getTypeClass)

  override def dataQuanta() = _dataQuanta

  /**
    * Create the [[DataQuanta]] built by this instance. Note the configuration being done in [[dataQuanta()]].
    *
    * @return the created and partially configured [[DataQuanta]]
    */
  override protected def build: DataQuanta[T] = applyTargetPlatforms(_dataQuanta, this.getTargetPlatforms())
}

/**
 * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.LogisticRegressionOperator]]s.
 *
 * @param inputDataQuanta0 [[DataQuantaBuilder]]  (features)
 * @param inputDataQuanta1 [[DataQuantaBuilder]]  (labels)
 */
class LogisticRegressionDataQuantaBuilder(inputDataQuanta0: DataQuantaBuilder[_, Array[Double]],
                                          inputDataQuanta1: DataQuantaBuilder[_, java.lang.Double],
                                          fitIntercept: Boolean = true)
                                         (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[LogisticRegressionDataQuantaBuilder, LogisticRegressionModel] {

  locally {
    this.outputTypeTrap.dataSetType = dataSetType[LogisticRegressionModel]
  }

  override protected def build: DataQuanta[LogisticRegressionModel] =
    applyTargetPlatforms(
    inputDataQuanta0
      .dataQuanta()
      .trainLogisticRegression(inputDataQuanta1.dataQuanta(), fitIntercept), this.getTargetPlatforms())


}




/**
  * This is not an actual [[DataQuantaBuilder]] but rather decorates such a [[DataQuantaBuilder]] with a key.
  */
class KeyedDataQuantaBuilder[Out, Key](private val dataQuantaBuilder: DataQuantaBuilder[_, Out],
                                       private val keyExtractor: SerializableFunction[Out, Key])
                                      (implicit javaPlanBuilder: JavaPlanBuilder) {

  /**
    * Joins this instance with the given one via their keys.
    *
    * @param that the instance to join with
    * @return a [[DataQuantaBuilder]] representing the join product
    */
  def join[ThatOut](that: KeyedDataQuantaBuilder[ThatOut, Key]) =
    dataQuantaBuilder.join(this.keyExtractor, that.dataQuantaBuilder, that.keyExtractor)

  /**
    * Co-groups this instance with the given one via their keys.
    *
    * @param that the instance to join with
    * @return a [[DataQuantaBuilder]] representing the co-group product
    */
  def coGroup[ThatOut](that: KeyedDataQuantaBuilder[ThatOut, Key]) =
    dataQuantaBuilder.coGroup(this.keyExtractor, that.dataQuantaBuilder, that.keyExtractor)

}

/**
  * [[DataQuantaBuilder]] implementation for [[org.apache.wayang.basic.operators.SemanticFilterOperator]]s.
  *
  * @param inputDataQuanta [[DataQuantaBuilder]] for the input [[DataQuanta]]
  * @param udf             UDF for the [[SemanticFilterOperator]]
  */
class SemanticFilterDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T], prompt: String)
                                (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[SemanticFilterDataQuantaBuilder[T], T] {

    // Reuse the input TypeTrap to enforce type equality between input and output.
    override def getOutputTypeTrap: TypeTrap = inputDataQuanta.outputTypeTrap

    /** [[LoadProfileEstimator]] to estimate the [[LoadProfile]] of the [[udf]]. */
    private var udfLoadProfileEstimator: LoadProfileEstimator = _

    /** Selectivity of the filter predicate. */
    private var selectivity: ProbabilisticDoubleInterval = _

    /*

    */
    private val targetModels: ListBuffer[AnyRef] = ListBuffer()

    @varargs def withTargetModels(models: AnyRef *): SemanticFilterDataQuantaBuilder[T] = {
        models.foreach(targetModels.+=_)
        this
      }

    override protected def build = applyTargetPlatforms(
      inputDataQuanta.dataQuanta()
          .semanticFilterPrompt(prompt, targetModels: _ *), 
      this.getTargetPlatforms()
    )
  }

class SpatialFilterDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T],
                                        keySelector: SerializableFunction[T, _ <: SpatialGeometry],
                                        predicateType: SpatialPredicate,
                                        filterGeometry: SpatialGeometry)
                                       (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[SpatialFilterDataQuantaBuilder[T], T] {

  private var columnName: String = _

  def withSqlGeometryColumnName(columnName: String): SpatialFilterDataQuantaBuilder[T] = {
    this.columnName = columnName
    this
  }

  override protected def build: DataQuanta[T] = {
    val dq = inputDataQuanta.dataQuanta()
    dq.spatialFilterJava(keySelector, predicateType, filterGeometry, this.columnName)
  }
}

class SpatialJoinDataQuantaBuilder[In0, In1](inputDataQuanta0: DataQuantaBuilder[_, In0],
                                             inputDataQuanta1: DataQuantaBuilder[_, In1],
                                             keyUdf0: SerializableFunction[In0, _ <: SpatialGeometry],
                                             keyUdf1: SerializableFunction[In1, _ <: SpatialGeometry],
                                             predicateType: SpatialPredicate)
                                            (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[SpatialJoinDataQuantaBuilder[In0, In1], RT2[In0, In1]] {

  override protected def build: DataQuanta[RT2[In0, In1]] = {
    val dq0 = inputDataQuanta0.dataQuanta()
    val dq1 = inputDataQuanta1.dataQuanta()
    applyTargetPlatforms(
      dq0.spatialJoinJava(keyUdf0, dq1, keyUdf1, predicateType)(inputDataQuanta1.classTag),
      this.getTargetPlatforms()
    )
  }
}
