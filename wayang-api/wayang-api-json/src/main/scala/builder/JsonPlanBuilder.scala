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
package org.apache.wayang.api.json.builder

import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson.ExecutionPlatforms
import org.apache.wayang.api.json.parserutil.{SerializableLambda, SerializableLambda2}
import org.apache.wayang.api.json.operatorfromjson.{ComposedOperatorFromJson, OperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.binary.{CartesianOperatorFromJson, CoGroupOperatorFromJson, IntersectOperatorFromJson, JoinOperatorFromJson, PredictOperatorFromJson, DLTrainingOperatorFromJson, UnionOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.other.KMeansFromJson
import org.apache.wayang.api.json.operatorfromjson.input.{InputCollectionFromJson, JDBCRemoteInputFromJson, TableInputFromJson, TextFileInputFromJson, ParquetInputFromJson}
import org.apache.wayang.api.json.operatorfromjson.loop.{DoWhileOperatorFromJson, ForeachOperatorFromJson, RepeatOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.output.TextFileOutputFromJson
import org.apache.wayang.api.json.operatorfromjson.unary.{CountOperatorFromJson, DistinctOperatorFromJson, FilterOperatorFromJson, FlatMapOperatorFromJson, GroupByOperatorFromJson, MapOperatorFromJson, MapPartitionsOperatorFromJson, ReduceByOperatorFromJson, ReduceOperatorFromJson, SampleOperatorFromJson, SortOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.PlanFromJson
import org.apache.wayang.api._
import org.apache.wayang.basic.operators._
import org.apache.wayang.core.api.exception.WayangException
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.apache.wayang.core.platform.Platform
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.postgres.operators.PostgresTableSource
import org.apache.wayang.spark.Spark
import org.apache.wayang.flink.Flink
import org.apache.wayang.tensorflow.Tensorflow
import org.apache.wayang.genericjdbc.GenericJdbc
import org.apache.wayang.sqlite3.Sqlite3
import org.apache.wayang.core.plugin.Plugin
import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.data.Record;

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

class JsonPlanBuilder() {

  var planBuilder: PlanBuilder = _
  var configuration: Configuration = null
  var operators: Map[Long, OperatorFromJson] = Map()
  var plugins: List[Plugin] = List(
    Java.basicPlugin,
    Spark.basicPlugin
  )
  var origin: String = null

  def setPlatforms(platforms: List[String]): JsonPlanBuilder = {
    plugins = platforms.map(pl => getPlatformPlugin(pl))
    this
  }

  def setOrigin(contextOrigin: String): JsonPlanBuilder = {
    origin = contextOrigin
    this
  }

  def fromPlan(plan: PlanFromJson): JsonPlanBuilder = {
    setPlatforms(plan.context.platforms)
    setOrigin(plan.context.origin)
    setConfiguration(plan.context.configuration)
    setOperators(plan.operators)

    this
  }

  def setConfiguration(config: Map[String, String]): JsonPlanBuilder = {
    // Check if a wayang.properties file is declared using env variables, otherwise try default location.
    val wayangPropertiesFile: String = sys.props("wayang.configuration")

    if (Files.exists(Paths.get("wayang.properties"))) {
      println(s"Loading configuration from $wayangPropertiesFile.")
      try {
        this.configuration = new Configuration(wayangPropertiesFile)
      }
      catch {
        case _: WayangException =>
          println(s"Could not load configuration from $wayangPropertiesFile. Using default Wayang configuration file.")
          this.configuration = new Configuration()
      }
    }
    // If no wayang.properties file can be found, load default configuration.
    else {
      this.configuration = new Configuration()

      if (config.size == 0) {
        println("Using default Wayang configuration file.")
      } else {
        config.foreach(prop => this.configuration.setProperty(prop._1, prop._2))
      }
    }

    this
  }

  def setOperators(operators: List[OperatorFromJson]): JsonPlanBuilder = {
    setOperatorsRec(operators)

    // Create context with plugins
    val wayangContext = new WayangContext(this.configuration)
    plugins.foreach(plugin => wayangContext.withPlugin(plugin))

    // Check if there is a jdbc remote input. If yes, set configuration appropriately
    operators.foreach {
      case jdbcRemoteInputFromJson: JDBCRemoteInputFromJson =>
        configuration.setProperty("wayang.postgres.jdbc.url", jdbcRemoteInputFromJson.data.uri)
        configuration.setProperty("wayang.postgres.jdbc.user", jdbcRemoteInputFromJson.data.username)
        configuration.setProperty("wayang.postgres.jdbc.password", jdbcRemoteInputFromJson.data.password)
        wayangContext.withPlugin(Postgres.plugin())
      case _ =>
    }

    // Create plan builder and return
    planBuilder = new PlanBuilder(wayangContext)
      .withUdfJarsOf(classOf[JsonPlanBuilder])

    this
  }

  private def setOperatorsRec(operators: List[OperatorFromJson]): JsonPlanBuilder = {
    for (operator <- operators) {
      operator match {
        case composedOperatorFromJson: ComposedOperatorFromJson => setOperatorsRec(composedOperatorFromJson.operators.toList)
        case operator => this.operators = this.operators + (operator.id -> operator)
      }
    }
    this
  }

  def execute(): DataQuanta[Any] = {
    var outputOperators: List[OperatorFromJson] = operators.values.filter(operator => operator.cat == OperatorFromJson.Categories.Output).toList
    if (outputOperators.isEmpty)
      outputOperators = operators.values.filter(operator => operator.output.length == 0).toList
    val outputOperator = outputOperators.head
    executeRecursive(outputOperator, planBuilder)
  }

  private def executeRecursive(operator: OperatorFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    operator match {
      // input
      case inputOperator: TextFileInputFromJson => visit(inputOperator, planBuilder)
      case inputOperator: ParquetInputFromJson => visit(inputOperator, planBuilder)
      case inputOperator: InputCollectionFromJson => visit(inputOperator, planBuilder)
      case inputOperator: TableInputFromJson => visit(inputOperator, planBuilder)
      case inputOperator: JDBCRemoteInputFromJson => visit(inputOperator, planBuilder)

      // output
      case outputOperator: TextFileOutputFromJson => visit(outputOperator, executeRecursive(this.operators(operator.input(0)), planBuilder))

      // unary
      case operator: MapOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: FilterOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: FlatMapOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: ReduceByOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: CountOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: GroupByOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: SortOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: DistinctOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: ReduceOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: SampleOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: MapPartitionsOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))

      // binary
      case operator: UnionOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: JoinOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: CartesianOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: CoGroupOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: IntersectOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: PredictOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))
      case operator: DLTrainingOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder), executeRecursive(this.operators(operator.input(1)), planBuilder))

      // loop
      case operator: DoWhileOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: RepeatOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
      case operator: ForeachOperatorFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))

      // Other
      case operator: KMeansFromJson => this.visit(operator, executeRecursive(this.operators(operator.input(0)), planBuilder))
    }
  }

  //
  // Input operators
  //

  private def visit(operator: TextFileInputFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      planBuilder.readTextFile(operator.data.filename).asInstanceOf[DataQuanta[Any]]
    else
      planBuilder.readTextFile(operator.data.filename).asInstanceOf[DataQuanta[Any]].withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: ParquetInputFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      planBuilder.readParquet(operator.data.filename, operator.data.projection).asInstanceOf[DataQuanta[Any]]
    else
      planBuilder.readParquet(operator.data.filename, operator.data.projection).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform)).asInstanceOf[DataQuanta[Any]]
  }

  private def visit(operator: InputCollectionFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    val iterable = operator.data.collection
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      planBuilder.loadCollection(iterable)
    else
      planBuilder.loadCollection(iterable).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: TableInputFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      planBuilder.readTable(new TableSource(operator.data.tableName, operator.data.columnNames: _*)).asInstanceOf[DataQuanta[Any]]
    else
      planBuilder.readTable(new TableSource(operator.data.tableName, operator.data.columnNames: _*)).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform)).asInstanceOf[DataQuanta[Any]]
  }

  private def visit(operator: JDBCRemoteInputFromJson, planBuilder: PlanBuilder): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      planBuilder.readTable(new PostgresTableSource(operator.data.table, operator.data.columnNames: _*)).asInstanceOf[DataQuanta[Any]]
    else
      planBuilder.readTable(new PostgresTableSource(operator.data.table, operator.data.columnNames: _*)).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform)).asInstanceOf[DataQuanta[Any]]
  }

  //
  // Output operators
  //

  private def visit(operator: TextFileOutputFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    dataQuanta.writeTextFile(operator.data.filename, (x: Any) => x.toString)
    dataQuanta
  }

  //
  // Unary operators
  //

  private def visit(operator: MapOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    if (this.origin == "python") {
      val inputType: Class[_] = operator.data.inputType match {
        case Some(nDimArray) => nDimArray.toClassTag()
        case _ => classOf[Object]
      }
      val outputType: Class[_] = operator.data.outputType match {
        case Some(nDimArray) => nDimArray.toClassTag()
        case _ => classOf[Object]
      }
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.mapPartitionsPython(operator.data.udf, inputType, outputType)
      else
        dataQuanta.mapPartitionsPython(operator.data.udf, inputType, outputType).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    } else {
      val lambda = SerializableLambda.createLambda[Any, Any](operator.data.udf)
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.map(lambda)
      else
        dataQuanta.map(lambda).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    }
  }

  private def visit(operator: FilterOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    if (this.origin == "python") {
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.filterPython(operator.data.udf)
      else
        dataQuanta.filterPython(operator.data.udf).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    } else {
      val lambda = SerializableLambda.createLambda[Any, Boolean](operator.data.udf)
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.filter(lambda)
      else
        dataQuanta.filter(lambda).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    }
  }

  private def visit(operator: FlatMapOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    if (this.origin == "python") {
      val inputType: Class[_] = operator.data.inputType match {
        case Some(nDimArray) => nDimArray.toClassTag()
        case _ => classOf[Object]
      }
      val outputType: Class[_] = operator.data.outputType match {
        case Some(nDimArray) => nDimArray.toClassTag()
        case _ => classOf[Object]
      }
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.mapPartitionsPython(operator.data.udf, inputType, outputType)
      else
        dataQuanta.mapPartitionsPython(operator.data.udf, inputType, outputType).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    } else {
      val lambda = SerializableLambda.createLambda[Any, Iterable[Any]](operator.data.udf)
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.flatMap(lambda)
      else
        dataQuanta.flatMap(lambda).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    }
  }

  private def visit(operator: ReduceByOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    if (this.origin == "python") {
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.reduceByKeyPython(operator.data.keyUdf, operator.data.udf)
      else
        dataQuanta.reduceByKeyPython(operator.data.keyUdf, operator.data.udf).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    } else {
      val lambda1 = SerializableLambda.createLambda[Any, Any](operator.data.keyUdf)
      val lambda2 = SerializableLambda2.createLambda[Any, Any, Any](operator.data.udf)
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.reduceByKey(lambda1, lambda2)
      else
        dataQuanta.reduceByKey(lambda1, lambda2).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    }
  }

  private def visit(operator: CountOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta.count.asInstanceOf[DataQuanta[Any]]
    else
      dataQuanta.count.asInstanceOf[DataQuanta[Any]].withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: GroupByOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    val lambda = SerializableLambda.createLambda[Any, Any](operator.data.keyUdf)
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta.groupByKey(lambda).map(arrayList => arrayList.asScala.toList)
    else
      dataQuanta.groupByKey(lambda).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
        .map(arrayList => arrayList.asScala.toList).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform)).asInstanceOf[DataQuanta[Any]]
  }

  private def visit(operator: SortOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    val lambda = SerializableLambda.createLambda[Any, Any](operator.data.keyUdf)
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta.sort(lambda)
    else
      dataQuanta.sort(lambda).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: DistinctOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform)) {
      dataQuanta.distinct
    } else
      dataQuanta.distinct.withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: ReduceOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    if (this.origin == "python") {
      val inputType: Class[_] = operator.data.inputType match {
        case Some(nDimArray) => nDimArray.toClassTag()
        case _ => classOf[Object]
      }
      val outputType: Class[_] = operator.data.outputType match {
        case Some(nDimArray) => nDimArray.toClassTag()
        case _ => classOf[Object]
      }
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.mapPartitionsPython(operator.data.udf, inputType, outputType)
      else
        dataQuanta.mapPartitionsPython(operator.data.udf, inputType, outputType).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    } else {
      val lambda = SerializableLambda2.createLambda[Any, Any, Any](operator.data.udf)
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.reduce(lambda)
      else
        dataQuanta.reduce(lambda)
    }
  }

  private def visit(operator: SampleOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta.sample(operator.data.sampleSize)
    else
      dataQuanta.sample(operator.data.sampleSize).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: MapPartitionsOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    if (this.origin == "python") {
      val inputType: Class[_] = operator.data.inputType match {
        case Some(nDimArray) => nDimArray.toClassTag()
        case _ => classOf[Object]
      }
      val outputType: Class[_] = operator.data.outputType match {
        case Some(nDimArray) => nDimArray.toClassTag()
        case _ => classOf[Object]
      }
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.mapPartitionsPython(operator.data.udf, inputType, outputType)
      else
        dataQuanta.mapPartitionsPython(operator.data.udf, inputType, outputType).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    } else {
      val lambda = SerializableLambda.createLambda[Iterable[Any], Iterable[Any]](operator.data.udf)
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta.mapPartitions(lambda)
      else
        dataQuanta.mapPartitions(lambda).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
    }
  }

  //
  // Binary operators
  //

  private def visit(operator: UnionOperatorFromJson, dataQuanta1: DataQuanta[Any],  dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta1.union(dataQuanta2)
    else
      dataQuanta1.union(dataQuanta2).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: JoinOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    if (this.origin == "python") {
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta1.joinPython(operator.data.thisKeyUdf, dataQuanta2, operator.data.thatKeyUdf)
          .map(tuple2 => (tuple2.field0, tuple2.field1))
      else
        dataQuanta1.joinPython(operator.data.thisKeyUdf, dataQuanta2, operator.data.thatKeyUdf)
          .map(tuple2 => (tuple2.field0, tuple2.field1)).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform)).asInstanceOf[DataQuanta[Any]]
    } else {
      val lambda1 = SerializableLambda.createLambda[Any, Any](operator.data.thisKeyUdf)
      val lambda2 = SerializableLambda.createLambda[Any, Any](operator.data.thatKeyUdf)
      if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
        dataQuanta1.join(lambda1, dataQuanta2, lambda2)
          .map(tuple2 => (tuple2.field0, tuple2.field1))
      else
        dataQuanta1.join(lambda1, dataQuanta2, lambda2).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
          .map(tuple2 => (tuple2.field0, tuple2.field1)).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform)).asInstanceOf[DataQuanta[Any]]
    }
  }

  private def visit(operator: PredictOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    val inputType: Class[_] = operator.data.inputType match {
      case Some(nDimArray) => nDimArray.toClassTag()
      case _ => classOf[Object]
    }
    val outputType: Class[_] = operator.data.outputType match {
      case Some(nDimArray) => nDimArray.toClassTag()
      case _ => classOf[Object]
    }
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta1.predict(dataQuanta2, inputType, outputType)
    else
      dataQuanta1.predict(dataQuanta2, inputType, outputType).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: DLTrainingOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    val inputType: Class[_] = operator.data.inputType match {
      case Some(nDimArray) => nDimArray.toClassTag()
      case _ => classOf[Object]
    }
    val outputType: Class[_] = operator.data.outputType match {
      case Some(nDimArray) => nDimArray.toClassTag()
      case _ => classOf[Object]
    }
    val (model, option) = parseDLTrainingData(operator);
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta1.dlTraining(model, option, dataQuanta2, inputType, outputType)
    else
      dataQuanta1.dlTraining(model, option, dataQuanta2, inputType, outputType).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def parseDLTrainingData(operator: DLTrainingOperatorFromJson): (DLModel, DLTrainingOperator.Option) = {
    val modelOps = operator.data.model.op.toModelOp()
    val model = new DLModel(modelOps);

    val criterion = operator.data.option.criterion.toModelOp()
    val optimizer = operator.data.option.optimizer.toModelOptimizer()
    val batchSize = operator.data.option.batchSize;
    val epoch = operator.data.option.epoch;
    val option: DLTrainingOperator.Option = new DLTrainingOperator.Option(criterion, optimizer, batchSize.toInt, epoch.toInt);

    (model, option)
  }


  private def visit(operator: CartesianOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta1.cartesian(dataQuanta2)
        .map(tuple2 => (tuple2.field0, tuple2.field1))
    else
      dataQuanta1.cartesian(dataQuanta2).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
        .map(tuple2 => (tuple2.field0, tuple2.field1)).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform)).asInstanceOf[DataQuanta[Any]]
  }

  private def visit(operator: CoGroupOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    val lambda1 = SerializableLambda.createLambda[Any, Any](operator.data.thisKeyUdf)
    val lambda2 = SerializableLambda.createLambda[Any, Any](operator.data.thatKeyUdf)
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta1.coGroup(lambda1, dataQuanta2, lambda2)
        .map(tuple2 => (tuple2.field0.asScala.toList, tuple2.field1.asScala.toList))
    else
      dataQuanta1.coGroup(lambda1, dataQuanta2, lambda2).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
        .map(tuple2 => (tuple2.field0.asScala.toList, tuple2.field1.asScala.toList)).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform)).asInstanceOf[DataQuanta[Any]]
  }

  private def visit(operator: IntersectOperatorFromJson, dataQuanta1: DataQuanta[Any], dataQuanta2: DataQuanta[Any]): DataQuanta[Any] = {
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta1.intersect(dataQuanta2)
    else
      dataQuanta1.intersect(dataQuanta2).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  //
  // Loop operators
  //
  private def visit(operator: DoWhileOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    val lambda1 = SerializableLambda.createLambda[Any, Boolean](operator.data.udf)
    val lambda2 = SerializableLambda.createLambda[DataQuanta[Any], (DataQuanta[Any], DataQuanta[Any])]("import org.apache.wayang.api.DataQuanta\n" + operator.data.bodyBuilder)
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta.doWhile(lambda1, lambda2)
    else
      dataQuanta.doWhile(lambda1, lambda2).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: RepeatOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    val lambda = SerializableLambda.createLambda[DataQuanta[Any], DataQuanta[Any]]("import org.apache.wayang.api.DataQuanta\n" + operator.data.bodyBuilder)
    if (!ExecutionPlatforms.All.contains(operator.executionPlatform))
      dataQuanta.repeat(operator.data.n, lambda)
    else
      dataQuanta.repeat(operator.data.n, lambda).withTargetPlatforms(getExecutionPlatform(operator.executionPlatform))
  }

  private def visit(operator: ForeachOperatorFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {
    val lambda = SerializableLambda.createLambda[Any, Boolean](operator.data.udf)
    dataQuanta.foreach(lambda)
    dataQuanta
  }

  //
  // Other
  //

  private def visit(operator: KMeansFromJson, dataQuanta: DataQuanta[Any]): DataQuanta[Any] = {

    val distanceUdf = SerializableLambda2.createLambda[Any, Any, Any](operator.data.distanceUdf)
    val sumUdf = SerializableLambda2.createLambda[Any, Any, Any](operator.data.sumUdf)
    val divideUdf = SerializableLambda2.createLambda[Any, Any, Any](operator.data.divideUdf)
    val initialCentroidsUdf = SerializableLambda.createLambda[Any, Any](operator.data.initialCentroidsUdf)

    class SelectNearestCentroid extends ExtendedSerializableFunction[Any, Any] {

      var centroids: java.util.Collection[(Any, Int)] = _

      override def open(executionCtx: ExecutionContext): Unit = {
        centroids = executionCtx.getBroadcast[(Any, Int)]("centroids")
      }

      override def apply(point: Any): Any = {
        var minDistance = Double.PositiveInfinity
        var nearestCentroidId: Int = -1
        for (centroid <- centroids.asScala) {
          val distance = distanceUdf(point, centroid._1).asInstanceOf[Double]
          if (distance < minDistance) {
            minDistance = distance
            nearestCentroidId = centroid._2
          }
        }
        TaggedPointCounter(point, nearestCentroidId)
      }
    }

    case class TaggedPointCounter(point: Any, centroidId: Int, count: Int = 1) {
      def +(that: TaggedPointCounter): TaggedPointCounter =
        TaggedPointCounter(
          sumUdf(this.point, that.point),
          this.centroidId,
          this.count + that.count
        )

      def average: (Any, Int) = (divideUdf(point, count), centroidId)
    }

    val initialCentroids = planBuilder.loadCollection(initialCentroidsUdf(operator.data.k).asInstanceOf[Iterable[Any]])

    val finalCentroids = initialCentroids.map(_.asInstanceOf[(Any, Int)]).repeat(
      operator.data.maxIterations,
      { currentCentroids =>
        dataQuanta
          .mapJava(
            new SelectNearestCentroid
            // udfLoad = LoadProfileEstimators.createFromSpecification("wayang.apps.kmeans.udfs.select-centroid.load", configuration)
          )
          .withBroadcast(currentCentroids, "centroids")
          .reduceByKey(_.asInstanceOf[TaggedPointCounter].centroidId, _.asInstanceOf[TaggedPointCounter] + _.asInstanceOf[TaggedPointCounter])
          .withCardinalityEstimator(operator.data.k)
          .map(_.asInstanceOf[TaggedPointCounter].average)
      }
    )

    finalCentroids.asInstanceOf[DataQuanta[Any]]
  }


  /*
  * Execution platforms
  */
  private def getExecutionPlatform(executionPlatform: String): Platform = {
    executionPlatform match {
      case OperatorFromJson.ExecutionPlatforms.Java => Java.platform()
      case OperatorFromJson.ExecutionPlatforms.Spark => Spark.platform()
      case _ => null
    }
  }

  private def getPlatformPlugin(executionPlatform: String): Plugin = {
    executionPlatform match {
      case OperatorFromJson.ExecutionPlatforms.Java => Java.basicPlugin
      case OperatorFromJson.ExecutionPlatforms.Spark => Spark.basicPlugin
      case OperatorFromJson.ExecutionPlatforms.Flink => Flink.basicPlugin
      case OperatorFromJson.ExecutionPlatforms.JDBC => GenericJdbc.plugin
      case OperatorFromJson.ExecutionPlatforms.Postgres => Postgres.plugin
      case OperatorFromJson.ExecutionPlatforms.SQLite3 => Sqlite3.plugin
      case OperatorFromJson.ExecutionPlatforms.Tensorflow => Tensorflow.plugin
      case _ => null
    }
  }
}
