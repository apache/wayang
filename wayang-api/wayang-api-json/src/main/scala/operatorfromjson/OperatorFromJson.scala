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
package org.apache.wayang.api.json.operatorfromjson

import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson.OperatorNames
import org.apache.wayang.api.json.operatorfromjson.binary._
import org.apache.wayang.api.json.operatorfromjson.input._
import org.apache.wayang.api.json.operatorfromjson.loop._
import org.apache.wayang.api.json.operatorfromjson.other._
import org.apache.wayang.api.json.operatorfromjson.output._
import org.apache.wayang.api.json.operatorfromjson.unary._

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "operatorName", visible = true)
@JsonSubTypes(
  Array(
    // Input operators
    new JsonSubTypes.Type(value = classOf[TextFileInputFromJson], name = OperatorNames.TextFileInput),
    new JsonSubTypes.Type(value = classOf[ParquetInputFromJson], name = OperatorNames.ParquetInput),
    new JsonSubTypes.Type(value = classOf[InputCollectionFromJson], name = OperatorNames.InputCollection),
    new JsonSubTypes.Type(value = classOf[InputCollectionFromJson], name = OperatorNames.Table),
    new JsonSubTypes.Type(value = classOf[JDBCRemoteInputFromJson], name = OperatorNames.JDBCRemoteInput),
    new JsonSubTypes.Type(value = classOf[GeoJsonFileInputFromJson], name = OperatorNames.GeoJsonFileInput),

    // Output operators
    new JsonSubTypes.Type(value = classOf[TextFileOutputFromJson], name = OperatorNames.TextFileOutput),
    new JsonSubTypes.Type(value = classOf[GeoJsonFileOutputFromJson], name = OperatorNames.GeoJsonFileOutput),

    // Unary operators
    new JsonSubTypes.Type(value = classOf[MapOperatorFromJson], name = OperatorNames.Map),
    new JsonSubTypes.Type(value = classOf[FlatMapOperatorFromJson], name = OperatorNames.FlatMap),
    new JsonSubTypes.Type(value = classOf[FilterOperatorFromJson], name = OperatorNames.Filter),
    new JsonSubTypes.Type(value = classOf[SpatialFilterOperatorFromJson], name = OperatorNames.SpatialFilter),
    new JsonSubTypes.Type(value = classOf[ReduceByOperatorFromJson], name = OperatorNames.ReduceBy),
    new JsonSubTypes.Type(value = classOf[CountOperatorFromJson], name = OperatorNames.Count),
    new JsonSubTypes.Type(value = classOf[GroupByOperatorFromJson], name = OperatorNames.GroupBy),
    new JsonSubTypes.Type(value = classOf[SortOperatorFromJson], name = OperatorNames.Sort),
    new JsonSubTypes.Type(value = classOf[DistinctOperatorFromJson], name = OperatorNames.Distinct),
    new JsonSubTypes.Type(value = classOf[ReduceByOperatorFromJson], name = OperatorNames.Reduce),
    new JsonSubTypes.Type(value = classOf[SampleOperatorFromJson], name = OperatorNames.Sample),
    new JsonSubTypes.Type(value = classOf[MapPartitionsOperatorFromJson], name = OperatorNames.MapPartitions),

    // Loop operators
    new JsonSubTypes.Type(value = classOf[DoWhileOperatorFromJson], name = OperatorNames.DoWhile),
    new JsonSubTypes.Type(value = classOf[RepeatOperatorFromJson], name = OperatorNames.Repeat),
    new JsonSubTypes.Type(value = classOf[ForeachOperatorFromJson], name = OperatorNames.Foreach),

    // Binary operators
    new JsonSubTypes.Type(value = classOf[UnionOperatorFromJson], name = OperatorNames.Union),
    new JsonSubTypes.Type(value = classOf[JoinOperatorFromJson], name = OperatorNames.Join),
    new JsonSubTypes.Type(value = classOf[SpatialJoinOperatorFromJson], name = OperatorNames.SpatialJoin),
    new JsonSubTypes.Type(value = classOf[PredictOperatorFromJson], name = OperatorNames.Predict),
    new JsonSubTypes.Type(value = classOf[DLTrainingOperatorFromJson], name = OperatorNames.DLTraining),
    new JsonSubTypes.Type(value = classOf[CartesianOperatorFromJson], name = OperatorNames.Cartesian),
    new JsonSubTypes.Type(value = classOf[CoGroupOperatorFromJson], name = OperatorNames.CoGroup),
    new JsonSubTypes.Type(value = classOf[IntersectOperatorFromJson], name = OperatorNames.Intersect),

    // Composed
    new JsonSubTypes.Type(value = classOf[ComposedOperatorFromJson], name = OperatorNames.Composed),

    // Other operators
    new JsonSubTypes.Type(value = classOf[KMeansFromJson], name = OperatorNames.KMeans)

  )
)
class OperatorFromJson(
  val id: Long,
  val input: Array[Long],
  val output: Array[Long],
  val cat: String,
  val operatorName: String,
  val executionPlatform: String = null
) extends Serializable {
  //
  // Because case classes combined with inheritance were kind of difficult to change a field,
  // we use a workaround with json serialization -> modification -> deserialization.
  //
  private def getJsonMapper: JsonMapper = {
    JsonMapper.builder()
      .addModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .build()
  }

  def replaceId(value: Long): OperatorFromJson = {
    val mapper = getJsonMapper
    val jsonNode: JsonNode = mapper.valueToTree(this)
    val updatedJsonNode: ObjectNode = jsonNode.asInstanceOf[ObjectNode]
    updatedJsonNode.put("id", value)
    mapper.readValue(mapper.writeValueAsString(updatedJsonNode), new TypeReference[OperatorFromJson] {})
  }

  def replaceInputArray(array: Array[Long]): OperatorFromJson = {
    replaceArray("input", array)
  }

  def replaceOutputArray(array: Array[Long]): OperatorFromJson = {
    replaceArray("output", array)
  }

  private def replaceArray(field: String, array: Array[Long]): OperatorFromJson = {
    val mapper = getJsonMapper
    val jsonNode: JsonNode = mapper.valueToTree(this)
    val updatedJsonNode: ObjectNode = jsonNode.asInstanceOf[ObjectNode]
    val arrayNode: ArrayNode = mapper.valueToTree(array).asInstanceOf[ArrayNode]
    updatedJsonNode.set(field, arrayNode)
    mapper.readValue(mapper.writeValueAsString(updatedJsonNode), new TypeReference[OperatorFromJson] {})
  }
}

object OperatorFromJson {
  object Categories {
    final val Input = "input"
    final val Output = "output"
    final val Unary = "unary"
    final val Binary = "binary"
    final val Loop = "loop"
    final val Composed = "composed"
  }

  object OperatorNames {
    // Input
    final val TextFileInput = "textFileInput"
    final val ParquetInput = "parquetInput"
    final val InputCollection = "inputCollection"
    final val Table = "table"
    final val JDBCRemoteInput = "jdbcRemoteInput"
    final val GeoJsonFileInput = "geoJsonFileInput"

    // Output
    final val TextFileOutput = "textFileOutput"
    final val GeoJsonFileOutput = "geoJsonFileOutput"

    // Unary
    final val Map = "map"
    final val FlatMap = "flatMap"
    final val Filter = "filter"
    final val SpatialFilter = "spatialFilter"
    final val ReduceBy = "reduceBy"
    final val Count = "count"
    final val GroupBy = "groupBy"
    final val Sort = "sort"
    final val Distinct = "distinct"
    final val Reduce = "reduce"
    final val Sample = "sample"
    final val MapPartitions = "mapPartitions"

    // Loop
    final val DoWhile = "doWhile"
    final val Repeat = "repeat"
    final val Foreach = "foreach"

    // Binary
    final val Union = "union"
    final val Join = "join"
    final val SpatialJoin = "spatialJoin"
    final val Predict = "predict"
    final val DLTraining = "dlTraining"
    final val Cartesian = "cartesian"
    final val CoGroup = "coGroup"
    final val Intersect = "intersect"

    // Composed
    final val Composed = "composed"

    //  Other
    final val KMeans = "kmeans"
  }

  object ExecutionPlatforms {
    final val Java = "java"
    final val Spark = "spark"
    final val Flink = "flink"
    final val JDBC = "jdbc"
    final val Postgres = "postgres"
    final val SQLite3 = "sqlite3"
    final val Tensorflow = "tensorflow"
    final val All = List(Java, Spark, Flink, JDBC, Postgres, SQLite3, Tensorflow)
  }
}
