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
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson.OperatorNames
import org.apache.wayang.api.json.operatorfromjson.binary.{CartesianOperatorFromJson, CoGroupOperatorFromJson, IntersectOperatorFromJson, JoinOperatorFromJson, UnionOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.other.KMeansFromJson
import org.apache.wayang.api.json.operatorfromjson.input.{InputCollectionFromJson, JDBCRemoteInputFromJson, TextFileInputFromJson}
import org.apache.wayang.api.json.operatorfromjson.loop.{DoWhileOperatorFromJson, ForeachOperatorFromJson, RepeatOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.output.TextFileOutputFromJson
import org.apache.wayang.api.json.operatorfromjson.unary.{CountOperatorFromJson, DistinctOperatorFromJson, FilterOperatorFromJson, FlatMapOperatorFromJson, GroupByOperatorFromJson, MapOperatorFromJson, MapPartitionsOperatorFromJson, ReduceByOperatorFromJson, SampleOperatorFromJson, SortOperatorFromJson}

class ContextFromJson(val platforms: List[String],
                      val origin: String,
                      val configuration: Map[String, String],
                      val plugins: List[String] = List(),
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

  private def replaceArray(field: String, array: Array[String]): ContextFromJson = {
    val mapper = getJsonMapper
    val jsonNode: JsonNode = mapper.valueToTree(this)
    val updatedJsonNode: ObjectNode = jsonNode.asInstanceOf[ObjectNode]
    val arrayNode: ArrayNode = mapper.valueToTree(array).asInstanceOf[ArrayNode]
    updatedJsonNode.set(field, arrayNode)
    mapper.readValue(mapper.writeValueAsString(updatedJsonNode), new TypeReference[ContextFromJson] {})
  }
}

object ContextFromJson {
  object ExecutionPlatforms {
    final val Java = "java"
    final val Spark = "spark"
    final val Flink = "flink"
    final val JDBC = "jdbc"
    final val Postgres = "postgres"
    final val SQLite3 = "sqlite3"
    final val All = List(Java, Spark, Flink, JDBC, Postgres, SQLite3)
  }

  object Plugins {
    final val Spatial = "spatial"
  }
}
