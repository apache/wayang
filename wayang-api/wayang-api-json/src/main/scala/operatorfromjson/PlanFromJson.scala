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

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class PlanFromJson(val context: ContextFromJson,
                   val operators: List[OperatorFromJson],
                   val collect: Boolean = false
                   ) extends Serializable {

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
