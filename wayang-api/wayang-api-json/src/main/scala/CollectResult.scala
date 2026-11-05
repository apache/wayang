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
package org.apache.wayang.api.json

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.databind.json.JsonMapper

import scala.collection.JavaConverters._

/** JSON values supported by the in-memory collection API; never serialize arbitrary objects. */
private[json] object CollectResult {
  private val mapper = JsonMapper.builder()
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    .build()

  def response(values: Iterable[Any], jsonOutput: Boolean): String = {
    val results = mapper.createArrayNode()
    values.foreach { value =>
      val decoded = if (jsonOutput) {
        require(value.isInstanceOf[String], "Expected JSON-encoded Python collect results")
        mapper.readValue(value.asInstanceOf[String], classOf[Object]) match {
          case error: java.util.Map[_, _] if error.size() == 1 && error.get("error").isInstanceOf[String] =>
            throw new IllegalArgumentException(s"Python collect failed: ${error.get("error")}")
          case decoded => decoded
        }
      } else value
      results.add(toJson(decoded))
    }
    val response = mapper.createObjectNode()
    response.set[JsonNode]("results", results)
    mapper.writeValueAsString(response)
  }

  private def toJson(value: Any): JsonNode = {
    val nodes = mapper.getNodeFactory
    value match {
      case text: String => nodes.textNode(text)
      case boolean: Boolean => nodes.booleanNode(boolean)
      case number: Byte => nodes.numberNode(number.toLong)
      case number: Short => nodes.numberNode(number.toLong)
      case number: Int => nodes.numberNode(number.toLong)
      case number: Long => nodes.numberNode(number)
      case number: Float => toJson(number.toDouble)
      case number: Double =>
        require(!number.isNaN && !number.isInfinity, "Collect results require finite floats")
        nodes.numberNode(number)
      case values: java.util.List[_] => toJson(values.asScala.toVector)
      case values: Array[_] => toJson(values.toVector)
      case pair: Tuple2[_, _] => toJson(Vector(pair._1, pair._2))
      case values: Seq[_] =>
        val array = mapper.createArrayNode()
        values.foreach(value => array.add(toJson(value)))
        array
      case _ =>
        val name = if (value == null) "null" else value.getClass.getName
        throw new IllegalArgumentException(s"Unsupported collect result type: $name")
    }
  }
}
