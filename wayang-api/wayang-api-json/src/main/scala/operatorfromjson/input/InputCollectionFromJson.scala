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
package org.apache.wayang.api.json.operatorfromjson.input

import com.fasterxml.jackson.annotation.JsonTypeName
import com.fasterxml.jackson.databind.JsonNode
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson
import org.apache.wayang.api.json.parserutil.SerializableIterable

import scala.collection.JavaConverters._

@JsonTypeName(OperatorFromJson.OperatorNames.InputCollection)
case class InputCollectionFromJson(override val id: Long,
                                   override val input: Array[Long],
                                   override val output: Array[Long],
                                   override val cat: String,
                                   override val operatorName: String,
                                   val data: InputCollectionFromJson.Data,
                                   override val executionPlatform: String = null)
  extends OperatorFromJson(id, input, output, cat, operatorName, executionPlatform) {
}

object InputCollectionFromJson {
  case class Data(udf: String = null, values: JsonNode = null) {
    def collection: Iterable[Any] = {
      require((udf != null) != (values != null),
        "inputCollection requires exactly one of data.udf or data.values")
      if (values == null) return SerializableIterable.create(udf).get

      require(values.isArray, "inputCollection data.values must be an array")
      values.elements().asScala.map(readValue).toVector
    }
  }

  // Use serializable JVM values, not Jackson nodes or generated Scala code.
  private def readValue(value: JsonNode): Any = {
    if (value.isTextual) value.textValue()
    else if (value.isBoolean) value.booleanValue()
    else if (value.isIntegralNumber) {
      require(value.canConvertToLong, "inputCollection integers must fit in signed 64 bits")
      value.longValue()
    } else if (value.isFloatingPointNumber) {
      val number = value.doubleValue()
      require(!number.isNaN && !number.isInfinity, "inputCollection floats must be finite")
      number
    } else if (value.isArray) {
      val result = new java.util.ArrayList[Any]()
      value.elements().asScala.foreach(item => result.add(readValue(item)))
      result
    } else {
      throw new IllegalArgumentException(s"Unsupported inputCollection value: $value")
    }
  }
}

