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

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.wayang.api.json.operatorfromjson.unary.MapPartitionsOperatorFromJson
import org.apache.wayang.api.json.parserutil.ParsePlanFromJson
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class CollectResultTest {
  private val mapper = new ObjectMapper()

  @Test
  def nativeValuesKeepTheirTypes(): Unit = {
    val values = Vector[Any](1.toByte, 2.toShort, 3, Long.MinValue, Long.MaxValue,
      1.25f, 2.5d, -0.0d, true, false, "123", "true", "null", "")
    val result = mapper.readTree(CollectResult.response(values, jsonOutput = false)).get("results")
    assertEquals(values.size, result.size())
    (0 to 4).foreach(index => assertTrue(result.get(index).isIntegralNumber))
    assertEquals(Long.MinValue, result.get(3).longValue())
    assertEquals(Long.MaxValue, result.get(4).longValue())
    assertEquals(1.25, result.get(5).doubleValue())
    assertEquals(2.5, result.get(6).doubleValue())
    assertEquals(java.lang.Double.doubleToRawLongBits(-0.0),
      java.lang.Double.doubleToRawLongBits(result.get(7).doubleValue()))
    assertTrue(result.get(8).isBoolean)
    assertFalse(result.get(9).booleanValue())
    (10 to 13).foreach(index => assertTrue(result.get(index).isTextual))
  }

  @Test
  def nestedContainersBecomeArrays(): Unit = {
    val values = Vector[Any](Array(1, 2), java.util.Arrays.asList("雪", false),
      (3L, Vector("nested", Vector.empty[Any])))
    assertEquals(mapper.readTree("""{"results":[[1,2],["雪",false],[3,["nested",[]]]]}"""),
      mapper.readTree(CollectResult.response(values, jsonOutput = false)))
  }

  @Test
  def emptyResults(): Unit = {
    Seq(false, true).foreach { encoded =>
      assertEquals("""{"results":[]}""", CollectResult.response(Vector.empty, encoded))
    }
  }

  @Test
  def stringsAreNotInterpretedWithoutEncodingMarker(): Unit = {
    val text = "quotes \" and \\ and newline\n雪 ${System.exit(1)}"
    val values = Vector[Any](text, "123", "true", "[1,2]", "{\"error\":\"data\"}")
    val result = mapper.readTree(CollectResult.response(values, jsonOutput = false)).get("results")
    values.zipWithIndex.foreach { case (value, index) =>
      assertTrue(result.get(index).isTextual)
      assertEquals(value, result.get(index).textValue())
    }
  }

  @Test
  def decodesPythonRecords(): Unit = {
    val values = Vector[Any]("1", "1.25", "true", "\"123\"", "\"\"", "[1,[false,\"雪\"]]")
    assertEquals(mapper.readTree("""{"results":[1,1.25,true,"123","",[1,[false,"雪"]]]}"""),
      mapper.readTree(CollectResult.response(values, jsonOutput = true)))
  }

  @Test
  def rejectsUnsupportedNativeValues(): Unit = {
    Seq[Any](null, new Object(), Map("key" -> 1), java.math.BigInteger.TEN,
      Double.NaN, Double.PositiveInfinity, Float.NegativeInfinity).foreach { value =>
      assertThrows(classOf[IllegalArgumentException], () => {
        CollectResult.response(Vector(value), jsonOutput = false); ()
      })
    }
  }

  @Test
  def rejectsMalformedAndUnsupportedRecords(): Unit = {
    Seq("", "not json", "1 2", "{").foreach { record =>
      assertThrows(classOf[JsonProcessingException], () => {
        CollectResult.response(Vector(record), jsonOutput = true); ()
      })
    }
    Seq[Any](1, "null", "{}", "[null]", "[{}]", "9223372036854775808", "1e400",
      "{\"error\":1}", "{\"error\":\"message\",\"extra\":true}").foreach { record =>
      assertThrows(classOf[IllegalArgumentException], () => {
        CollectResult.response(Vector(record), jsonOutput = true); ()
      })
    }
  }

  @Test
  def pythonFailureDiscardsEarlierResults(): Unit = {
    val failure = assertThrows(classOf[IllegalArgumentException], () => {
      CollectResult.response(Vector("1", "{\"error\":\"ZeroDivisionError: division by zero\"}"),
        jsonOutput = true); ()
    })
    assertTrue(failure.getMessage.contains("Python collect failed: ZeroDivisionError"))
  }

  @Test
  def legacyPlansDefaultToTextAndUnencodedRecords(): Unit = {
    val plan = ParsePlanFromJson.parsePlanFromString("""{
      "context": {"origin":"python", "platforms":["java"], "configuration":{}},
      "operators": [{"id":1, "operatorName":"mapPartitions", "cat":"unary",
        "input":[2], "output":[], "data":{"udf":"unused"}}]
    }""").get
    assertFalse(plan.collect)
    assertFalse(plan.operators.head.asInstanceOf[MapPartitionsOperatorFromJson].data.jsonOutput)
  }
}
