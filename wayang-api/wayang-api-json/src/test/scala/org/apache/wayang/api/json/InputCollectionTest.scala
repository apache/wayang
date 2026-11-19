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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.wayang.api.json.builder.JsonPlanBuilder
import org.apache.wayang.api.json.operatorfromjson.input.InputCollectionFromJson
import org.apache.wayang.api.json.parserutil.{ParsePlanFromJson, SerializableIterable}
import org.apache.wayang.core.api.Configuration
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class InputCollectionTest {
  @TempDir
  var directory: Path = _

  private val mapper = new ObjectMapper()

  private def values(json: String): Iterable[Any] =
    InputCollectionFromJson.Data(values = mapper.readTree(json)).collection

  private def roundTrip[T](value: T): T = {
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(value) finally output.close()
    val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    try input.readObject().asInstanceOf[T] finally input.close()
  }

  @Test
  def literalTypesAndSerialization(): Unit = {
    val result = values("""[1,-9223372036854775808,9223372036854775807,1.25,true,"雪",[2,[],"nested"]]""").toVector
    assertEquals(Vector(1L, Long.MinValue, Long.MaxValue, 1.25, true, "雪"), result.take(6))
    assertTrue(result.head.isInstanceOf[Long])
    assertTrue(result(3).isInstanceOf[Double])
    assertEquals(java.util.Arrays.asList(2L, new java.util.ArrayList[Any](), "nested"), result(6))
    assertEquals(result, roundTrip(result))
  }

  @Test
  def emptyCollection(): Unit = {
    assertTrue(values("[]").isEmpty)
    assertTrue(roundTrip(values("[]")).isEmpty)
  }

  @Test
  def rejectsUnsupportedValues(): Unit = {
    Seq("null", "{}", "1", "[null]", "[{}]", "[9223372036854775808]", "[1e400]").foreach { json =>
      assertThrows(classOf[IllegalArgumentException], () => { values(json); () }, json)
    }
    assertThrows(classOf[IllegalArgumentException], () => InputCollectionFromJson.Data().collection)
    assertThrows(classOf[IllegalArgumentException], () =>
      InputCollectionFromJson.Data("List(1)", mapper.readTree("[]")).collection)
  }

  @Test
  def legacyIterableSerialization(): Unit = {
    val iterable = SerializableIterable.create("List(1, 2, 3)")
    assertEquals(List(1, 2, 3), roundTrip(iterable).get)
    assertEquals(List(1, 2, 3), iterable.get)
    assertEquals(List(1, 2, 3), roundTrip(iterable).get)
  }

  @Test
  def operatorJsonRoundTrip(): Unit = {
    Seq(
      InputCollectionFromJson.Data("List(1, 2, 3)"),
      InputCollectionFromJson.Data(values = mapper.readTree("[1,2,3]")),
      InputCollectionFromJson.Data(values = mapper.readTree("[]"))
    ).foreach { data =>
      val source = InputCollectionFromJson(1L, Array.empty[Long], Array(2L), "input", "inputCollection", data)
      val replaced = source.replaceId(3L).asInstanceOf[InputCollectionFromJson]
      assertEquals(3L, replaced.id)
      assertEquals(data.collection.toList, replaced.data.collection.toList)
    }
  }

  private def execute(data: String, filename: String, origin: String = "python"): String = {
    val output = directory.resolve(filename)
    val plan = ParsePlanFromJson.parsePlanFromString(s"""{
      "context": {"origin": "$origin", "platforms": ["java"], "configuration": {}},
      "operators": [
        {"id": 1, "operatorName": "inputCollection", "cat": "input",
         "input": [], "output": [2], "data": $data, "executionPlatform": "java"},
        {"id": 2, "operatorName": "textFileOutput", "cat": "output",
         "input": [1], "output": [], "data": {"filename": "${output.toUri}"}}
      ]
    }""").get
    val builder = new JsonPlanBuilder()
    builder.configuration = new Configuration()
    builder.setPlatforms(plan.context.platforms).setOrigin(plan.context.origin).setOperators(plan.operators).execute()
    new String(Files.readAllBytes(output), StandardCharsets.UTF_8)
  }

  @Test
  def literalPlanWritesFile(): Unit = {
    assertEquals("1\n2\n3\n", execute("""{"values": [1,2,3]}""", "integers.txt"))
    assertEquals("", execute("""{"values": []}""", "empty.txt"))
    assertEquals("[1, 2]\n[]\n", execute("""{"values": [[1,2],[]]}""", "nested.txt"))
  }

  @Test
  def stringsAreData(): Unit = {
    val text = "quotes \" and \\ and newline\n雪 ${System.exit(1)}"
    val data = mapper.createObjectNode()
    data.putArray("values").add(text).add("")
    assertEquals(text + "\n\n", execute(data.toString, "strings.txt"))
  }

  @Test
  def legacyPlansStillWriteFiles(): Unit = {
    assertEquals("1\n2\n3\n", execute("""{"udf": "List(1, 2, 3)"}""", "legacy.txt", "scala"))
    assertEquals("", execute("""{"udf": "List.empty[Int]"}""", "legacy-empty.txt", "scala"))
  }
}
