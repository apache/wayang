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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.wayang.api.json.builder.JsonPlanBuilder
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson
import org.apache.wayang.api.json.operatorfromjson.binary.{JoinOperatorFromJson, UnionOperatorFromJson}
import org.apache.wayang.api.json.operatorfromjson.input.TextFileInputFromJson
import org.apache.wayang.api.json.operatorfromjson.loop.RepeatOperatorFromJson
import org.apache.wayang.api.json.operatorfromjson.unary.{DistinctOperatorFromJson, SampleOperatorFromJson}
import org.apache.wayang.core.api.Configuration
import org.apache.wayang.core.plan.wayangplan.{Operator, PlanTraversal}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import scala.collection.JavaConverters._

class JsonPlanBuilderTest {

  @TempDir
  var directory: Path = _

  @Test
  def testRepeatedInputToUnion(): Unit = {
    val builder = createBuilder(
      source(1, Array(2)),
      UnionOperatorFromJson(2, Array(1, 1), Array.empty, "binary", "union")
    )
    val result = builder.execute()

    // Directly repeating an input must still produce an executable plan.
    assertEquals(3, operators(result.operator).size)
    assertEquals(List("alpha", "alpha", "alpha", "alpha", "beta", "beta"),
      result.collect().toList.map(_.toString).sorted)
  }

  @Test
  def testSharedAncestorsAreBuiltOnce(): Unit = {
    val depth = 8
    val nodes = List.newBuilder[OperatorFromJson]
    nodes += source(1, Array(2, 3))
    for (level <- 0 until depth) {
      val previous = 1L + 3 * level
      nodes += DistinctOperatorFromJson(previous + 1, Array(previous), Array(previous + 3), "unary", "distinct")
      nodes += DistinctOperatorFromJson(previous + 2, Array(previous), Array(previous + 3), "unary", "distinct")
      val outputs = if (level == depth - 1) Array.empty[Long] else Array(previous + 4, previous + 5)
      nodes += UnionOperatorFromJson(previous + 3, Array(previous + 1, previous + 2), outputs, "binary", "union")
    }
    val result = createBuilder(nodes.result(): _*).execute()
    val left = input(result.operator, 0)
    val right = input(result.operator, 1)

    assertNotSame(left, right)
    assertSame(input(left, 0), input(right, 0))
    assertEquals(1 + 3 * depth, operators(result.operator).size)
    assertEquals(List("alpha", "alpha", "beta", "beta"),
      result.collect().toList.map(_.toString).sorted)
  }

  @Test
  def testDifferentIdsRemainSeparate(): Unit = {
    val builder = createBuilder(
      source(1, Array(3)),
      source(2, Array(3)),
      UnionOperatorFromJson(3, Array(1, 2), Array.empty, "binary", "union")
    )
    val result = builder.execute()

    assertNotSame(input(result.operator, 0), input(result.operator, 1))
    assertEquals(3, operators(result.operator).size)
  }

  @Test
  def testRepeatedExecutionBuildsFreshOperators(): Unit = {
    val builder = createBuilder(
      source(1, Array(2, 3)),
      DistinctOperatorFromJson(2, Array(1), Array(4), "unary", "distinct"),
      DistinctOperatorFromJson(3, Array(1), Array(4), "unary", "distinct"),
      UnionOperatorFromJson(4, Array(2, 3), Array.empty, "binary", "union")
    )
    val first = builder.execute()
    val second = builder.execute()
    val firstOperators = operators(first.operator)
    val secondOperators = operators(second.operator)

    assertTrue(firstOperators.intersect(secondOperators).isEmpty)
    assertEquals(4, firstOperators.size)
    assertEquals(4, secondOperators.size)
  }

  @Test
  def testRepeatedInputToJoin(): Unit = {
    val builder = createBuilder(
      source(1, Array(2)),
      JoinOperatorFromJson(2, Array(1, 1), Array.empty, "binary", "join",
        JoinOperatorFromJson.Data("(x: Any) => x", "(x: Any) => x", None, None))
    )
    val result = builder.execute()
    assertEquals(List.fill(4)(("alpha", "alpha")) :+ (("beta", "beta")),
      result.collect().toList.map(_.asInstanceOf[(String, String)]).sorted)
  }

  @Test
  def testRepeatedLoopOutput(): Unit = {
    val builder = createBuilder(
      source(1, Array(2)),
      RepeatOperatorFromJson(2, Array(1), Array(3), "loop", "repeat",
        RepeatOperatorFromJson.Data(2, "(data: DataQuanta[Any]) => data.map(x => x.toString + \"!\").asInstanceOf[DataQuanta[Any]]")),
      UnionOperatorFromJson(3, Array(2, 2), Array.empty, "binary", "union")
    )
    val result = builder.execute()

    assertEquals(List("alpha!!", "alpha!!", "alpha!!", "alpha!!", "beta!!", "beta!!"),
      result.collect().toList.map(_.toString).sorted)
  }

  @Test
  def testSharedSampleIsOneOperator(): Unit = {
    val builder = createBuilder(
      source(1, Array(2)),
      SampleOperatorFromJson(2, Array(1), Array(3, 4), "unary", "sample", SampleOperatorFromJson.Data(1)),
      DistinctOperatorFromJson(3, Array(2), Array(5), "unary", "distinct"),
      DistinctOperatorFromJson(4, Array(2), Array(5), "unary", "distinct"),
      UnionOperatorFromJson(5, Array(3, 4), Array.empty, "binary", "union")
    )
    val result = builder.execute()

    assertSame(input(input(result.operator, 0), 0), input(input(result.operator, 1), 0))
    assertEquals(5, operators(result.operator).size)
  }

  private def source(id: Long, outputs: Array[Long]): TextFileInputFromJson = {
    val path = directory.resolve("input.txt")
    Files.write(path, "alpha\nbeta\nalpha\n".getBytes(StandardCharsets.UTF_8))
    TextFileInputFromJson(id, Array.empty, outputs, "input", "textFileInput",
      TextFileInputFromJson.Data(path.toUri.toString))
  }

  private def createBuilder(operators: OperatorFromJson*): JsonPlanBuilder = {
    val builder = new JsonPlanBuilder().setPlatforms(List("java"))
    builder.configuration = new Configuration()
    builder.configuration.setProperty("wayang.core.log.executions", directory.resolve("executions.json").toString)
    builder.configuration.setProperty("wayang.core.log.cardinalities", directory.resolve("cardinalities.json").toString)
    builder.setOperators(operators.toList)
  }

  private def input(operator: Operator, index: Int): Operator =
    operator.getInput(index).getOccupant.getOwner

  private def operators(root: Operator): Set[Operator] =
    PlanTraversal.upstream().traverse(root).getTraversedNodes.asScala.toSet
}
