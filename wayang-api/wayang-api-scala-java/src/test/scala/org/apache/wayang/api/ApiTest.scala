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

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import java.sql.{Connection, Statement}
import java.util.function.Consumer

import org.apache.wayang.basic.WayangBasics
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializablePredicate
import org.apache.wayang.core.function.{ExecutionContext, TransformationDescriptor}
import org.apache.wayang.core.util.fs.LocalFileSystem
import org.apache.wayang.java.Java
import org.apache.wayang.java.operators.JavaMapOperator
import org.apache.wayang.spark.Spark
import org.apache.wayang.sqlite3.Sqlite3
import org.apache.wayang.sqlite3.operators.Sqlite3TableSource
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/**
  * Tests the Wayang API.
  */
class ApiTest {

  @Test
  def testReadMapCollect(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Wayang plan.
    val outputValues = wayang
      .loadCollection(inputValues).withName("Load input values")
      .map(_ + 2).withName("Add 2")
      .collect()

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2)
    assertArrayEquals(expectedOutputValues, outputValues.toArray)
  }

  @Test
  def testCustomOperator(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Wayang plan.
    val inputDataSet = wayang.loadCollection(inputValues).withName("Load input values")

    // Add the custom operator.
    val IndexedSeq(addedValues) = wayang.customOperator(new JavaMapOperator(
      dataSetType[Int],
      dataSetType[Int],
      new TransformationDescriptor(
        toSerializableFunction[Int, Int](_ + 2),
        basicDataUnitType[Int], basicDataUnitType[Int]
      )
    ), inputDataSet)
    addedValues.withName("Add 2")

    // Collect the result.
    val outputValues = addedValues.asInstanceOf[DataQuanta[Int]].collect()

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2)
    assertArrayEquals(expectedOutputValues, outputValues.toArray)
  }

  @Test
  def testCustomOperatorShortCut(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Wayang plan.
    val outputValues = wayang
      .loadCollection(inputValues).withName("Load input values")
      .customOperator[Int](new JavaMapOperator(
      dataSetType[Int],
      dataSetType[Int],
      new TransformationDescriptor(
        toSerializableFunction[Int, Int](_ + 2),
        basicDataUnitType[Int], basicDataUnitType[Int]
      )
    )).withName("Add 2")
      .collect()

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2)
    assertArrayEquals(expectedOutputValues, outputValues.toArray)
  }

  @Test
  def testWordCount(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    // Generate some test data.
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count WayangPlan.
    val wordCounts = wayang
      .loadCollection(inputValues).withName("Load input values")
      .flatMap(_.split("\\s+")).withName("Split words")
      .map(_.replaceAll("\\W+", "").toLowerCase).withName("To lowercase")
      .map((_, 1)).withName("Attach counter")
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)).withName("Sum counters")
      .collect().toSet

    val expectedWordCounts = Set(("big", 3), ("is", 2), ("data", 3))

    assertEquals(expectedWordCounts, wordCounts)
  }

  @Test
  def testWordCountOnSparkAndJava(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    // Generate some test data.
    val inputValues = Array("Big data is big.", "Is data big data?")

    // Build and execute a word count WayangPlan.
    val wordCounts = wayang
      .loadCollection(inputValues).withName("Load input values").withTargetPlatforms(Java.platform)
      .flatMap(_.split("\\s+")).withName("Split words").withTargetPlatforms(Java.platform)
      .map(_.replaceAll("\\W+", "").toLowerCase).withName("To lowercase").withTargetPlatforms(Spark.platform)
      .map((_, 1)).withName("Attach counter").withTargetPlatforms(Spark.platform)
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)).withName("Sum counters").withTargetPlatforms(Spark.platform)
      .collect().toSet

    val expectedWordCounts = Set(("big", 3), ("is", 2), ("data", 3))

    assertEquals(expectedWordCounts, wordCounts)
  }

  @Test
  def testSample(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    // Generate some test data.
    val inputValues = for (i <- 0 until 100) yield i

    // Build and execute the WayangPlan.
    val sample = wayang
      .loadCollection(inputValues)
      .sample(10)
      .collect()

    // Check the result.
    assertEquals(10, sample.size)
    assertEquals(10, sample.toSet.size)
  }

  @Test
  def testDoWhile(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    // Generate some test data.
    val inputValues = Array(1, 2)

    // Build and execute a word count WayangPlan.

    val values = wayang
      .loadCollection(inputValues).withName("Load input values")
      .doWhile[Int](vals => vals.max > 100, {
      start =>
        val sum = start.reduce(_ + _).withName("Sum")
        (start.union(sum).withName("Old+new"), sum)
    }).withName("While <= 100")
      .collect().toSet

    val expectedValues = Set(1, 2, 3, 6, 12, 24, 48, 96, 192)
    assertEquals(expectedValues, values)
  }

  @Test
  def testRepeat(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    // Generate some test data.
    val inputValues = Array(1, 2)

    // Build and execute a word count WayangPlan.

    val values = wayang
      .loadCollection(inputValues).withName("Load input values").withName(inputValues.mkString(","))
      .repeat(3,
        _.reduce(_ * _).withName("Multiply")
          .flatMap(v => Seq(v, v + 1)).withName("Duplicate")
      ).withName("Repeat 3x")
      .collect().toSet

    // initial: 1,2 -> 1st: 2,3 -> 2nd: 6,7 => 3rd: 42,43
    val expectedValues = Set(42, 43)
    assertEquals(expectedValues, values)
  }

  @Test
  def testBroadcast() = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)
    val builder = new PlanBuilder(wayang)

    // Generate some test data.
    val inputStrings = Array("Hello", "World", "Hi", "Mars")
    val selectors = Array('o', 'l')

    val selectorsDataSet = builder.loadCollection(selectors).withName("Load selectors")

    // Build and execute a word count WayangPlan.
    val values = builder
      .loadCollection(inputStrings).withName("Load input values")
      .filterJava(new ExtendedSerializablePredicate[String] {

        var selectors: Iterable[Char] = _

        override def open(ctx: ExecutionContext): Unit = {
          import scala.collection.JavaConversions._
          selectors = collectionAsScalaIterable(ctx.getBroadcast[Char]("selectors"))
        }

        override def test(t: String): Boolean = selectors.forall(selector => t.contains(selector))

      }).withName("Filter words")
      .withBroadcast(selectorsDataSet, "selectors")
      .collect().toSet

    val expectedValues = Set("Hello", "World")
    assertEquals(expectedValues, values)
  }

  @Test
  def testGroupBy() = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    val inputValues = Array(1, 2, 3, 4, 5, 7, 8, 9, 10)

    val result = wayang
      .loadCollection(inputValues)
      .groupByKey(_ % 2).withName("group odd and even")
      .map {
        group =>
          import scala.collection.JavaConversions._
          val buffer = group.toBuffer
          buffer.sortBy(identity)
          if (buffer.size % 2 == 0) (buffer(buffer.size / 2 - 1) + buffer(buffer.size / 2)) / 2
          else buffer(buffer.size / 2)
      }.withName("median")
      .collect()

    val expectedValues = Set(5, 6)
    assertEquals(expectedValues, result.toSet)
  }

  @Test
  def testGroup() = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    val inputValues = Array(1, 2, 3, 4, 5, 7, 8, 9, 10)

    val result = wayang
      .loadCollection(inputValues)
      .group()
      .map {
        group =>
          import scala.collection.JavaConversions._
          val buffer = group.toBuffer
          buffer.sortBy(int => int)
          if (buffer.size % 2 == 0) (buffer(buffer.size / 2) + buffer(buffer.size / 2 + 1)) / 2
          else buffer(buffer.size / 2)
      }
      .collect()

    val expectedValues = Set(5)
    assertEquals(expectedValues, result.toSet)
  }

  @Test
  def testJoin() = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    val inputValues1 = Array(("Water", 0), ("Tonic", 5), ("Juice", 10))
    val inputValues2 = Array(("Apple juice", "Juice"), ("Tap water", "Water"), ("Orange juice", "Juice"))

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val result = dataQuanta1
      .join[(String, String), String](_._1, dataQuanta2, _._2)
      .map(joinTuple => (joinTuple.field1._1, joinTuple.field0._2))
      .collect()

    val expectedValues = Set(("Apple juice", 10), ("Tap water", 0), ("Orange juice", 10))
    assertEquals(expectedValues, result.toSet)
  }

  @Test
  def testJoinAndAssemble() = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    val inputValues1 = Array(("Water", 0), ("Tonic", 5), ("Juice", 10))
    val inputValues2 = Array(("Apple juice", "Juice"), ("Tap water", "Water"), ("Orange juice", "Juice"))

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val result = dataQuanta1.keyBy(_._1).join(dataQuanta2.keyBy(_._2))
      .assemble((dq1, dq2) => (dq2._1, dq1._2))
      .collect()

    val expectedValues = Set(("Apple juice", 10), ("Tap water", 0), ("Orange juice", 10))
    assertEquals(expectedValues, result.toSet)
  }


  @Test
  def testCoGroup() = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    val inputValues1 = Array(("Water", 0), ("Cola", 5), ("Juice", 10))
    val inputValues2 = Array(("Apple juice", "Juice"), ("Tap water", "Water"), ("Orange juice", "Juice"))

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val result = dataQuanta1
      .coGroup[(String, String), String](_._1, dataQuanta2, _._2)
      .collect()

    import scala.collection.JavaConversions._
    val actualValues = result.map(coGroup => (coGroup.field0.toSet, coGroup.field1.toSet)).toSet
    val expectedValues = Set(
      (Set(("Water", 0)), Set(("Tap water", "Water"))),
      (Set(("Cola", 5)), Set()),
      (Set(("Juice", 10)), Set(("Apple juice", "Juice"), ("Orange juice", "Juice")))
    )
    assertEquals(expectedValues, actualValues)
  }

  @Test
  def testIntersect() = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    val inputValues1 = Array(1, 2, 3, 4, 5, 7, 8, 9, 10)
    val inputValues2 = Array(0, 2, 3, 3, 4, 5, 7, 8, 9, 11)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val result = dataQuanta1
      .intersect(dataQuanta2)
      .collect()

    val expectedValues = Set(2, 3, 4, 5, 7, 8, 9)
    assertEquals(expectedValues, result.toSet)
  }


  @Test
  def testSort() = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    val inputValues1 = Array(3, 4, 5, 2, 1)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val result = dataQuanta1
      .sort(r=>r)
      .collect()

    val expectedValues = Array(1, 2, 3, 4, 5)
    assertArrayEquals(expectedValues, result.toArray)
  }


  @Test
  def testPageRank() = {
    // Set up WayangContext.
    val wayang = new WayangContext()
      .withPlugin(Java.graphPlugin)
      .withPlugin(WayangBasics.graphPlugin)
      .withPlugin(Java.basicPlugin)
    import org.apache.wayang.api.graph._

    val edges = Seq((0, 1), (0, 2), (0, 3), (1, 0), (2, 1), (3, 2), (3, 1)).map(t => Edge(t._1, t._2))

    val pageRanks = wayang
      .loadCollection(edges).withName("Load edges")
      .pageRank(20).withName("PageRank")
      .collect()
      .map(t => t.field0.longValue -> t.field1)
      .toMap

    print(pageRanks)
    // Let's not check absolute numbers but only the relative ordering.
    assertTrue(pageRanks(1) > pageRanks(0))
    assertTrue(pageRanks(0) > pageRanks(2))
    assertTrue(pageRanks(2) > pageRanks(3))
  }

  @Test
  def testMapPartitions() = {
    // Set up WayangContext.
    val wayang = new WayangContext()
      .withPlugin(Java.basicPlugin())
      .withPlugin(Spark.basicPlugin)

    val typeCounts = wayang
      .loadCollection(Seq(0, 1, 2, 3, 4, 6, 8))
        .mapPartitions { ints =>
          var (numOdds, numEvens) = (0, 0)
          ints.foreach(i => if ((i & 1) == 0) numEvens += 1 else numOdds += 1)
          Seq(("odd", numOdds), ("even", numEvens))
        }
      .reduceByKey(_._1, { case ((kind1, count1), (kind2, count2)) => (kind1, count1 + count2) })
      .collect()

    assertEquals(Set(("odd", 2), ("even", 5)), typeCounts.toSet)
  }

  @Test
  def testZipWithId() = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin).withPlugin(Spark.basicPlugin)

    val inputValues = for (i <- 0 until 100; j <- 0 until 42) yield i

    val result = wayang
      .loadCollection(inputValues)
      .zipWithId
      .groupByKey(_.field1)
      .map { group =>
        import scala.collection.JavaConversions._
        (group.map(_.field0).toSet.size, 1)
      }
      .reduceByKey(_._1, (t1, t2) => (t1._1, t1._2 + t2._2))
      .collect()

    val expectedValues = Set((42, 100))
    assertEquals(expectedValues, result.toSet)
  }

  @Test
  def testWriteTextFile() = {
    val tempDir = LocalFileSystem.findTempDir
    val targetUrl = LocalFileSystem.toURL(new File(tempDir, "testWriteTextFile.txt"))

    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = for (i <- 0 to 5) yield i * 0.333333333333

    val result = wayang
      .loadCollection(inputValues)
      .writeTextFile(targetUrl, formatterUdf = d => f"${d % .2f}")

    val lines = scala.collection.mutable.Set[String]()
    Files.lines(Paths.get(new URI(targetUrl))).forEach(new Consumer[String] {
      override def accept(line: String): Unit = lines += line
    })

    val expectedLines = inputValues.map(v => f"${v % .2f}").toSet
    assertEquals(expectedLines, lines)
  }

  @Test
  def testSqlOnJava() = {
    // Initialize some test data.
    val configuration = new Configuration
    val sqlite3dbFile = File.createTempFile("wayang-sqlite3", "db")
    sqlite3dbFile.deleteOnExit()
    configuration.setProperty("wayang.sqlite3.jdbc.url", "jdbc:sqlite:" + sqlite3dbFile.getAbsolutePath)

    val connection: Connection = Sqlite3.platform.createDatabaseDescriptor(configuration).createJdbcConnection
    try {
      val statement: Statement = connection.createStatement
      statement.addBatch("DROP TABLE IF EXISTS customer;")
      statement.addBatch("CREATE TABLE customer (name TEXT, age INT);")
      statement.addBatch("INSERT INTO customer VALUES ('John', 20)")
      statement.addBatch("INSERT INTO customer VALUES ('Timmy', 16)")
      statement.addBatch("INSERT INTO customer VALUES ('Evelyn', 35)")
      statement.executeBatch()
    } finally {
      if (connection != null) connection.close()
    }

    // Set up WayangContext.
    val wayang = new WayangContext(configuration).withPlugin(Java.basicPlugin).withPlugin(Sqlite3.plugin)

    val result = wayang
      .readTable(new Sqlite3TableSource("customer", "name", "age"))
      .filter(r => r.getField(1).asInstanceOf[Integer] >= 18, sqlUdf = "age >= 18").withTargetPlatforms(Java.platform)
      .projectRecords(Seq("name"))
      .map(_.getField(0).asInstanceOf[String])
      .collect()
      .toSet

    val expectedValues = Set("John", "Evelyn")
    assertEquals(expectedValues, result)
  }

  @Test
  def testSqlOnSqlite3() = {
    // Initialize some test data.
    val configuration = new Configuration
    val sqlite3dbFile = File.createTempFile("wayang-sqlite3", "db")
    sqlite3dbFile.deleteOnExit()
    configuration.setProperty("wayang.sqlite3.jdbc.url", "jdbc:sqlite:" + sqlite3dbFile.getAbsolutePath)

    val connection: Connection = Sqlite3.platform.createDatabaseDescriptor(configuration).createJdbcConnection
    try {
      val statement: Statement = connection.createStatement
      statement.addBatch("DROP TABLE IF EXISTS customer;")
      statement.addBatch("CREATE TABLE customer (name TEXT, age INT);")
      statement.addBatch("INSERT INTO customer VALUES ('John', 20)")
      statement.addBatch("INSERT INTO customer VALUES ('Timmy', 16)")
      statement.addBatch("INSERT INTO customer VALUES ('Evelyn', 35)")
      statement.executeBatch
    } finally {
      if (connection != null) connection.close()
    }

    // Set up WayangContext.
    val wayang = new WayangContext(configuration).withPlugin(Java.basicPlugin).withPlugin(Sqlite3.plugin)

    val result = wayang
      .readTable(new Sqlite3TableSource("customer", "name", "age"))
      .filter(r => r.getField(1).asInstanceOf[Integer] >= 18, sqlUdf = "age >= 18")
      .projectRecords(Seq("name")).withTargetPlatforms(Sqlite3.platform)
      .map(_.getField(0).asInstanceOf[String])
      .collect()
      .toSet

    val expectedValues = Set("John", "Evelyn")
    assertEquals(expectedValues, result)
  }
}
