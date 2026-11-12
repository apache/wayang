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


package org.apache.wayang.api.serialization

import org.apache.wayang.api.{MultiContext, PlanBuilder, createPlanBuilder}
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.java.Java
import org.apache.wayang.postgres.Postgres
import org.apache.wayang.postgres.operators.PostgresTableSource
import org.apache.wayang.sqlite3.Sqlite3
import org.apache.wayang.sqlite3.operators.Sqlite3TableSource
import org.junit.jupiter.api.Test

import java.io.{File, PrintWriter}
import java.sql.{Connection, Statement}
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`


class OperatorSerializationTests extends SerializationTestBase {

  @Test
  def testReadMapCollect(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    // Generate some test data.
    val inputValues = (for (i <- 1 to 10) yield i).toArray

    // Build and execute a Wayang plan.
    val dq = wayang
      .loadCollection(inputValues).withName("Load input values")
      .map(_ + 2).withName("Add 2")

    dq.operator.getTargetPlatforms

    // Check the outcome.
    val expectedOutputValues = inputValues.map(_ + 2).map(_.toString).toList
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedOutputValues)
  }

  @Test
  def testFilterDistinctCount(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    // Generate some test data.
    val inputValues = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8)

    // Build and execute a Wayang plan.
    val dq = wayang
      .loadCollection(inputValues)
      .filter(n => n >= 4)
      .distinct
      .count

    // Check the outcome.
    val expected = List("5")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expected)
  }

  @Test
  def testReduce(): Unit = {
    // Set up WayangContext.
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    // Generate some test data.
    val inputValues = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    // Build and execute a Wayang plan.
    val dq = wayang
      .loadCollection(inputValues)
      .reduce((a, b) => a + b)

    // Check the outcome.
    val expected = List("55")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expected)
  }

  @Test
  def testWordCount(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array("Big data is big.", "Is data big data?")

    val dq = wayang
      .loadCollection(inputValues).withName("Load input values")
      .flatMap(_.split("\\s+")).withName("Split words")
      .map(_.replaceAll("\\W+", "").toLowerCase).withName("To lowercase")
      .map((_, 1)).withName("Attach counter")
      .reduceByKey(_._1, (a, b) => (a._1, a._2 + b._2)).withName("Sum counters")

    val expectedWordCounts = List(("big", 3), ("data", 3), ("is", 2)).map(_.toString())
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedWordCounts)
  }

  @Test
  def testGroupBy(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2, 3, 4, 5, 7, 8, 9, 10)

    val dq = wayang
      .loadCollection(inputValues)
      .groupByKey(_ % 2).withName("group odd and even")
      .map {
        group =>

          val buffer = group.toBuffer
          buffer.sortBy(identity)
          if (buffer.size % 2 == 0) (buffer(buffer.size / 2 - 1) + buffer(buffer.size / 2)) / 2
          else buffer(buffer.size / 2)
      }.withName("median")

    val expectedOutputValues = List("6", "5")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedOutputValues)
  }

  @Test
  def testSort(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(3, 4, 5, 2, 1)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dq = dataQuanta1
      .sort(r => r)

    val expectedValues = List(1, 2, 3, 4, 5).map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testMapPartitions(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin())

    val dq = wayang
      .loadCollection(Seq(0, 1, 2, 3, 4, 6, 8))
      .mapPartitions { ints =>
        var (numOdds, numEvens) = (0, 0)
        ints.foreach(i => if ((i & 1) == 0) numEvens += 1 else numOdds += 1)
        Seq(("odd", numOdds), ("even", numEvens))
      }
      .reduceByKey(_._1, { case ((kind1, count1), (kind2, count2)) => (kind1, count1 + count2) })

    val expectedValues = List(("even", 5), ("odd", 2)).map(_.toString())
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testZipWithId(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = for (i <- 0 until 100; j <- 0 until 42) yield i

    val dq = wayang
      .loadCollection(inputValues)
      .zipWithId
      .groupByKey(_.field1)
      .map { group =>

        (group.map(_.field0).toSet.size, 1)
      }
      .reduceByKey(_._1, (t1, t2) => (t1._1, t1._2 + t2._2))

    val expectedValues = List((42, 100)).map(_.toString())
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  // Function to create a temp file and write content to it
  def createTempFile(content: String, prefix: String): File = {
    val tempFile = File.createTempFile(prefix, ".txt")
    val writer = new PrintWriter(tempFile)
    try {
      writer.write(content.trim)
    } finally {
      writer.close()
    }
    tempFile
  }

  @Test
  def testJoin(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    // Contents for the temp files
    val content1 =
      """
        |Water, 0
        |Tonic, 5
        |Juice, 10
      """.stripMargin

    val content2 =
      """
        |Apple juice, Juice
        |Tap water, Water
        |Orange juice, Juice
      """.stripMargin

    // Create temp files
    val tempFile1 = createTempFile(content1, "testFile1")
    val tempFile2 = createTempFile(content2, "testFile2")

    // Build initial data quanta
    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.readTextFile(s"file://$tempFile1")
      .map(s => {
        val values = s.split(", ")
        (values(0), values(1).toInt)
      })
    val dataQuanta2 = builder.readTextFile(s"file://$tempFile2")
      .map(s => {
        val values = s.split(", ")
        (values(0), values(1))
      })

    // Join
    val dq = dataQuanta1
      .join[(String, String), String](_._1, dataQuanta2, _._2)
      .map(joinTuple => (joinTuple.field1._1, joinTuple.field0._2))

    // Assert output
    val expectedValues = List(("Apple juice", 10), ("Tap water", 0), ("Orange juice", 10))
      .map(s => s.toString())
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)

    // Clean up: Delete the temp files
    tempFile1.delete()
    tempFile2.delete()
  }

  @Test
  def testJoin2(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(1, 3, 2, 4, 5)
    val inputValues2 = Array(3, 5)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val dq = dataQuanta1
      .join[Int, Int](n => n, dataQuanta2, n => n)
      .map(joinTuple => joinTuple.field0)

    val expectedValues = List("3", "5")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testCoGroup(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    // Contents for the temp files
    val content1 =
      """
        |Water, 0
        |Cola, 5
        |Juice, 10
      """.stripMargin

    val content2 =
      """
        |Apple juice, Juice
        |Tap water, Water
        |Orange juice, Juice
      """.stripMargin

    // Create temp files
    val tempFile1 = createTempFile(content1, "testFile1")
    val tempFile2 = createTempFile(content2, "testFile2")

    // Build initial data quanta
    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.readTextFile(s"file://$tempFile1")
      .map(s => {
        val values = s.split(", ")
        (values(0), values(1).toInt)
      })
    val dataQuanta2 = builder.readTextFile(s"file://$tempFile2")
      .map(s => {
        val values = s.split(", ")
        (values(0), values(1))
      })

    // Co-group
    val dq = dataQuanta1
      .coGroup[(String, String), String](_._1, dataQuanta2, _._2)

    // Assert output
    val expectedValues = List(
      "([(Water,0)], [(Tap water,Water)])",
      "([(Juice,10)], [(Apple juice,Juice), (Orange juice,Juice)])",
      "([(Cola,5)], [])"
    )
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)

    // Clean up: Delete the temp files
    tempFile1.delete()
    tempFile2.delete()
  }


  @Test
  def testUnion(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(1, 2, 3, 4)
    val inputValues2 = Array(0, 1, 3, 5)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val dq = dataQuanta1.union(dataQuanta2)

    val unionExpectedValues = List(1, 2, 3, 4, 0, 1, 3, 5).map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, unionExpectedValues)
  }

  @Test
  def testIntersect(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues1 = Array(1, 2, 3, 4, 5, 7, 8, 9, 10)
    val inputValues2 = Array(9, 0, 2, 3, 3, 4, 5, 7, 8, 11)

    val builder = new PlanBuilder(wayang)
    val dataQuanta1 = builder.loadCollection(inputValues1)
    val dataQuanta2 = builder.loadCollection(inputValues2)
    val dq = dataQuanta1.intersect(dataQuanta2)

    val intersectExpectedValues = List(2, 3, 4, 5, 7, 8, 9).map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, intersectExpectedValues)
  }

  @Test
  def testRepeat(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2)

    val dq = wayang
      .loadCollection(inputValues)
      .repeat(3,
        _.reduce(_ * _)
          .flatMap(v => Seq(v, v + 1))
      )

    // initial: 1,2 -> 1st: 2,3 -> 2nd: 6,7 => 3rd: 42,43
    val expectedValues = List("42", "43")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testRepeat2(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2)

    val dq = wayang
      .loadCollection(inputValues)
      .repeat(3,
        _.reduce(_ * _)
          .flatMap(v => Seq(v, v + 1))
      )
      .filter(n => n == 43)

    // initial: 1,2 -> 1st: 2,3 -> 2nd: 6,7 => 3rd: 42,43
    val expectedValues = List("43")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testRepeat3(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2, 3, 4, 5)

    val dq = wayang
      .loadCollection(inputValues)
      .repeat(10, _.map(_ + 1))

    val expectedValues = List("11", "12", "13", "14", "15")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testDoWhile(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2)

    val dq = wayang
      .loadCollection(inputValues)
      .doWhile[Int](vals => vals.max > 100, {
        start =>
          val sum = start.reduce(_ + _).withName("Sum")
          (start.union(sum), sum)
      })

    val expectedValues = List(1, 2, 3, 6, 12, 24, 48, 96, 192).map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testDoWhile2(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = Array(1, 2)

    val dq = wayang
      .loadCollection(inputValues)
      .doWhile[Int](vals => vals.max > 100, {
        start =>
          val sum = start.reduce(_ + _).withName("Sum")
          (start.union(sum), sum)
      })
      .filter(n => n > 50)

    val expectedValues = List(96, 192).map(_.toString)
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

  @Test
  def testSample(): Unit = {
    val wayang = new WayangContext().withPlugin(Java.basicPlugin)

    val inputValues = for (i <- 0 until 100) yield i

    val dq = wayang
      .loadCollection(inputValues)
      .sample(10)

    val tempFilenameOut = serializeDeserializeExecute(dq.operator, wayang)
    SerializationTestBase.assertOutputFileLineCount(tempFilenameOut, 10)
  }

  @Test
  def testSqlite3(): Unit = {
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
    val wayang = new MultiContext(configuration).withPlugin(Java.basicPlugin).withPlugin(Sqlite3.plugin)

    // Build plan
    val dq = wayang
      .readTable(new Sqlite3TableSource("customer", "name", "age"))
      .filter(r => r.getField(1).asInstanceOf[Integer] >= 18, sqlUdf = "age >= 18").withTargetPlatforms(Java.platform)
      .projectRecords(Seq("name"))
      .map(_.getField(0).asInstanceOf[String])

    // Execute and assert
    val expectedValues = List("John", "Evelyn")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

//  @Test
  def testPostgres(): Unit = {
    // Initialize some test data.
    val configuration = new Configuration
    configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/test_erp")
    configuration.setProperty("wayang.postgres.jdbc.user", "postgres")
    configuration.setProperty("wayang.postgres.jdbc.password", "1234")

    // Set up WayangContext.
    val wayang = new MultiContext(configuration).withPlugin(Java.basicPlugin).withPlugin(Postgres.plugin)

    // Build plan
    val dq = wayang
      .readTable(new PostgresTableSource("clients", "first_name", "role"))
      .projectRecords(Seq("first_name"))
      .map(_.getField(0).asInstanceOf[String]).withTargetPlatforms(Java.platform)

    // Execute and assert
    val expectedValues = List("John", "Jane")
    serializeDeserializeExecuteAssert(dq.operator, wayang, expectedValues)
  }

}
