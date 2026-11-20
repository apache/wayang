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

package org.apache.wayang.apps.kmeans.postgres

import org.apache.wayang.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper}

import java.util
import org.apache.wayang.api.{PlanBuilder, _}
import org.apache.wayang.apps.util.ProfileDBHelper
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.ExecutionContext
import org.apache.wayang.core.function.FunctionDescriptor.ExtendedSerializableFunction
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators
import org.apache.wayang.core.plugin.Plugin
import org.apache.wayang.core.util.fs.FileSystems
import org.apache.wayang.postgres.operators.PostgresTableSource

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.util.Random


/**
  * K-Means app for Apache Wayang on PostgreSQL.
  * <p>Note the UDF load property `wayang.apps.kmeans.udfs.select-centroid.load`.</p>
  */
class Kmeans(plugin: Plugin*) {

  def apply(k: Int, tableName: String, iterations: Int = 20, isResurrect: Boolean = true)
           (implicit experiment: Experiment, configuration: Configuration): Iterable[Point] = {
    // Set up the WayangContext.
    implicit val wayangCtx = new WayangContext(configuration)
    plugin.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)
      .withJobName(s"k-means ($tableName, k=$k, $iterations iterations)")
      .withExperiment(experiment)
      .withUdfJarsOf(this.getClass)

    // Read and parse the input file(s).
    val points = planBuilder
      .readTable(new PostgresTableSource(tableName, "x", "y")).withName("Read file")
      .map(record => Point(record.getDouble(0), record.getDouble(1))).withName("Create points")

    // Create initial centroids.
    val initialCentroids = planBuilder
      .loadCollection(Kmeans.createRandomCentroids(k)).withName("Load random centroids")

    // Do the k-means loop.
    val finalCentroids = initialCentroids.repeat(iterations, { currentCentroids =>
      val newCentroids = points
        .mapJava(
          new SelectNearestCentroid,
          udfLoad = LoadProfileEstimators.createFromSpecification("wayang.apps.kmeans.udfs.select-centroid.load", configuration)
        )
        .withBroadcast(currentCentroids, "centroids").withName("Find nearest centroid")
        .reduceByKey(_.centroidId, _ + _).withName("Add up points")
        .withCardinalityEstimator(k)
        .map(_.average).withName("Average points")


      if (isResurrect) {
        // Resurrect "lost" centroids (that have not been nearest to ANY point).
        val _k = k
        val resurrectedCentroids = newCentroids
          .map(centroid => 1).withName("Count centroids (a)")
          .reduce(_ + _).withName("Count centroids (b)")
          .flatMap(num => {
            if (num < _k) println(s"Resurrecting ${_k - num} point(s).")
            Kmeans.createRandomCentroids(_k - num)
          }).withName("Resurrect centroids")
        newCentroids.union(resurrectedCentroids).withName("New+resurrected centroids").withCardinalityEstimator(k)
      } else newCentroids
    }).withName("Loop")

    // Collect the result.
    finalCentroids
      .map(_.toPoint).withName("Strip centroid names")
      .collect()
  }


}

/**
  * Companion object of [[Kmeans]].
  */
object Kmeans extends ExperimentDescriptor {

  override def version = "0.1.0"

  def main(args: Array[String]): Unit = {
    // Parse args.
    if (args.length == 0) {
      println(s"Usage: scala <main class> ${Parameters.experimentHelp} <plugin(,plugin)*> <table> <k> <#iterations>")
      sys.exit(1)
    }

    implicit val experiment = Parameters.createExperiment(args(0), this)
    implicit val configuration = new Configuration
    val plugins = Parameters.loadPlugins(args(1))
    experiment.getSubject.addConfiguration("plugins", args(1))
    val file = args(2)
    experiment.getSubject.addConfiguration("input", args(2))
    val k = args(3).toInt
    experiment.getSubject.addConfiguration("k", args(3))
    val numIterations = args(4).toInt
    experiment.getSubject.addConfiguration("iterations", args(4))

    // Initialize k-means.
    val kmeans = new Kmeans(plugins: _*)

    // Run k-means.
    val centroids = kmeans(k, file, numIterations)

    // Store experiment data.
    val fileSize = FileSystems.getFileSize(file)
    if (fileSize.isPresent) experiment.getSubject.addConfiguration("inputSize", fileSize.getAsLong)
    ProfileDBHelper.store(experiment, configuration)

    // Print the result.
    println(s"Found ${centroids.size} centroids:")

  }

  /**
    * Creates random centroids.
    *
    * @param n      the number of centroids to create
    * @param random used to draw random coordinates
    * @return the centroids
    */
  def createRandomCentroids(n: Int, random: Random = new Random()): IndexedSeq[TaggedPoint] =
  // TODO: The random cluster ID makes collisions during resurrection less likely but in general permits ID collisions.
    for (i <- 1 to n) yield TaggedPoint(random.nextGaussian(), random.nextGaussian(), random.nextInt())

}

/**
  * UDF to select the closest centroid for a given [[Point]].
  */
class SelectNearestCentroid extends ExtendedSerializableFunction[Point, TaggedPointCounter] {

  /** Keeps the broadcasted centroids. */
  var centroids: util.Collection[TaggedPoint] = _

  override def open(executionCtx: ExecutionContext) = {
    centroids = executionCtx.getBroadcast[TaggedPoint]("centroids")
  }

  override def apply(point: Point): TaggedPointCounter = {
    var minDistance = Double.PositiveInfinity
    var nearestCentroidId = -1
    for (centroid <- centroids) {
      val distance = point.distanceTo(centroid)
      if (distance < minDistance) {
        minDistance = distance
        nearestCentroidId = centroid.centroidId
      }
    }
    new TaggedPointCounter(point, nearestCentroidId, 1)
  }
}


/**
  * Represents objects with an x and a y coordinate.
  */
sealed trait PointLike {

  /**
    * @return the x coordinate
    */
  def x: Double

  /**
    * @return the y coordinate
    */
  def y: Double

}

/**
  * Represents a two-dimensional point.
  *
  * @param x the x coordinate
  * @param y the y coordinate
  */
case class Point(x: Double, y: Double) extends PointLike {

  /**
    * Calculates the Euclidean distance to another [[Point]].
    *
    * @param that the other [[PointLike]]
    * @return the Euclidean distance
    */
  def distanceTo(that: PointLike) = {
    val dx = this.x - that.x
    val dy = this.y - that.y
    math.sqrt(dx * dx + dy * dy)
  }

  override def toString: String = f"($x%.2f, $y%.2f)"
}

/**
  * Represents a two-dimensional point with a centroid ID attached.
  */
case class TaggedPoint(x: Double, y: Double, centroidId: Int) extends PointLike {

  /**
    * Creates a [[Point]] from this instance.
    *
    * @return the [[Point]]
    */
  def toPoint = Point(x, y)

}

/**
  * Represents a two-dimensional point with a centroid ID and a counter attached.
  */
case class TaggedPointCounter(x: Double, y: Double, centroidId: Int, count: Int = 1) extends PointLike {

  def this(point: PointLike, centroidId: Int, count: Int) = this(point.x, point.y, centroidId, count)

  /**
    * Adds coordinates and counts of two instances.
    *
    * @param that the other instance
    * @return the sum
    */
  def +(that: TaggedPointCounter) = TaggedPointCounter(this.x + that.x, this.y + that.y, this.centroidId, this.count + that.count)

  /**
    * Calculates the average of all added instances.
    *
    * @return a [[TaggedPoint]] reflecting the average
    */
  def average = TaggedPoint(x / count, y / count, centroidId)

}
