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

package org.apache.wayang.apps.sindy

import java.lang.Iterable
import java.util
import org.apache.wayang.api._
import org.apache.wayang.apps
import Sindy.{CellCreator, CellMerger, IndCandidateGenerator, IndCandidateMerger}
import org.apache.wayang.apps.util.{ExperimentDescriptor, Parameters, ProfileDBHelper, StdOut}
import org.apache.wayang.apps.util.ProfileDBHelper
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.function.FunctionDescriptor.{SerializableBinaryOperator, SerializableFunction}
import org.apache.wayang.core.plugin.Plugin
import org.apache.wayang.core.util.fs.FileSystems

/**
  * This is an Apache Wayang-based implementation of the SINDY algorithm.
  */
class Sindy(plugins: Plugin*) {

  /**
    * Execute the SINDY algorithm.
    *
    * @param paths         input file or directory URLs
    * @param seperator     CSV separator in the files
    * @param configuration Apache Wayang configuration
    * @param experiment    the experiment to log measurements to
    * @return the INDs
    */
  def apply(paths: Seq[String], seperator: Char = ';')
           (implicit configuration: Configuration, experiment: Experiment) = {
    val wayangContext = new WayangContext(configuration)
    plugins.foreach(wayangContext.register)
    val planBuilder = new PlanBuilder(wayangContext)
      .withJobName(s"Sindy ($paths)")
      .withExperiment(experiment)
      .withUdfJarsOf(classOf[Sindy])

    val fileColumnIdOffsets = paths.flatMap(resolveDirs).zipWithIndex.map { case (url, index) => (url, index * 1000) }
    val allCells = fileColumnIdOffsets
      .map { case (path, offset) =>
        planBuilder
          .readTextFile(path).withName(s"Load $path")
          .flatMapJava(new CellCreator(offset, seperator)).withName(s"Create cells for $path")
      }
      .reduce(_ union _)

    val rawInds = allCells
      .map(cell => (cell._1, Array(cell._2))).withName("Prepare cell merging")
      .reduceByKeyJava(toSerializableFunction(_._1), new CellMerger).withName("Merge cells")
      .flatMapJava(new IndCandidateGenerator).withName("Generate IND candidate sets")
      .reduceByKeyJava(toSerializableFunction(_._1), new IndCandidateMerger).withName("Merge IND candidate sets")
      .filter(_._2.length > 0).withName("Filter empty candidate sets")
      .collect()

    def resolveColumnId(id: Int) = fileColumnIdOffsets
      .find { case (file, offset) => offset <= id && offset + 1000 > id } match {
      case Some((file, offset)) => s"$file[${id - offset}]"
      case _ => s"???[$id]"
    }

    rawInds.map {
      case (dep, refs) => (s"${resolveColumnId(dep)}", refs.map(resolveColumnId).toSeq)
    }
  }

  /**
    * If the given URL is a directory, list all its files recursively.
    *
    * @param url the URL
    * @return the file URLs
    */
  def resolveDirs(url: String): scala.Iterable[String] = {
    import scala.collection.JavaConversions._
    val fs = FileSystems.requireFileSystem(url)
    if (fs.isDirectory(url)) fs.listChildren(url).flatMap(resolveDirs) else Seq(url)
  }

}

/**
  * Companion object for [[Sindy]].
  */
object Sindy extends ExperimentDescriptor {

  def version = "0.1.0"

  def main(args: Array[String]): Unit = {
    // Parse parameters.
    if (args.isEmpty) {
      sys.error(s"Usage: <main class> ${Parameters.experimentHelp} <plugin>(,<plugin>)* <CSV separator> <input URL>(;<input URL>)*")
      sys.exit(1)
    }

    implicit val configuration = new Configuration
    implicit val experiment = Parameters.createExperiment(args(0), this)
    val plugins = Parameters.loadPlugins(args(1))
    experiment.getSubject.addConfiguration("plugins", args(1))
    val separator = if (args(2).length == 1) args(2).charAt(0) else args(2) match {
      case "tab" => '\t'
      case "\\t" => '\t'
      case "comma" => ','
      case "semicolon" => ';'
      case "\\|" => '|'
      case "pipe" => '|'
      case other: String => throw new IllegalArgumentException("Unknown separator.")
    }
    val inputUrls = args(3).split(";")
    experiment.getSubject.addConfiguration("inputs", inputUrls)

    // Prepare the PageRank.
    val sindy = new Sindy(plugins: _*)

    // Run the PageRank.
    val inds = sindy(inputUrls.toSeq, separator).toSeq

    // Store experiment data.
    val inputFileSizes = inputUrls.map(url => FileSystems.getFileSize(url))
    if (inputFileSizes.forall(_.isPresent))
      experiment.getSubject.addConfiguration("inputSize", inputFileSizes.map(_.getAsLong).sum)
    ProfileDBHelper.store(experiment, configuration)

    // Print the result.
    StdOut.printLimited(inds)
  }

  /**
    * UDF to parse a CSV row and create cells.
    *
    * @param offset the column ID offset for the input CSV rows
    */
  class CellCreator(val offset: Int, val separator: Char) extends SerializableFunction[String, java.lang.Iterable[(String, Int)]] {

    override def apply(row: String): Iterable[(String, Int)] = {
      val fields = row.split(separator)
      val cells = new util.ArrayList[(String, Int)](fields.length)
      var columnId = offset
      for (field <- fields) {
        cells.add((field, columnId))
        columnId += 1
      }
      cells
    }
  }

  /**
    * UDF to merge the column IDs of two cells.
    */
  class CellMerger extends SerializableBinaryOperator[(String, Array[Int])] {

    import scala.collection.mutable

    lazy val merger = mutable.Set[Int]()

    override def apply(cell1: (String, Array[Int]), cell2: (String, Array[Int])): (String, Array[Int]) = {
      merger.clear()
      for (columnId <- cell1._2) merger += columnId
      for (columnId <- cell2._2) merger += columnId
      (cell1._1, merger.toArray)
    }
  }

  /**
    * UDF to create IND candidates from a cell group.
    */
  class IndCandidateGenerator extends SerializableFunction[(String, Array[Int]), java.lang.Iterable[(Int, Array[Int])]] {

    override def apply(cellGroup: (String, Array[Int])): java.lang.Iterable[(Int, Array[Int])] = {
      val columnIds = cellGroup._2
      val result = new util.ArrayList[(Int, Array[Int])](columnIds.length)
      for (i <- columnIds.indices) {
        val refColumnIds = new Array[Int](columnIds.length - 1)
        java.lang.System.arraycopy(columnIds, 0, refColumnIds, 0, i)
        java.lang.System.arraycopy(columnIds, i + 1, refColumnIds, i, refColumnIds.length - i)
        result.add((columnIds(i), refColumnIds))
      }
      result
    }
  }

  /**
    * UDF to merge two IND candidates.
    */
  class IndCandidateMerger extends SerializableBinaryOperator[(Int, Array[Int])] {

    import scala.collection.mutable

    lazy val merger = mutable.Set[Int]()

    override def apply(indc1: (Int, Array[Int]), indc2: (Int, Array[Int])): (Int, Array[Int]) = {
      merger.clear()
      for (columnId <- indc1._2) merger += columnId
      (indc1._1, indc2._2.filter(merger.contains))
    }

  }


}
