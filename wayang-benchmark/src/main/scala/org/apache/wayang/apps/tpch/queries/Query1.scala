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

package org.apache.wayang.apps.tpch.queries

import org.apache.wayang.apps.tpch.CsvUtils
import org.apache.wayang.apps.tpch.data.LineItem
import org.apache.wayang.apps.util.ExperimentDescriptor
import org.apache.wayang.api._
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plugin.Plugin
import org.apache.wayang.jdbc.operators.JdbcTableSource
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate

/**
  * Apache Wayang implementation of TPC-H Query 1.
  *
  * {{{
  * select
  *   l_returnflag,
  *   l_linestatus,
  *   sum(l_quantity) as sum_qty,
  *   sum(l_extendedprice) as sum_base_price,
  *   sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
  *   sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
  *   avg(l_quantity) as avg_qty,
  *   avg(l_extendedprice) as avg_price,
  *   avg(l_discount) as avg_disc,
  *   count(*) as count_order
  * from
  *   lineitem
  * where
  *   l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
  * group by
  *   l_returnflag,
  *   l_linestatus
  * order by
  *   l_returnflag,
  *   l_linestatus;
  * }}}
  */
class Query1(plugins: Plugin*) extends ExperimentDescriptor {

  override def version = "0.1.0"

  def apply(configuration: Configuration,
            jdbcPlatform: JdbcPlatformTemplate,
            createTableSource: (String, Seq[String]) => JdbcTableSource,
            delta: Int = 90)
           (implicit experiment: Experiment): Iterable[Query1.Result] = {

    val wayangCtx = new WayangContext(configuration)
    plugins.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)
      .withJobName(s"TPC-H (${this.getClass.getSimpleName})")
      .withUdfJarsOf(classOf[Query1])
      .withExperiment(experiment)

    experiment.getSubject.addConfiguration("jdbcUrl", configuration.getStringProperty(jdbcPlatform.jdbcUrlProperty))
    experiment.getSubject.addConfiguration("delta", delta)

    // Read, filter, and project the customer data.
    val _delta = delta
    val result = planBuilder
      .readTable(createTableSource("LINEITEM", LineItem.fields))
      .withName("Load LINEITEM table")

      .filter(t => CsvUtils.parseDate(t.getString(10)) <= CsvUtils.parseDate("1998-12-01") - _delta,
        sqlUdf = s"date(l_shipdate) <= date('1998-12-01', '- ${_delta} day')", selectivity = .25)
      .withName("Filter line items")

      .projectRecords(Seq("l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax"))
      .withName("Project line items")

      .map(record => Query1.Result(
        record.getString(0),
        record.getString(1),
        record.getDouble(2),
        record.getDouble(3),
        record.getDouble(3) * (1 - record.getDouble(4)),
        record.getDouble(3) * (1 - record.getDouble(4)) * (1 + record.getDouble(5)),
        record.getDouble(2),
        record.getDouble(3),
        record.getDouble(4),
        1
      ))
      .withName("Calculate result fields")

      .reduceByKey(
        result => (result.l_returnflag, result.l_linestatus),
        (r1, r2) => Query1.Result(
          r1.l_returnflag,
          r1.l_linestatus,
          r1.sum_qty + r2.sum_qty,
          r1.sum_base_price + r2.sum_base_price,
          r1.sum_disc_price + r2.sum_disc_price,
          r1.sum_charge + r2.sum_charge,
          r1.avg_qty + r2.avg_qty,
          r1.avg_price + r2.avg_price,
          r1.avg_disc + r2.avg_disc,
          r1.count_order + r2.count_order
        )
      )
      .withName("Aggregate line items")

      .map(result => Query1.Result(
        result.l_returnflag,
        result.l_linestatus,
        result.sum_qty,
        result.sum_base_price,
        result.sum_disc_price,
        result.sum_charge,
        result.avg_qty / result.count_order,
        result.avg_price / result.count_order,
        result.avg_disc / result.count_order,
        result.count_order
      ))
      .withName("Post-process line item aggregates")
      .collect()

    result
  }

}

object Query1 {

  case class Result(l_returnflag: String,
                    l_linestatus: String,
                    sum_qty: Double,
                    sum_base_price: Double,
                    sum_disc_price: Double,
                    sum_charge: Double,
                    avg_qty: Double,
                    avg_price: Double,
                    avg_disc: Double,
                    count_order: Int)

}
