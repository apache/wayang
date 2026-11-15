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
import org.apache.wayang.apps.tpch.data.{Customer, LineItem, Order}
import org.apache.wayang.apps.util.ExperimentDescriptor
import org.apache.wayang.api._
import org.apache.wayang.apps.tpch.data.LineItem
import org.apache.wayang.commons.util.profiledb.model.Experiment
import org.apache.wayang.core.api.{Configuration, WayangContext}
import org.apache.wayang.core.plugin.Plugin
import org.apache.wayang.jdbc.operators.JdbcTableSource
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate

/**
  * Apache Wayang implementation of TPC-H Query 3.
  *
  * {{{
  * select
  *   l_orderkey,
  *   sum(l_extendedprice*(1-l_discount)) as revenue,
  *   o_orderdate,
  *   o_shippriority
  * from
  *   customer,
  *   orders,
  *   lineitem
  * where
  *   c_mktsegment = '[SEGMENT]'
  *   and c_custkey = o_custkey
  *   and l_orderkey = o_orderkey
  *   and o_orderdate < date '[DATE]'
  *   and l_shipdate > date '[DATE]'
  * group by
  *   l_orderkey,
  *   o_orderdate,
  *   o_shippriority
  * order by
  *   revenue desc,
  *   o_orderdate;
  * }}}
  */
class Query3Database(plugins: Plugin*) extends ExperimentDescriptor {

  override def version = "0.1.0"

  def apply(configuration: Configuration,
            jdbcPlatform: JdbcPlatformTemplate,
            createTableSource: (String, Seq[String]) => JdbcTableSource,
            segment: String = "BUILDING",
            date: String = "1995-03-15")
           (implicit experiment: Experiment) = {

    val wayangCtx = new WayangContext(configuration)
    plugins.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)
      .withJobName(s"TPC-H (${this.getClass.getSimpleName})")
      .withUdfJarsOf(classOf[Query3Database])
      .withExperiment(experiment)

    val schema = configuration.getOptionalStringProperty("wayang.apps.tpch.schema").orElse(null)
    def withSchema(table: String) = schema match {
      case null => table
      case str: String => s"$str.$table"
    }

    experiment.getSubject.addConfiguration("jdbcUrl", configuration.getStringProperty(jdbcPlatform.jdbcUrlProperty))
    if (schema != null) experiment.getSubject.addConfiguration("schema", schema)
    experiment.getSubject.addConfiguration("segment", segment)
    experiment.getSubject.addConfiguration("date", date)

    // Read, filter, and project the customer data.
    val _segment = segment
    val customerKeys = planBuilder
      .readTable(createTableSource(withSchema("CUSTOMER"), Customer.fields))
      .withName("Load CUSTOMER table")

      .filter(_.getString(6) == _segment, sqlUdf = s"c_mktsegment LIKE '$segment%'", selectivity = .25)
      .withName("Filter customers")

      .projectRecords(Seq("c_custkey"))
      .withName("Project customers")

      .map(_.getLong(0))
      .withName("Extract customer ID")

    // Read, filter, and project the order data.
    val _date = CsvUtils.parseDate(date)
    val orders = planBuilder
      .load(createTableSource(withSchema("ORDERS"), Order.fields))
      .withName("Load ORDERS table")

      .filter(t => CsvUtils.parseDate(t.getString(4)) > _date, sqlUdf = s"o_orderdate < date('$date')")
      .withName("Filter orders")

      .projectRecords(Seq("o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"))
      .withName("Project orders")

      .map(order => (order.getLong(0), // orderKey
        order.getLong(1), // custKey
        CsvUtils.parseDate(order.getString(2)), // orderDate
        order.getInt(3)) // shipPriority
      )
      .withName("Unpack orders")

    // Read, filter, and project the line item data.
    val lineItems = planBuilder
      .readTable(createTableSource(withSchema("LINEITEM"), LineItem.fields))
      .withName("Load LINEITEM table")

      .filter(t => CsvUtils.parseDate(t.getString(10)) > _date, sqlUdf = s"l_shipDate > date('$date')")
      .withName("Filter line items")

      .projectRecords(Seq("l_orderkey", "l_extendedprice", "l_discount"))
      .withName("Project line items")

      .map(li => (
        li.getLong(0), //li.orderKey,
        li.getDouble(1) * (1 - li.getDouble(2)) //li.extendedPrice * (1 - li.discount)
      ))
      .withName("Extract line item data")

    // Join and aggregate the different datasets.
    customerKeys
      .join[(Long, Long, Int, Int), Long](identity, orders, _._2)
      .withName("Join customers with orders")

      .map(_.field1) // (orderKey, custKey, orderDate, shipPriority)
      .withName("Project customer-order join product")

      .join[(Long, Double), Long](_._1, lineItems, _._1)
      .withName("Join CO with line items")

      .map(coli => Query3Result(
        orderKey = coli.field1._1,
        revenue = coli.field1._2,
        orderDate = coli.field0._3,
        shipPriority = coli.field0._4
      ))
      .withName("Project CO-line-item join product")

      .reduceByKey(
        t => (t.orderKey, t.orderDate, t.shipPriority),
        (t1, t2) => {
          t1.revenue += t2.revenue;
          t2
        }
      )
      .withName("Aggregate revenue")
      .collect()
  }

}
