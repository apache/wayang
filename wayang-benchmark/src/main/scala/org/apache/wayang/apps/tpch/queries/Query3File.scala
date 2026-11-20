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
class Query3File(plugins: Plugin*) extends ExperimentDescriptor {

  override def version = "0.1.0"

  def apply(configuration: Configuration,
            segment: String = "BUILDING",
            date: String = "1995-03-15")
           (implicit experiment: Experiment) = {

    val wayangCtx = new WayangContext(configuration)
    plugins.foreach(wayangCtx.register)
    val planBuilder = new PlanBuilder(wayangCtx)
      .withUdfJarsOf(classOf[Query3Database])
      .withExperiment(experiment)
      .withJobName(s"TPC-H (${this.getClass.getSimpleName})")

    val customerFile = configuration.getStringProperty("wayang.apps.tpch.csv.customer")
    val ordersFile = configuration.getStringProperty("wayang.apps.tpch.csv.orders")
    val lineitemFile = configuration.getStringProperty("wayang.apps.tpch.csv.lineitem")

    experiment.getSubject.addConfiguration("customerInput", customerFile)
    experiment.getSubject.addConfiguration("ordersInput", ordersFile)
    experiment.getSubject.addConfiguration("lineitemInput", lineitemFile)
    experiment.getSubject.addConfiguration("segment", segment)
    experiment.getSubject.addConfiguration("date", date)

    // Read, filter, and project the customer data.
    val _segment = segment
    val customerKeys = planBuilder
      .readTextFile(customerFile)
      .withName("Read customers")
      .map(Customer.parseCsv)
      .withName("Parse customers")

      .filter(_.mktSegment == _segment, selectivity = .25)
      .withName("Filter customers")

      .map(_.custKey)
      .withName("Project customers")

    // Read, filter, and project the order data.
    val _date = CsvUtils.parseDate(date)
    val orders = planBuilder
      .readTextFile(ordersFile)
      .withName("Read orders")
      .map(Order.parseCsv)
      .withName("Parse orders")

      .filter(_.orderDate < _date)
      .withName("Filter orders")

      .map(order => (order.orderKey, order.custKey, order.orderDate, order.shipPrioritiy))
      .withName("Project orders")

    // Read, filter, and project the line item data.
    val lineItems = planBuilder
      .readTextFile(lineitemFile)
      .withName("Read line items")
      .map(LineItem.parseCsv)
      .withName("Parse line items")

      .filter(_.shipDate > _date)
      .withName("Filter line items")

      .map(li => (li.orderKey, li.extendedPrice * (1 - li.discount)))
      .withName("Project line items")

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

case class Query3Result(orderKey: Long, var revenue: Double, orderDate: Int, shipPriority: Int)
