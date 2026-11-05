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

import zio._
import zio.http._
import scala.util.Try
import java.io.{StringWriter, PrintWriter}

import org.apache.wayang.api.json.builder.JsonPlanBuilder
import org.apache.wayang.api.json.operatorfromdrawflow.OperatorFromDrawflowConverter
import org.apache.wayang.api.json.operatorfromjson.OperatorFromJson
import org.apache.wayang.api.json.operatorfromjson.unary.MapPartitionsOperatorFromJson
import org.apache.wayang.api.json.parserutil.ParseOperatorsFromDrawflow
import org.apache.wayang.api.json.parserutil.ParsePlanFromJson

object Main extends ZIOAppDefault {
  val drawRoute =
    Method.POST / "wayang-api-json" / "submit-plan" / "drawflow-format" -> handler { (req: Request) =>
     (for {
        requestBody <- req.body.asString
        operatorsFromDrawflow <- ZIO.fromTry(Try(ParseOperatorsFromDrawflow.parseOperatorsFromString(requestBody).get))
        operatorsFromJson = operatorsFromDrawflow.flatMap(op => OperatorFromDrawflowConverter.toOperatorFromJson(op))
        result <- ZIO.attempt(new JsonPlanBuilder().setOperators(operatorsFromJson).execute())
        responseBody <- ZIO.attempt {
          if (operatorsFromJson.exists(op => op.cat == OperatorFromJson.Categories.Output))
            "Success"
          else
            result.collect().toString()
        }
        resBody <- ZIO.succeed(Response.text(responseBody))
     } yield resBody).catchAll(t => ZIO.succeed(Response.text(t.getMessage)))
  }

  val jsonRoute =
    Method.POST / "wayang-api-json" / "submit-plan" / "json" -> handler { (req: Request) =>
     (for {
        requestBody <- req.body.asString
        planFromJson <- ZIO.fromTry(Try(ParsePlanFromJson.parsePlanFromString(requestBody).get))
        _ <- ZIO.attempt {
          if (planFromJson.collect) {
            require(!planFromJson.operators.exists(_.cat == OperatorFromJson.Categories.Output),
              "Collect plans cannot contain output sinks")
            require(planFromJson.operators.count(_.output.isEmpty) == 1,
              "Collect plans require exactly one terminal operator")
          }
        }
        result <- ZIO.attempt(new JsonPlanBuilder().fromPlan(planFromJson).execute())
        responseBody <- ZIO.attempt {
          if (planFromJson.collect) {
            val jsonOutput = planFromJson.operators.exists {
              case op: MapPartitionsOperatorFromJson => op.output.isEmpty && op.data.jsonOutput
              case _ => false
            }
            Response.json(CollectResult.response(result.collect(), jsonOutput))
          } else if (planFromJson.operators.exists(op => op.cat == OperatorFromJson.Categories.Output)) {
            println("Successfully executed WayangJob")
            Response.text("Success")
          } else {
            Response.text(result.collect().toString())
          }
        }
     } yield responseBody).catchAll(t => {
      // Error messages printed in server
      t.printStackTrace

      // Error messages sent to client
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)
      t.printStackTrace(printWriter)
      printWriter.flush()
      val fullErrorOutput = stringWriter.toString()

      // Returns status code and the stacktrace (error message)
      ZIO.succeed(Response(status = Status.BadRequest, body = Body.fromString(fullErrorOutput)))
     })
    }

  // Create HTTP route
  val app = Routes(drawRoute, jsonRoute).toHttpApp

  // Run it like any simple app
  def run = for {
    args <- getArgs
    _ <- Server.serve(app).provide(Server.defaultWithPort((args.headOption getOrElse "8080" toInt)))
  } yield ()
}
