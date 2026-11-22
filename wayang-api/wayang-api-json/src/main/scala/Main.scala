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
import org.apache.wayang.api.json.parserutil.ParseOperatorsFromDrawflow
import org.apache.wayang.api.json.parserutil.ParsePlanFromJson

object Main extends ZIOAppDefault {
  // Wayang cannot run several jobs at the same time, so jobs wait for this one permit and run one
  // after another. They run with attemptBlocking so a long job does not hold up the threads that
  // serve other requests; a waiting request is suspended, not blocking a thread.
  def runJob[A](job: => A, jobLock: Semaphore): Task[A] =
    jobLock.withPermit(ZIO.attemptBlocking(job))

  def drawRoute(jobLock: Semaphore) =
    Method.POST / "wayang-api-json" / "submit-plan" / "drawflow-format" -> handler { (req: Request) =>
     (for {
        requestBody <- req.body.asString
        operatorsFromDrawflow <- ZIO.fromTry(Try(ParseOperatorsFromDrawflow.parseOperatorsFromString(requestBody).get))
        operatorsFromJson = operatorsFromDrawflow.flatMap(op => OperatorFromDrawflowConverter.toOperatorFromJson(op))
        responseBody <- runJob({
          val result = new JsonPlanBuilder().setOperators(operatorsFromJson).execute()
          if (operatorsFromJson.exists(op => op.cat == OperatorFromJson.Categories.Output))
            "Success"
          else
            result.collect().toString()
        }, jobLock)
        resBody <- ZIO.succeed(Response.text(responseBody))
     } yield resBody).catchAll(t => ZIO.succeed(Response.text(t.getMessage)))
  }

  def jsonRoute(jobLock: Semaphore) =
    Method.POST / "wayang-api-json" / "submit-plan" / "json" -> handler { (req: Request) =>
     (for {
        requestBody <- req.body.asString
        planFromJson <- ZIO.fromTry(Try(ParsePlanFromJson.parsePlanFromString(requestBody).get))
        responseBody <- runJob({
          val result = new JsonPlanBuilder().fromPlan(planFromJson).execute()
          if (planFromJson.operators.exists(op => op.cat == OperatorFromJson.Categories.Output)) {
            println("Successfully executed WayangJob")
            "Success"
          } else {
            result.collect().toString()
          }
        }, jobLock)
        resBody <- ZIO.succeed(Response.text(responseBody))
     } yield resBody).catchAll(t => {
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

  // Run it like any simple app
  def run = for {
    args <- getArgs
    jobLock <- Semaphore.make(1)
    app = Routes(drawRoute(jobLock), jsonRoute(jobLock))
    _ <- Server.serve(app).provide(Server.defaultWithPort((args.headOption getOrElse "8080" toInt)))
  } yield ()
}
