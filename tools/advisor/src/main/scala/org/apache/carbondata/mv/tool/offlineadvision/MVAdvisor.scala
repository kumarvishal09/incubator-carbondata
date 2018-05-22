/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.mv.tool.offlineadvision

import java.io.{File, PrintWriter}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.mv.dsl._
import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.carbondata.mv.plans.util.Signature
import org.apache.carbondata.mv.tool.manager.CommonSubexpressionManager
import org.apache.carbondata.mv.tool.preprocessor.QueryBatchPreprocessor

/**
 * Class responsibility is to advise mv
 */
class MVAdvisor extends Logging{
  def adviseMVs(
      spark: SparkSession,
      qbPreprocessor: QueryBatchPreprocessor,
      csemanager: CommonSubexpressionManager,
      queryList: Seq[String],
      outPath: String) {
    val mvWriter = new PrintWriter(new File(outPath))
    for ((signature, batchBySignature) <- createQueryBatches(
      spark,
      qbPreprocessor,
      queryList)) {
      val cses = csemanager.execute(batchBySignature)
      cses.foreach { case (cse, freq) =>
        mvWriter.println(s"${ cse.asCompactSQL };\n")
      }
    }
    mvWriter.close()
  }

  private def createQueryBatches(
      spark: SparkSession,
      qbPreprocessor: QueryBatchPreprocessor,
      queryList: Seq[String]): Iterator[(Option[Signature], Seq[(ModularPlan, Int)])] = {
    val planBatch = mutable.ArrayBuffer[ModularPlan]()
      // get modular plan for all the input query
    batchQueries(spark, queryList.iterator, planBatch)
    val preprocessedBatch = qbPreprocessor.preprocess(planBatch.map(plan => (plan, 1)))
    preprocessedBatch.groupBy(_._1.signature).toIterator
  }

  private def batchQueries(
      spark: SparkSession,
      qIterator: Iterator[String],
      pBatch: mutable.ArrayBuffer[ModularPlan]): Unit = {
    for (query <- qIterator) {
      val analyzed = spark.sql(query).queryExecution.analyzed
      // check if the plan is well-formed
      if (analyzed.resolved &&
          !(analyzed.missingInput.nonEmpty && analyzed.children.nonEmpty)) {
        // please see the comment on preHarmonized of ModularPlan for the assumption of
        // the form of queries for harmonization.
        // We assume queries in processing conform to the form.  If not, customize
        // preHarmonized so that, for each query, the fact table is at the front of the
        // FROM clause of the query (similar method to canonicalizeJoinOrderIfFeasible
        // of DefaultCommonSubexpressionManager).
        Try(analyzed.optimize.modularize.harmonized) match {
          case Success(m) => pBatch += m
          case Failure(e) =>
            logInfo("throw away query that does not have modular plan: " + query)
        }
      }
    }
  }
}
