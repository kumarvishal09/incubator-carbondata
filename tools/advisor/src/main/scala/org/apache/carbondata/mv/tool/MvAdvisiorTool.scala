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
package org.apache.carbondata.mv.tool

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.mv.tool.offlineadvision.OfflineAdvisior
import org.apache.carbondata.mv.tool.reader.JsonFileReader

object MvAdvisiorTool {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      sys.error("Invalid arguments! " +
                "Valid Format:" +
                "query log file path, table details file path, output filepath, store path(optional)")
    }
    if (!FileFactory.isFileExist(args(0))) {
      sys.error("Query Log file path is invalid. Please provide valid file path")
    }
    if (!FileFactory.isFileExist(args(1))) {
      sys.error("Table details file path is invalid. Please provide valid file path")
    }
    if (!FileFactory.isFileExist(args(2))) {
      sys.error("Output folder path is invalid. Please provide valid folder path")
    }
    val querySqls = JsonFileReader.readQuerySql(args(0)).asScala
    val tableDetails = JsonFileReader.readTableDetails(args(1)).asScala
    val outputFileName = "/mv-candidate_" + System.nanoTime() + ".sql"
    val outLocation: String = args(2) + outputFileName
    val storePath = if (args.length > 4) {
      args(4)
    } else {
      null
    }
    val sparkSession = createSession(storePath, args(3))
    OfflineAdvisior(sparkSession).adviseMV(querySqls, tableDetails, outLocation)
  }

  private def createSession(storePath: String, masterUrl:String): SparkSession = {
    import org.apache.spark.sql.CarbonSession._
    val sparkConf = new SparkConf(loadDefaults = true)
    val builder = SparkSession
      .builder()
      .master(masterUrl)
      .config(sparkConf)
      .appName("MV Advisior(uses CarbonSession)")
      .enableHiveSupport()
    builder.getOrCreateCarbonSession(storePath)
  }
}
