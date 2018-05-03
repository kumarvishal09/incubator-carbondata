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

package org.apache.carbondata.examples

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonSessionExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    System.setProperty("path.target", s"$rootPath/examples/spark2/target")
    // print profiler log to a separated file: target/profiler.log
    PropertyConfigurator.configure(
      s"$rootPath/examples/spark2/src/main/resources/log4j.properties")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "false")
    val spark = ExampleUtils.createCarbonSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("INFO")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

//    spark.sql(
    //      "create table complexcarbontable(mobile struct<imei:string, imsi:string>)  STORED BY 'org.apache.carbondata.format'")
    //    spark.sql("desc formatted complexcarbontable").show(truncate = false)
    //    spark.sql("LOAD DATA local inpath '" + "/home/root1/samplecsvfiles" +
    //        "/complextypesample.csv' INTO table complexcarbontable  OPTIONS('DELIMITER'=',', " +
    //        "'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,ROMName," +
    //        "purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber', " +
    //        "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')");
    //    spark.sql("select * from complexcarbontable").show()
//
    spark.sql("drop table if exists complexcarbontable")
    spark.sql(
      "create table complexcarbontable(deviceInformationId int, channelsId string, ROMSize " +
      "string, ROMName String, purchasedate string, mobile struct<imei:string, imsi:string>, MAC " +
      "array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, " +
      "ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, " +
      "proddate struct<productionDate:string,activeDeactivedate:array<string>>)  STORED BY 'org.apache.carbondata.format'")
        spark.sql("LOAD DATA local inpath '" + "/home/root1/samplecsvfiles" +
            "/complextypesample.csv' INTO table complexcarbontable  OPTIONS('DELIMITER'=',', " +
            "'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,ROMName," +
            "purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber', " +
            "'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')");
    spark.sql("select * from complexcarbontable").show
  }
}