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

package org.apache.carbondata.externalstreaming


import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.command.management.CarbonAddExternalStreamingSegmentCommand
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog, HandOffOptions, HandoffHbaseSegmentCommand, SparkHBaseConf}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.functions._

import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hbase.HBaseConstants


class TestHBaseStreaming extends QueryTest with BeforeAndAfterAll {
  var htu: HBaseTestingUtility = _
  var hBaseConfPath:String = _
  val writeCat =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"Phoenix"},
       |"rowkey":"col0",
       |"columns":{
       |"rowkey.col0":{"cf":"rowkey", "col":"col0", "type":"int"},
       |"cf1.col0":{"cf":"cf1", "col":"col0", "type":"int"},
       |"cf1.col1":{"cf":"cf1", "col":"col1", "type":"string"},
       |"cf1.col2":{"cf":"cf1", "col":"col2", "type":"int"}
       |}
       |}""".stripMargin

  def withCatalog(cat: String): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  val writeCatTimestamp =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleTable1", "tableCoder":"Phoenix"},
       |"rowkey":"col0",
       |"columns":{
       |"rowkey.col0":{"cf":"rowkey", "col":"col0", "type":"int"},
       |"cf1.col0":{"cf":"cf1", "col":"col0", "type":"int"},
       |"cf1.col1":{"cf":"cf1", "col":"col1", "type":"string"},
       |"cf1.col2":{"cf":"cf1", "col":"col2", "type":"int"}
       |}
       |}""".stripMargin

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS source")
    sql("DROP TABLE IF EXISTS sourceWithTimestamp")
    val data = (0 until 10).map { i =>
      IntKeyRecord(i)
    }
    htu = new HBaseTestingUtility()
    htu.startMiniCluster(1)
    SparkHBaseConf.conf = htu.getConfiguration
    import sqlContext.implicits._
    hBaseConfPath = s"$integrationPath/hbase/src/test/resources/hbase-site-local.xml"
    CarbonProperties.getInstance()
      .addProperty(HBaseConstants.CARBON_HBASE_CONF_FILE_PATH, hBaseConfPath)
    val shcExampleTableOption = Map(HBaseTableCatalog.tableCatalog -> writeCat,
      HBaseTableCatalog.newTable -> "5", HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath)

    val updateDf = sqlContext.sparkContext
      .parallelize(data)
      .toDF
      .select(col("col0").as("rowkey.col0"),
        col("col0").as("cf1.col0"),
        col("col1").as("cf1.col1"),
        col("col2").as("cf1.col2"))

    updateDf.write.options(shcExampleTableOption)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    sql("DROP TABLE IF EXISTS source")
    sql(
      "create table source(col0 int, col1 String, col2 int) stored as carbondata")
    var options = Map("format" -> "HBase")
    options = options + ("defaultColumnFamily" -> "cf1")
    options = options + ("rowKeyColumnFamily" -> "rowkey")
    options = options + ("MinMaxColumns" -> "col0")
    CarbonAddExternalStreamingSegmentCommand(Some("default"), "source", writeCat, None, options).processMetadata(
      sqlContext.sparkSession)

    val data1 = (0 until 10).map { i =>
      IntKeyRecord(i)
    }
    val shcExampleTable1Option = Map(HBaseTableCatalog.tableCatalog -> writeCatTimestamp,
      HBaseTableCatalog.newTable -> "5",
      HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath
    )
    val updateDf1 = sqlContext.sparkContext
      .parallelize(data1)
      .toDF
      .select(col("col0").as("rowkey.col0"),
        col("col0").as("cf1.col0"),
        col("col1").as("cf1.col1"),
        col("col2").as("cf1.col2"))

    updateDf1.write.options(shcExampleTable1Option)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    sql("DROP TABLE IF EXISTS sourceWithTimestamp")
    sql(
      "create table sourceWithTimestamp(col0 int, col1 String, col2 int) " +
      "stored as carbondata")
    var optionsNew = Map("format" -> "HBase")
    optionsNew = optionsNew + ("defaultColumnFamily" -> "cf1")
    optionsNew = optionsNew + ("rowKeyColumnFamily" -> "rowkey")
    optionsNew = options + ("MinMaxColumns" -> "col0")
    CarbonAddExternalStreamingSegmentCommand(Some("default"),
      "sourceWithTimestamp",writeCatTimestamp, None,
      optionsNew).processMetadata(
      sqlContext.sparkSession)
    CarbonAddExternalStreamingSegmentCommand(Some("default"),
      "sourceWithTimestamp",writeCatTimestamp, None,
      optionsNew).processMetadata(
      sqlContext.sparkSession)
  }

  test("test Full Scan Query") {
    val frame = withCatalog(writeCat).drop("rowkey.col0")
    checkAnswer(sql("select * from source"), frame)
  }

  test("test Filter Scan Query") {
    val frame = withCatalog(writeCat).drop("rowkey.col0")
    checkAnswer(sql("select * from source where col0=3"), frame.filter("`cf1.col0`=3"))
  }

  test("test handoff segment") {
    val prevRows1 = sql("select col1,col0 from sourceWithTimestamp where col1='String0 extra'").collect()
    assert(prevRows1.length == 1)
    val prevRows2 = sql("select * from sourceWithTimestamp where col1='String0 extra'").collect()
    assert(prevRows2.length == 1)
    val prevRows = sql("select * from sourceWithTimestamp").collect()
    HandoffHbaseSegmentCommand(None, "sourceWithTimestamp", Option.empty, new HandOffOptions().setDeleteRows(false).setGraceTimeInMillis(0)).run(sqlContext.sparkSession)
    checkAnswer(sql("select * from sourceWithTimestamp"), prevRows)
    val data = (10 until 20).map { i =>
      IntKeyRecord(i)
    }
    val shcExampleTable1Option = Map(HBaseTableCatalog.tableCatalog -> writeCatTimestamp,
      HBaseTableCatalog.newTable -> "5",
      HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath
    )
    import sqlContext.implicits._
    val updateDf = sqlContext
      .sparkContext
      .parallelize(data)
      .toDF
      .select(col("col0").as("rowkey.col0"),
        col("col0").as("cf1.col0"),
        col("col1").as("cf1.col1"),
        col("col2").as("cf1.col2"))

    updateDf.write.options(shcExampleTable1Option)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    val rows = sql("select * from sourceWithTimestamp").collectAsList()
    assert(rows.size() == 20)
    assert(sql("select * from sourceWithTimestamp where segmentid(1)").collectAsList().size() == 10)
    assert(sql("select * from sourceWithTimestamp where segmentid(2)").collectAsList().size() == 10)
    assert(sql("select * from sourceWithTimestamp where col0 = 5").collectAsList().size() == 1)
    assert(sql("select * from sourceWithTimestamp where col0 = 14").collectAsList().size() == 1)
    assert(sql("select * from sourceWithTimestamp where col0 > 1 and col0 < 7").collectAsList().size() == 5)
    assert(sql("select * from sourceWithTimestamp where col0 > 12 and col0 < 16").collectAsList().size() == 3)
  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS source")
    sql("DROP TABLE IF EXISTS sourceWithTimestamp")
    htu.shutdownMiniCluster()
  }
}