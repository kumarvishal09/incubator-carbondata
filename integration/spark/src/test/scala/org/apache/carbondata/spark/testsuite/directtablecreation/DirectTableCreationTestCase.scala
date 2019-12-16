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

package org.apache.carbondata.spark.testsuite.directtablecreation

import java.sql.Date

import scala.collection.JavaConverters._

import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.mutation.merge._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.CarbonSessionCatalogUtil
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for join query with orderby and limit
 */

class DirectTableCreationTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {

  }



  test("check the direct table creation ") {
    sql("DROP TABLE IF EXISTS source_native")
    CarbonSessionCatalogUtil.getClient(sqlContext.sparkSession).runSqlHive(
      s"""
         |CREATE EXTERNAL TABLE `source_native`(
         |  `empno` smallint COMMENT '',
         |  `empname` string COMMENT '',
         |  `designation` string COMMENT '',
         |  `doj` Timestamp COMMENT '',
         |  `workgroupcategory` string COMMENT '',
         |  `workgroupcategoryname` string COMMENT '')
         |partitioned by (`deptname` String COMMENT '')
         |ROW FORMAT SERDE
         |  'org.apache.carbondata.hive.CarbonHiveSerDe'
         |WITH SERDEPROPERTIES (
         |  'isTransactional'='true')
         |STORED AS INPUTFORMAT
         |  'org.apache.carbondata.hive.MapredCarbonInputFormat'
         |OUTPUTFORMAT
         |  'org.apache.carbondata.hive.MapredCarbonOutputFormat'
         |""".stripMargin)

    sql("DROP TABLE IF EXISTS source")
    //
    // Create table
    sql(
      s"""
         | CREATE TABLE source(
         | empno SHORT,
         | empname string,
         | designation string,
         | doj Timestamp,
         | workgroupcategory STRING,
         | workgroupcategoryname string,
         | deptname string
         | )
         | stored as carbondata
       """.stripMargin)

    val path = s"$resourcesPath/data.csv"
    //
    //    // scalastyle:off
    sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE source
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)

    sql("insert overwrite table source_native partition(deptname) select * from source")
    checkAnswer(sql("select * from source"),  sql("select * from source_native"))
  }

  override def afterAll {
    sql("drop table if exists order")
  }
}
