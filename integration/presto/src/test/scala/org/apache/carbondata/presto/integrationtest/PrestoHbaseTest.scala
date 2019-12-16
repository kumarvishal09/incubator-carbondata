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

package org.apache.carbondata.presto.integrationtest

import java.io.File
import java.sql.Timestamp
import java.util

import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.presto.server.PrestoServer


class PrestoHbaseTest extends FunSuiteLike with BeforeAndAfterAll {

  private val logger = LogServiceFactory
    .getLogService(classOf[PrestoHbaseTest].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"
  private val systemPath = s"$rootPath/integration/presto/target/system"
  private val prestoServer = new PrestoServer

  // Table schema:
  // +-------------+----------------+-------------+------------+
  // | Column name | Data type      | Column type | Dictionary |
  // +-------------+----------------+--------------+-----------+
  // | id          | string         | dimension   | yes        |
  // +-------------+----------------+-------------+------------+
  // | date        | date           | dimension   | yes        |
  // +-------------+----------------+-------------+------------+
  // | country     | string         | dimension   | yes        |
  // +-------------+----------------+-------------+-------------
  // | name        | string         | dimension   | yes        |
  // +-------------+----------------+-------------+-------------
  // | phonetype   | string         | dimension   | yes        |
  // +-------------+----------------+-------------+-------------
  // | serialname  | string         | dimension   | true       |
  // +-------------+----------------+-------------+-------------
  // | bonus       |short decimal   | measure     | false      |
  // +-------------+----------------+-------------+-------------
  // | monthlyBonus| longdecimal    | measure     | false      |
  // +-------------+----------------+-------------+-------------
  // | dob         | timestamp      | dimension   | true       |
  // +-------------+----------------+-------------+------------+
  // | shortField  | shortfield     | measure     | true       |
  // +-------------+----------------+-------------+-------------
  // |isCurrentEmp | boolean        | measure     | true       |
  // +-------------+----------------+-------------+------------+

  override def beforeAll: Unit = {
    import org.apache.carbondata.presto.util.CarbonDataStoreCreator
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION,
      systemPath)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
      "Presto")
    val map = new util.HashMap[String, String]()
    map.put("hive.metastore.uri", "thrift://localhost:9083")
    map.put("hbase.zookeeper.quorum", "localhost")
    map.put("hbase.zookeeper.property.clientPort", "2181")
//    map.put("hive.metastore.catalog.dir", s"file://$storePath")

    prestoServer.startServer("default", map)
//    prestoServer.execute("drop table if exists testdb.testtable")
//    prestoServer.execute("drop schema if exists testdb")
//    prestoServer.execute("create schema testdb")
//    prestoServer.execute("create table testdb.testtable(ID int, date date, country varchar, name varchar, phonetype varchar, serialname varchar,salary double, bonus decimal(10,4), monthlyBonus decimal(18,4), dob timestamp, shortField smallint, iscurrentemployee boolean) with(format='CARBON') ")
//    CarbonDataStoreCreator
//      .createCarbonStore(storePath,
//        s"$rootPath/integration/presto/src/test/resources/alldatatype.csv")
//    logger.info(s"\nCarbon store is created at location: $storePath")
//    cleanUp
  }

  override def afterAll(): Unit = {
    prestoServer.stopServer()
    CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath))
  }

  test("test the result for count(*) in presto") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from sourceWithTimestamp")
    System.out.println(actualResult)
  }

  test("test the result for count(*) in presto with key filter") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select col0, col1 from sourceWithTimestamp where col0 = 1")
    System.out.println(actualResult)
  }

  test("test the result for count(*) in presto with col filter") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select col0, col1 from sourceWithTimestamp where col1 = 'String1 extra' and col0 = 1")
    System.out.println(actualResult)
  }

  test("test the result for alldatatypes in presto ") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from alldatatype")
    System.out.println(actualResult)
  }

  test("test the result for alldatatypes in presto with point query") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from alldatatype where col0=10 and col1='String10 extra'")
    System.out.println(actualResult)
  }

  test("test the result for alldatatypes in presto with multiple point query in hbase") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from alldatatype where col0 in (11,12) and col1 in ('String11 extra', 'String12 extra')")
    System.out.println(actualResult)
  }

  test("test the result for alldatatypes in presto with multiple point query in carbon") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from alldatatype where col0 in (8,9) and col1 in ('String8 extra', 'String9 extra')")
    System.out.println(actualResult)
  }

  test("test the result for alldatatypes in presto with paritial point query") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from alldatatype where col0=8")
    System.out.println(actualResult)
  }
  test("test the result for alldatatypes in presto with one column selection") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select col1 from alldatatype")
    System.out.println(actualResult)
  }

  test("test the result for alldatatypes count(*) in presto ") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select count(*) from alldatatype")
    System.out.println(actualResult)
  }

  test("test the result for alldatatypes with lessthan query ") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from alldatatype where col6<800")
    System.out.println(actualResult)
  }

  test("test the result for scdhbaseCarbon with lessthan query ") {
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("select * from scdhbaseCarbon where id < 'id10'")
    System.out.println(actualResult)
  }
}