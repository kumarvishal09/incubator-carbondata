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

import com.google.gson.{JsonArray, JsonObject, JsonPrimitive}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.carbondata.mv.tool.manager.CommonSubexpressionManager
import org.apache.carbondata.mv.tool.preprocessor.QueryBatchPreprocessor
import org.apache.carbondata.mv.tool.vo.{ColStats, TableDetailsVo}

/**
 * Offline advisor
 * @param sparkSession spark session
 */
case class OfflineAdvisior(sparkSession: SparkSession) {
  def adviseMV(querySqls: Seq[String],
      tableDetails: Seq[TableDetailsVo],
      outputLoc: String): Unit = {
    createTablesAndCopyStats(tableDetails)
    val mvAdvisor = new MVAdvisor
    mvAdvisor.adviseMVs(sparkSession,
      DefaultQueryBatchPreprocessor,
      new DefaultSubExpressionManager(sparkSession,
        sparkSession.sessionState.conf,
        getTableProperty(tableDetails)),
      querySqls,
      outputLoc)
  }

  def cleanUp(tableDetails: Seq[TableDetailsVo]): Unit = {
    tableDetails.foreach { tblD =>
      sparkSession.sql("DROP TABLE IF EXISTS " + tblD.getTableName)
    }
  }
  /**
   * Below method will used to create the table and copy the stats
   * @param tableDetails table details
   */
  private def createTablesAndCopyStats(tableDetails: Seq[TableDetailsVo]): Unit = {
    tableDetails.foreach { tblDetail =>
      sparkSession.sql(tblDetail.getCreateTableStmt)
      val tblIdentifier = TableIdentifier(tblDetail.getTableName, Some("default"))
      var catalog = sparkSession.sessionState.catalog.getTableMetadata(tblIdentifier)
      val list = scala.collection.mutable.ListBuffer.empty[(String, ColumnStat)]
      catalog.schema.fields.foreach { cols =>
        val columnStat = tblDetail.getColumnStats.get(cols.name)
        if (null != columnStat) {
          list += getColumnStats(cols, columnStat)
        }
      }
      catalog = catalog.copy(stats = Some(
        CatalogStatistics(tblDetail.getTblStatsVo.getTableSize,
          Some(tblDetail.getTblStatsVo.getRowCount), list.toMap)))
      sparkSession.sessionState.catalog.alterTable(catalog)
    }
  }

  /**
   * Method to get columns stats for table
   * @param cols cols from catalog
   * @param userColSats stats provided by used
   * @return column name to column stats mapping
   */
  private def getColumnStats(cols: StructField, userColSats: ColStats): (String, ColumnStat) = {
    var maxValue: Option[Any] = None
    var minValue: Option[Any] = None
    cols.dataType match {
      case IntegerType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toInt)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue.toInt)
        }
      case LongType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toLong)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue)
        }
      case DoubleType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toDouble)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue.toDouble)
        }
      case FloatType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toFloat)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue.toFloat)
        }
      case ShortType =>
        if (null != userColSats.getMaxValue) {
          maxValue = Some(userColSats.getMaxValue.toShort)
        }
        if (null != userColSats.getMinValue) {
          minValue = Some(userColSats.getMinValue.toShort)
        }
      case _=>
    }
    (cols.name,
      new ColumnStat(
        userColSats.getDistinctCount,
        minValue,
        maxValue,
        userColSats.getNullCount,
        0l,
        userColSats.getMaxLength))
  }

  /**
   * Create a JSON string for fact and dimension table
   * @param tableDetails table details
   * @return json property string
   */
  private def getTableProperty(tableDetails: Seq[TableDetailsVo]): String = {
    val factArray = new JsonArray
    val dimensionArray = new JsonArray
    tableDetails.foreach { tblDetail =>
      if (tblDetail.isFactTable) {
        factArray.add(new JsonPrimitive( "default."+ tblDetail.getTableName))
      } else {
        dimensionArray.add(new JsonPrimitive("default." + tblDetail.getTableName))
      }
    }
    val jsonObject = new JsonObject
    jsonObject.add("fact", factArray)
    if(dimensionArray.size < 0) {
      jsonObject.add("dimension", dimensionArray)
    }
    jsonObject.toString
  }
}

/**
 * Default Implementation of sub expression manager
 * @param sparkSession spark session
 * @param sQLConf sql conf
 * @param tableProperty table property string
 */
private class DefaultSubExpressionManager(
    sparkSession: SparkSession,
    sQLConf: SQLConf,
    tableProperty: String) extends CommonSubexpressionManager(sparkSession,
  sQLConf.copy(SQLConf.CASE_SENSITIVE -> false,
    SQLConf.CBO_ENABLED -> true,
    SQLConf.buildConf("spark.mv.recommend.speedup.threshold").doubleConf.createWithDefault(0.5) ->
    0.5,
    SQLConf.buildConf("spark.mv.recommend.rowcount.threshold").doubleConf.createWithDefault(0.1) ->
    0.1,
    SQLConf.buildConf("spark.mv.recommend.frequency.threshold").doubleConf.createWithDefault(2) ->
    2,
    SQLConf.buildConf("spark.mv.tableCluster").stringConf.createWithDefault(s"""""") ->
    tableProperty)) {
}

/**
 * Default query batch processor
 */
object DefaultQueryBatchPreprocessor extends QueryBatchPreprocessor

