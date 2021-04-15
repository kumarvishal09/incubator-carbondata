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

package org.apache.spark.sql.execution.datasources.hbase

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.{Checker, DataCommand, ExecutionErrors, UpdateTableModel}
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoWithDf
import org.apache.spark.sql.execution.command.mutation.{DeleteExecution, HorizontalCompaction}
import org.apache.spark.sql.execution.command.mutation.merge.{CarbonMergeDataSetException, MutationActionFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.extrenalschema.ExternalSchema
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.transaction.{TransactionActionType, TransactionManager}
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hbase.HBaseConstants._
import org.apache.carbondata.hbase.HBaseUtil._
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.tranaction.SessionTransactionManager

case class HandoffHbaseSegmentCommand(
    databaseNameOp: Option[String],
    tableName: String,
    joinColumns: Option[Array[String]],
    handOffOptions: HandOffOptions)
  extends DataCommand {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val rltn = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(databaseNameOp, tableName)(sparkSession)
      .asInstanceOf[CarbonRelation]
    val carbonTable = rltn.carbonTable
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    LOGGER.info(s"Handoff Started for table: ${carbonTable.getTableUniqueName}")
    // if insert overwrite in progress, do not allow add segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "handoff segment")
    }
    val details =
      SegmentStatusManager.readLoadMetadata(
        CarbonTablePath.getMetadataPath(carbonTable.getTablePath))
    val detail = details.filter(_.getSegmentStatus.equals(SegmentStatus.SUCCESS)).
      find(_.getFileFormat.getFormat.equalsIgnoreCase(CARBON_HBASE_FORMAT_NAME))
      .getOrElse(throw new AnalysisException(s"Segment with format hbase doesn't exist"))
    val transactionManager = TransactionManager
      .getInstance()
      .getTransactionManager
      .asInstanceOf[SessionTransactionManager]
    val fullTableName = String.join(".", carbonTable.getDatabaseName, carbonTable.getTableName)
    var transactionId = transactionManager.getTransactionId(sparkSession,
      fullTableName)
    var isTransactionStarted = false
    if (transactionId == null) {
      isTransactionStarted = true
      transactionId = transactionManager.startTransaction(sparkSession, fullTableName)
    }
    val externalSchema = CarbonUtil.getExternalSchema(carbonTable.getAbsoluteTableIdentifier)
    val tableCols =
      carbonTable.getTableInfo
        .getFactTable
        .getListOfColumns
        .asScala
        .sortBy(_.getSchemaOrdinal)
        .map(_.getColumnName)
        .filterNot(_.equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
    val header = tableCols.mkString(",")
    val store = new SegmentFileStore(carbonTable.getTablePath, detail.getSegmentFile)
    val columnMetaDataInfo = store.getSegmentFile
      .getSegmentMetaDataInfo
      .getSegmentColumnMetaDataInfoMap
      .get(CARBON_HABSE_ROW_TIMESTAMP_COLUMN)
    val minTimestamp = if (columnMetaDataInfo != null) {
      ByteUtil.toLong(columnMetaDataInfo.getColumnMinValue, 0, ByteUtil.SIZEOF_LONG) + 1
    } else {
      0L
    }
    val hbaseConfFilePath = CarbonProperties.getInstance()
      .getProperty(CARBON_HBASE_CONF_FILE_PATH)

    val hBaseRelation = new CarbonHBaseRelation(Map(
      HBaseTableCatalog.tableCatalog -> externalSchema.getHandOffSchema,
      HBaseRelation.MIN_STAMP -> minTimestamp.toString,
      HBaseRelation.MAX_STAMP -> Long.MaxValue.toString,
      HBaseRelation.HBASE_CONFIGFILE -> hbaseConfFilePath), Option.empty)(sparkSession.sqlContext)
    val rdd = hBaseRelation.buildScan(hBaseRelation.schema.map(f => f.name).toArray, Array.empty)
    val loadDF = sparkSession.sqlContext.createDataFrame(rdd, hBaseRelation.schema).cache
    val tempView = UUID.randomUUID().toString.replace("-", "")
    loadDF.createOrReplaceTempView(tempView)
    val rows = sparkSession.sql(s"select max($CARBON_HABSE_ROW_TIMESTAMP_COLUMN) from $tempView")
      .collect()
    if (rows.isEmpty || rows.head.isNullAt(0)) {
      transactionManager.rollbackTransaction(transactionId);
      return Seq.empty
    }
    val maxTimeStamp = rows.head.getLong(0) - handOffOptions.getGraceTimeInMillis
    val updated = loadDF.where(col(CARBON_HABSE_ROW_TIMESTAMP_COLUMN).leq(lit(maxTimeStamp)))
    val setValue = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_MERGE_WITHIN_SEGMENT,
        CarbonCommonConstants.CARBON_MERGE_WITHIN_SEGMENT_DEFAULT)
    val defaultColumnFamily = externalSchema.getParam(CABON_HBASE_DEFAULT_COLUMN_FAMILY)
    val colWithCf = tableCols.map(columnName => updateColumnFamily(defaultColumnFamily, columnName))
    val columnsToSelect = colWithCf.zip(tableCols).map(f => { col(f._1).as(f._2) })
    try {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MERGE_WITHIN_SEGMENT, "false")
      if (joinColumns.isDefined) {
        insertAndUpdateData(sparkSession,
          carbonTable,
          columnsToSelect,
          header,
          updated,
          detail,
          transactionManager,
          transactionId,
          externalSchema)
      } else {
        insertData(sparkSession, carbonTable, columnsToSelect, header, updated)
      }
      val segmentId = transactionManager
        .getAndSetCurrentTransactionSegment(transactionId,
          carbonTable.getDatabaseName + DOT + carbonTable.getTableName)
      transactionManager.recordTransactionAction(transactionId,
        new HbaseSegmentTransactionAction(carbonTable, segmentId, detail.getLoadName, maxTimeStamp),
        TransactionActionType.COMMIT_SCOPE)
      if (isTransactionStarted) {
        transactionManager.commitTransaction(transactionId)
      }
    } finally {
      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MERGE_WITHIN_SEGMENT, setValue)
    }

    // delete rows
    if (handOffOptions.getDeleteRows) {
      val hBaseRelationForDelete =
        new CarbonHBaseRelation(Map(
          HBaseTableCatalog.tableCatalog -> externalSchema.getHandOffSchema,
          HBaseRelation.HBASE_CONFIGFILE -> hbaseConfFilePath,
          "deleterows" -> "true"), Option.empty)(sparkSession.sqlContext)
      hBaseRelationForDelete.insert(updated.select(hBaseRelationForDelete.schema
        .map("`" + _.name + "`")
        .map(col): _*), false)
    }
    LOGGER.info(s"Handoff finished for table: ${carbonTable.getTableUniqueName}")
    Seq.empty
  }

  private def insertData(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      tableCols: Seq[Column],
      header: String,
      updated: Dataset[Row]) = {
    CarbonInsertIntoWithDf(
      databaseNameOp = Some(carbonTable.getDatabaseName),
      tableName = carbonTable.getTableName,
      options = Map(("fileheader" -> header)),
      isOverwriteTable = false,
      dataFrame = updated.select(tableCols: _*),
      updateModel = None,
      tableInfoOp = Some(carbonTable.getTableInfo)).process(sparkSession)
  }

  private def insertAndUpdateData(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      tableCols: Seq[Column],
      header: String,
      updated: Dataset[Row],
      loadMetadataDetails: LoadMetadataDetails,
      transactionManager: SessionTransactionManager,
      transactionId: String,
      externalSchema: ExternalSchema) = {
    val operationTypeColumn = updateColumnFamily(externalSchema.getParam(
      CABON_HBASE_DEFAULT_COLUMN_FAMILY),
      externalSchema.getParam(CARBON_HBASE_OPERATION_TYPE_COLUMN))
    val iDf = updated.where(col(operationTypeColumn).equalTo(lit(externalSchema.getParam(
      CARBON_HBASE_INSERT_OPERATION_VALUE)))).select(tableCols: _*)
    var uDf = updated.where(col(operationTypeColumn).equalTo(lit(externalSchema.getParam(
      CARBON_HBASE_UPDATE_OPERATION_VALUE)))).select(tableCols: _*)
    val dDf = updated.where(col(operationTypeColumn).equalTo(lit(externalSchema.getParam(
      CARBON_HBASE_DELETE_OPERATION_VALUE)))).select(tableCols: _*)
    val tableDF =
      sparkSession.sql(s"SELECT * FROM ${ carbonTable.getDatabaseName }.${
        carbonTable
          .getTableName
      } WHERE excludesegmentId(${ loadMetadataDetails.getLoadName })")

    val uCount = uDf.count()
    var uWithTuples: Dataset[Row] = sparkSession.emptyDataFrame
    if (uCount > 0) {
      LOGGER.info(s"Number of update count ${uCount} for table ${carbonTable.getTableUniqueName}")
      uDf = uDf.union(dDf)
      // TODO make it configurable
      if (uCount < handOffOptions.getFilterJoinPushLimit) {
        val rows = uDf.select(joinColumns.get.map(col): _*).collect()
        val filter = joinColumns.get.zipWithIndex.map { j =>
          col(j._1).isInCollection(rows.map(r => lit(r.get(j._2))))
        }.reduce[Column]((l, r) => l.and(r))
        uWithTuples = tableDF.withColumn(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
          expr("getTupleId()"))
          .filter(filter).select(col(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
      } else {
        uWithTuples = tableDF.withColumn(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
          expr("getTupleId()")).join(uDf.select(joinColumns.get.map(col): _*),
          joinColumns.get
            .map(c => tableDF.col(c).equalTo(uDf.col(c)))
            .reduce[Column]((l, r) => l.and(r)))
          .select(col(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
      }
      uWithTuples.cache()
    }
    if (uCount <= 0) {
      val dCount = dDf.count()
      if (dCount > 0) {
        LOGGER.info(s"Number of deleted count ${ dCount } for table ${
          carbonTable
            .getTableUniqueName
        }")
        val rows = dDf.select(joinColumns.get.map(col): _*).collect()
        val filter = joinColumns.get.zipWithIndex.map { j =>
          col(j._1).isInCollection(rows.map(r => lit(r.get(j._2))))
        }.reduce[Column]((l, r) => l.and(r))

        val dWithTuples = tableDF.withColumn(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID,
          expr("getTupleId()"))
          .filter(filter).select(col(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
        uWithTuples = uWithTuples.union(dWithTuples)
      }
    }

    val executorErrors = ExecutionErrors(FailureCauses.NONE, "")
    val timestamp = System.currentTimeMillis
    val tuple1 = DeleteExecution.deleteDeltaExecutionInternal(Some(carbonTable.getDatabaseName),
      carbonTable.getTableName,
      sparkSession, uWithTuples.rdd,
      timestamp.toString,
      true, executorErrors, Some(0))
    val tuple = DeleteExecution.processSegments(executorErrors, tuple1._1, carbonTable,
      timestamp.toString, tuple1._2)
    MutationActionFactory.checkErrors(executorErrors)

    CarbonInsertIntoWithDf(
      databaseNameOp = Some(carbonTable.getDatabaseName),
      tableName = carbonTable.getTableName,
      options = Map(("fileheader" -> header)),
      isOverwriteTable = false,
      dataFrame = iDf.union(uDf),
      updateModel = Some(new UpdateTableModel(true, timestamp,
        executorErrors, Array.empty[Segment], Option.empty, true)),
      tableInfoOp = Some(carbonTable.getTableInfo)).process(sparkSession)
    if (!CarbonUpdateUtil.updateSegmentStatus(tuple._1.asScala.asJava,
      carbonTable,
      timestamp.toString, false, false)) {
      LOGGER.error("writing of update status file failed")
      throw new CarbonMergeDataSetException("writing of update status file failed")
    }
    transactionManager.recordUpdateDetails(transactionId,
      carbonTable.getDatabaseName + DOT + carbonTable.getTableName,
      timestamp,
      tuple._2.toArray,
      true)
  }

  override protected def opName: String = {
    "ALTER SEGMENT ON TABLE tableName HANDOFF STREAMING SEGMENT "
  }
}