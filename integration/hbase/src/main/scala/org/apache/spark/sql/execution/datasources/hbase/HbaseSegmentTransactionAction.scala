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

import java.io.File
import java.util

import org.apache.hadoop.fs.FileStatus
import scala.collection.JavaConverters._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.Segment
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.transaction.TransactionAction
import org.apache.carbondata.core.util.{CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hbase.HBaseUtil
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

class HbaseSegmentTransactionAction(carbonTable: CarbonTable,
    loadingSegment: String,
    prevSegment: String,
    maxTimeStamp: Long) extends TransactionAction {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def commit(): Unit = {
    val model = new CarbonLoadModel
    model.setCarbonTransactionalTable(true)
    model.setCarbonDataLoadSchema(new CarbonDataLoadSchema(carbonTable))
    model.setDatabaseName(carbonTable.getDatabaseName)
    model.setTableName(carbonTable.getTableName)

    val existingLoadMetadataDetails = SegmentStatusManager.readLoadMetadata(CarbonTablePath
      .getMetadataPath(
        carbonTable.getTablePath))
    val hbaseDetail = existingLoadMetadataDetails.find(_.getLoadName.equals(loadingSegment))
    if (hbaseDetail.isEmpty) {
      throw new MalformedCarbonCommandException(s"Segmentid $loadingSegment is not found")
    }

    val externalSchema = CarbonUtil.getExternalSchema(carbonTable.getAbsoluteTableIdentifier)
    val minMaxColumnIdString = externalSchema.getParam("MinMaxColumns")
    val minMaxColumns = if (null != minMaxColumnIdString) {
      minMaxColumnIdString.toLowerCase.split(",").toSet
    } else {
      "".split(",").toSet
    }

    val currentLoadedLoadMetadata = existingLoadMetadataDetails.filter(detail => detail.getLoadName
      .equalsIgnoreCase(loadingSegment))
    val currentLoadedMetadataInfo = SegmentFileStore.readSegmentFile(
      CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath) + "/" +
      currentLoadedLoadMetadata(0).getSegmentFile).getSegmentMetaDataInfo

    model.setLoadMetadataDetails(existingLoadMetadataDetails.toList.asJava)
    val newLoadMetaEntry = new LoadMetadataDetails
    model.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime)
    CarbonLoaderUtil.populateNewLoadMetaEntry(newLoadMetaEntry,
      SegmentStatus.INSERT_IN_PROGRESS,
      model.getFactTimeStamp,
      false)
    newLoadMetaEntry.setFileFormat(new FileFormat("hbase"))
    CarbonLoaderUtil.recordNewLoadMetadata(newLoadMetaEntry, model, true, false)

    val minMaxColumnId = carbonTable
      .getTableInfo
      .getFactTable
      .getListOfColumns
      .asScala
      .filter(col => minMaxColumns.contains(col.getColumnName.toLowerCase))
      .map(col => col.getColumnUniqueId.toLowerCase).toSet.asJava


    val segmentMetaDataInto = HBaseUtil.getHBaseSegmentMetadataInfo(minMaxColumnId,
      currentLoadedMetadataInfo, maxTimeStamp)
    val segment = new Segment(
      model.getSegmentId,
      SegmentFileStore.genSegmentFileName(
        model.getSegmentId,
        System.nanoTime().toString) + CarbonTablePath.SEGMENT_EXT,
      "",
      new util.HashMap[String, String]())

    val writeSegment =
      SegmentFileStore.writeSegmentFileForExternalStreaming(
        carbonTable,
        segment,
        null,
        new util.ArrayList[FileStatus](),
        segmentMetaDataInto,
        false)
    val hbaseCurrentSuccessSegment = existingLoadMetadataDetails
      .filter(det => det.getFileFormat.toString.equals("hbase") &&
                     det.getSegmentStatus == SegmentStatus.SUCCESS).head
    hbaseCurrentSuccessSegment.setSegmentStatus(SegmentStatus.COMPACTED)
    hbaseCurrentSuccessSegment.setModificationOrDeletionTimestamp(model.getFactTimeStamp)
    hbaseCurrentSuccessSegment.setMergedLoadName(newLoadMetaEntry.getLoadName)

    val success = if (writeSegment) {
      newLoadMetaEntry.setSegmentFile(segment.getSegmentFileName)
      newLoadMetaEntry.setDataSize("0")
      newLoadMetaEntry.setIndexSize("0")
      newLoadMetaEntry.setSegmentStatus(SegmentStatus.SUCCESS)
      val allLoadMetadata = existingLoadMetadataDetails :+ newLoadMetaEntry
      SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(
        carbonTable.getAbsoluteTableIdentifier.getTablePath),
        allLoadMetadata)
      LOGGER.info(s"Added new Hbase segment for table ${
        carbonTable
          .getTableUniqueName
      }: segmenetid : ${ segment.getSegmentNo }")
      true
    } else {
      false
    }

    if (!success) {
      CarbonLoaderUtil.updateTableStatusForFailure(model, "uniqueTableStatusId")
      LOGGER.info("********starting clean up**********")
      // delete segment is applicable for transactional table
      CarbonLoaderUtil.deleteSegmentForFailure(model, model.getSegmentId.toInt)
      // delete corresponding segment file from metadata
      val segmentFile = CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath) +
                        File.separator + segment.getSegmentFileName
      FileFactory.deleteFile(segmentFile)
      LOGGER.info("********clean up done**********")
      LOGGER.error("Data load failed due to failure in table status updation.")
      throw new Exception("Data load failed due to failure in table status updation.")
    }
  }

}
