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
package org.apache.spark.sql.execution.strategy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.carbondata.execution.datasources.SparkCarbonFileFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{FileFormat, LogicalRelation}
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PrunedSegmentInfo
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.readcommitter.ReadCommittedScope
import org.apache.carbondata.core.statusmanager.{FileFormat => FileFormatName, LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonSessionInfo, SessionParams, ThreadLocalSessionInfo}

object MixedFormatHandler {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  val supportedFormats: Seq[String] =
    Seq("carbon", "carbondata", "parquet", "orc", "json", "csv", "text")

  def validateFormat(format: String): Boolean = {
    supportedFormats.exists(_.equalsIgnoreCase(format))
  }

  /**
   * collect schema, list of last level directory and list of all data files under given path
   *
   * @param sparkSession spark session
   * @param options option for ADD SEGMENT
   * @param inputPath under which path to collect
   * @return schema of the data file, map of last level directory (partition folder) to its
   *         children file list (data files)
   */
  def collectInfo(
      sparkSession: SparkSession,
      options: Map[String, String],
      inputPath: String): (StructType, mutable.Map[String, Seq[FileStatus]]) = {
    val path = new Path(inputPath)
    val fs = path.getFileSystem(SparkSQLUtil.sessionState(sparkSession).newHadoopConf())
    val rootPath = fs.getFileStatus(path)
    val leafDirFileMap = collectAllLeafFileStatus(sparkSession, rootPath, fs)
    val format = options.getOrElse("format", "carbondata").toLowerCase
    val fileFormat = if (format.equalsIgnoreCase("carbondata") ||
                         format.equalsIgnoreCase("carbon")) {
      new SparkCarbonFileFormat()
    } else {
      getFileFormat(new FileFormatName(format))
    }
    if (leafDirFileMap.isEmpty) {
      throw new RuntimeException("no partition data is found")
    }
    val schema = fileFormat.inferSchema(sparkSession, options, leafDirFileMap.head._2).get
    (schema, leafDirFileMap)
  }

  /**
   * collect leaf directories and leaf files recursively in given path
   *
   * @param sparkSession spark session
   * @param path path to collect
   * @param fs hadoop file system
   * @return mapping of leaf directory to its children files
   */
  private def collectAllLeafFileStatus(
      sparkSession: SparkSession,
      path: FileStatus,
      fs: FileSystem): mutable.Map[String, Seq[FileStatus]] = {
    val directories: ArrayBuffer[FileStatus] = ArrayBuffer()
    val leafFiles: ArrayBuffer[FileStatus] = ArrayBuffer()
    val lastLevelFileMap = mutable.Map[String, Seq[FileStatus]]()

    // get all files under input path
    val fileStatus = fs.listStatus(path.getPath, new PathFilter {
      override def accept(path: Path): Boolean = {
        !path.getName.equals("_SUCCESS") && !path.getName.endsWith(".crc")
      }
    })
    // collect directories and files
    fileStatus.foreach { file =>
      if (file.isDirectory) directories.append(file)
      else leafFiles.append(file)
    }
    if (leafFiles.nonEmpty) {
      // leaf file is found, so parent folder (input parameter) is the last level dir
      val updatedPath = FileFactory.getUpdatedFilePath(path.getPath.toString)
      lastLevelFileMap.put(updatedPath, leafFiles)
      lastLevelFileMap
    } else {
      // no leaf file is found, for each directory, collect recursively
      directories.foreach { dir =>
        val map = collectAllLeafFileStatus(sparkSession, dir, fs)
        lastLevelFileMap ++= map
      }
      lastLevelFileMap
    }
  }

  def extraSegments(identifier: AbsoluteTableIdentifier,
      readCommittedScope: ReadCommittedScope): Array[LoadMetadataDetails] = {
    val loadMetadataDetails = readCommittedScope.getSegmentList
    val segsToAccess = getSegmentsToAccess(identifier)
    loadMetadataDetails.filter { metaDetail =>
      metaDetail.getSegmentStatus.equals(SegmentStatus.SUCCESS) ||
      metaDetail.getSegmentStatus.equals(SegmentStatus.LOAD_PARTIAL_SUCCESS)
    }.filterNot { currLoad =>
      currLoad.getFileFormat.equals(FileFormatName.COLUMNAR_V3) ||
      currLoad.getFileFormat.equals(FileFormatName.ROW_V1)
    }.filter {
      l => segsToAccess.isEmpty || segsToAccess.contains(l.getLoadName)
    }
  }

  /**
   * Generates the RDD for non carbon segments. It uses the spark underlying file formats and
   * generates the RDD in its native format without changing any of its flow to keep the original
   * performance and features.
   *
   * If multiple segments are with different formats like parquet , orc etc then it creates RDD for
   * each format segments and union them.
   */
  def extraRDD(plan: LogicalPlan,
      l: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      prunedSegmentInfo: Map[FileFormatName, List[PrunedSegmentInfo]],
      supportBatch: Boolean = true): Option[(RDD[InternalRow], Boolean)] = {
    val rdds = prunedSegmentInfo.map { case (format, prunedSegmentInfo) =>
        MixedFormatHandlerFactory.createFormatBasedHandler(format)
          .getRDDForExternalSegments(plan,
            format,
            prunedSegmentInfo,
            l,
            projects,
            filters,
            supportBatch)
      }
    if (rdds.nonEmpty) {
      if (rdds.size == 1) {
        Some(rdds.head)
      } else {
        if (supportBatch && rdds.exists(!_._2)) {
          extraRDD(plan, l, projects, filters, prunedSegmentInfo, false)
        } else {
          var rdd: RDD[InternalRow] = null
          rdds.foreach { r =>
            if (rdd == null) {
              rdd = r._1
            } else {
              rdd = rdd.union(r._1)
            }
          }
          Some(rdd, rdds.forall(_._2))
        }
      }
    } else {
      None
    }
  }

  def getFileFormat(fileFormat: FileFormatName, supportBatch: Boolean = true): FileFormat = {
    if (fileFormat.equals(new FileFormatName("parquet"))) {
      new ExtendedParquetFileFormat(supportBatch)
    } else if (fileFormat.equals(new FileFormatName("orc"))) {
      new ExtendedOrcFileFormat(supportBatch)
    } else if (fileFormat.equals(new FileFormatName("json"))) {
      new JsonFileFormat
    } else if (fileFormat.equals(new FileFormatName("csv"))) {
      new CSVFileFormat
    } else if (fileFormat.equals(new FileFormatName("text"))) {
      new TextFileFormat
    } else {
      throw new UnsupportedOperationException("Format not supported " + fileFormat)
    }
  }

  class ExtendedParquetFileFormat(supportBatch: Boolean) extends ParquetFileFormat {
    override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
      super.supportBatch(sparkSession, schema) && supportBatch
    }
  }

  class ExtendedOrcFileFormat(supportBatch: Boolean) extends OrcFileFormat {
    override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
      super.supportBatch(sparkSession, schema) && supportBatch
    }
  }

  def getSegmentsToAccess(identifier: AbsoluteTableIdentifier): Seq[String] = {
    val carbonSessionInfo: CarbonSessionInfo = {
      var info = ThreadLocalSessionInfo.getCarbonSessionInfo
      if (info == null || info.getSessionParams == null) {
        info = new CarbonSessionInfo
        info.setSessionParams(new SessionParams())
      }
      info.getSessionParams.addProps(CarbonProperties.getInstance().getAddedProperty)
      info
    }
    val tableUniqueKey = identifier.getDatabaseName + "." + identifier.getTableName
    val inputSegmentsKey = CarbonCommonConstants.CARBON_INPUT_SEGMENTS + tableUniqueKey
    val segmentsStr = carbonSessionInfo.getThreadParams
      .getProperty(inputSegmentsKey, carbonSessionInfo.getSessionParams
        .getProperty(inputSegmentsKey,
          CarbonProperties.getInstance().getProperty(inputSegmentsKey, "*")))
    if (!segmentsStr.equals("*")) {
      segmentsStr.split(",")
    } else {
      Seq.empty
    }
  }

  /**
   * Returns true if any other non-carbon format segment exists
   */
  def otherFormatSegmentsExist(metadataPath: String): Boolean = {
    val allSegments = SegmentStatusManager.readLoadMetadata(metadataPath)
    allSegments.exists(a => a.getFileFormat != null && !a.isCarbonFormat)
  }



  object MixedFormatHandlerFactory {
    def createFormatBasedHandler(format: FileFormatName): ExternalFormatHandler = {
      format.toString match {
        case "hbase" =>
          CarbonReflectionUtils
            .createObject("org.apache.spark.sql.execution.strategy.HBaseFormatBasedHandler")._1
            .asInstanceOf[ExternalFormatHandler]
        case _ =>
          new GenericFormatHandler()
      }
    }
  }
}