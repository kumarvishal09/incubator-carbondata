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

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, FileFormat => FileFormatName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSqlAdapter
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, ExpressionSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FilterExec, ProjectExec}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.strategy.MixedFormatHandler.getFileFormat
import org.apache.spark.sql.types.StructType

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.PrunedSegmentInfo


class GenericFormatHandler extends ExternalFormatHandler {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  /**
   * Generates the RDD using the spark fileformat.
   */
  override def getRDDForExternalSegments(plan: LogicalPlan,
      format: FileFormatName,
      prunedSegmentInfo: List[PrunedSegmentInfo],
      l: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      supportBatch: Boolean = true): (RDD[InternalRow], Boolean) = {
    val paths = prunedSegmentInfo. flatMap { d =>
      val segmentFile = d.getSegmentFile

      // If it is a partition table, the path to create RDD should be the root path of the
      // partition folder (excluding the partition subfolder).
      // If it is not a partition folder, collect all data file paths
      if (segmentFile.getOptions.containsKey("partition")) {
        val segmentPath = segmentFile.getOptions.get("path")
        if (segmentPath == null) {
          throw new RuntimeException("invalid segment file, 'path' option not found")
        }
        Seq(new Path(segmentPath))
      } else {
        // If it is not a partition folder, collect all data file paths to create RDD
        segmentFile.getLocationMap.asScala.flatMap { case (p, f) =>
          f.getFiles.asScala.map { ef =>
            new Path(p + CarbonCommonConstants.FILE_SEPARATOR + ef)
          }.toSeq
        }.toSeq
      }
    }
    val fileFormat = getFileFormat(format, supportBatch)
    getRDD(l, projects, filters, paths, fileFormat);
  }

  /**
   * Generates the RDD using the spark fileformat.
   */
  private def getRDD(l: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      paths: Seq[Path],
      fileFormat: FileFormat): (RDD[InternalRow], Boolean) = {
    val sparkSession = l.relation.sqlContext.sparkSession
    val fsRelation = l.catalogTable match {
      case Some(catalogTable) =>
        val fileIndex =
          new InMemoryFileIndex(sparkSession, paths, catalogTable.storage.properties, None)
        // exclude the partition in data schema
        val dataSchema = catalogTable.schema.filterNot { column =>
          catalogTable.partitionColumnNames.contains(column.name)
        }
        HadoopFsRelation(
          fileIndex,
          catalogTable.partitionSchema,
          new StructType(dataSchema.toArray),
          catalogTable.bucketSpec,
          fileFormat,
          catalogTable.storage.properties)(sparkSession)
      case _ =>
        HadoopFsRelation(
          new InMemoryFileIndex(sparkSession, Seq.empty, Map.empty, None),
          new StructType(),
          l.relation.schema,
          None,
          fileFormat,
          null)(sparkSession)
    }

    // Filters on this relation fall into four categories based on where we can use them to avoid
    // reading unneeded data:
    //  - partition keys only - used to prune directories to read
    //  - bucket keys only - optionally used to prune files to read
    //  - keys stored in the data only - optionally used to skip groups of data in files
    //  - filters that need to be evaluated again after the scan
    val filterSet = ExpressionSet(filters)

    // The attribute name of predicate could be different than the one in schema in case of
    // case insensitive, we should change them to match the one in schema, so we do not need to
    // worry about case sensitivity anymore.
    val normalizedFilters = filters.map { e =>
      e transform {
        case a: AttributeReference =>
          a.withName(l.output.find(_.semanticEquals(a)).get.name)
      }
    }

    val partitionColumns =
      l.resolve(
        fsRelation.partitionSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)
    val partitionSet = AttributeSet(partitionColumns)
    val partitionKeyFilters =
      ExpressionSet(normalizedFilters
        .filter(_.references.subsetOf(partitionSet)))

    LOGGER.info(s"Pruning directories with: ${ partitionKeyFilters.mkString(",") }")

    val dataColumns =
      l.resolve(fsRelation.dataSchema, fsRelation.sparkSession.sessionState.analyzer.resolver)

    // Partition keys are not available in the statistics of the files.
    val dataFilters = normalizedFilters.filter(_.references.intersect(partitionSet).isEmpty)

    // Predicates with both partition keys and attributes need to be evaluated after the scan.
    val afterScanFilters = filterSet -- partitionKeyFilters.filter(_.references.nonEmpty)
    LOGGER.info(s"Post-Scan Filters: ${ afterScanFilters.mkString(",") }")
    val filterAttributes = AttributeSet(afterScanFilters)
    val requiredExpressions = new util.LinkedHashSet[NamedExpression](
      (projects.flatMap(p => findAttribute(dataColumns, p)) ++
       filterAttributes.map(p => dataColumns.find(_.exprId.equals(p.exprId)).get)).asJava
    ).asScala.toSeq
    val readDataColumns =
      requiredExpressions.filterNot(partitionColumns.contains).asInstanceOf[Seq[Attribute]]
    val outputSchema = readDataColumns.toStructType
    LOGGER.info(s"Output Data Schema: ${ outputSchema.simpleString(5) }")

    val outputAttributes = readDataColumns ++ partitionColumns

    val scan =
      SparkSqlAdapter.getScanForSegments(
        fsRelation,
        outputAttributes,
        outputSchema,
        partitionKeyFilters.toSeq,
        dataFilters,
        l.catalogTable.map(_.identifier))
    val afterScanFilter = afterScanFilters.toSeq.reduceOption(expressions.And)
    val withFilter = afterScanFilter.map(FilterExec(_, scan)).getOrElse(scan)
    val withProjections = if (projects == withFilter.output) {
      withFilter
    } else {
      ProjectExec(projects, withFilter)
    }
    (withProjections.inputRDDs().head, fileFormat.supportBatch(sparkSession, outputSchema))
  }

  // This function is used to get the unique columns based on expression Id from
  // filters and the projections list
  def findAttribute(dataColumns: Seq[Attribute], p: Expression): Seq[Attribute] = {
    dataColumns.find {
      x =>
        val attr = findAttributeReference(p)
        attr.isDefined && x.exprId.equals(attr.get.exprId)
    } match {
      case Some(c) => Seq(c)
      case None => Seq()
    }
  }

  private def findAttributeReference(p: Expression): Option[NamedExpression] = {
    p match {
      case a: AttributeReference =>
        Some(a)
      case al =>
        if (al.children.nonEmpty) {
          al.children.map(findAttributeReference).head
        } else {
          None
        }
      case _ => None
    }
  }

}
