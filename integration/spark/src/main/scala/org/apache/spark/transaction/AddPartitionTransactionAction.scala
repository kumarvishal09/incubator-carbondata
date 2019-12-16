package org.apache.carbondata.transaction

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableDropPartitionCommand, CommandUtils}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitioningUtils}
import org.apache.spark.sql.{CarbonEnv, SaveMode, SparkSession}

import org.apache.carbondata.core.transaction.TransactionAction
import org.apache.carbondata.spark.util.CarbonScalaUtil

class AddPartitionTransactionAction(sparkSession: SparkSession,
    catalogTable: Option[CatalogTable],
    mode: SaveMode,
    dynamicPartitionOverwrite: Boolean,
    partitionsTrackedByCatalog: Boolean,
    initialMatchingPartitions: Seq[TablePartitionSpec],
    updatedPartitionPaths: Set[String],
    partitionColumns: Seq[Attribute],
    fileIndex: Option[FileIndex],
    outputPath: Path) extends TransactionAction {
  override def commit(): Unit = {
    def refreshUpdatedPartitions(updatedPartitionPaths: Set[String]): Unit = {
      val updatedPartitions = updatedPartitionPaths.map(PartitioningUtils.parsePathFragment)
      if (partitionsTrackedByCatalog) {
        val newPartitions = updatedPartitions -- initialMatchingPartitions
        if (newPartitions.nonEmpty) {
          AlterTableAddPartitionCommand(
            catalogTable.get.identifier, newPartitions.toSeq.map(p => (p, None)),
            ifNotExists = true).run(sparkSession)
        }
        // For dynamic partition overwrite, we never remove partitions but only
        // update existing ones.
        if (mode == SaveMode.Overwrite && !dynamicPartitionOverwrite) {
          val deletedPartitions = initialMatchingPartitions.toSet -- updatedPartitions
          if (deletedPartitions.nonEmpty) {
            AlterTableDropPartitionCommand(
              catalogTable.get.identifier, deletedPartitions.toSeq,
              ifExists = true, purge = false,
              retainData = true /* already deleted */).run(sparkSession)
          }
        }
      }
    }

    val mappedParts = new mutable.LinkedHashMap[String, String]

    val update = updatedPartitionPaths.map {
      eachPath =>
        mappedParts.clear()
        val partitionFolders = eachPath.split("/")
        partitionFolders.map {
          folder =>
            val part = folder.split("=")
            mappedParts.put(part(0), part(1))
        }
        val convertedUpdatedPartitionPaths = CarbonScalaUtil.updatePartitions(
          mappedParts,
          CarbonEnv.getCarbonTable(catalogTable.get.identifier)(sparkSession)
        )

        val cols = partitionColumns
          .map(col => {
            val c = new mutable.StringBuilder()
            c.append(col.name).append("=")
              .append(convertedUpdatedPartitionPaths.get(col.name).get)
              .toString()
          })
        cols.toList.mkString("/")
    }

    // update metastore partition metadata
    refreshUpdatedPartitions(update)

    // refresh cached files in FileIndex
    fileIndex.foreach(_.refresh())
    // refresh data cache if table is cached
    sparkSession.catalog.refreshByPath(outputPath.toString)

    if (catalogTable.nonEmpty) {
      CommandUtils.updateTableStats(sparkSession, catalogTable.get)
    }
  }
}
