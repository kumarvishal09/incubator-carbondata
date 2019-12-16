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

import org.apache.hadoop.hbase.client.{Delete, Mutation, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.hbase.types.{SHCDataType, SHCDataTypeFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{LongType, MapType, StringType, StructType}
import org.apache.spark.util.Utils

class CarbonHBaseRelation(parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType])
  (sqlContext: SQLContext) extends HBaseRelation(parameters, userSpecifiedSchema)(sqlContext) {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new CarbonHBaseTableScanRDD(this,
      requiredColumns.filterNot(f => f.equalsIgnoreCase("rowtimestamp")),
      filters,
      requiredColumns)
  }

  /**
   *
   * @param data DataFrame to write to hbase
   * @param overwrite Overwrite existing values
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val delete = parameters.get("deleterows").map(_.toBoolean).getOrElse(false)
    if (!delete) {
      super.insert(data, overwrite)
    } else {
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, s"${catalog.namespace}:${catalog.name}")
      val job = Job.getInstance(hbaseConf)
      job.setOutputFormatClass(classOf[TableOutputFormat[String]])
      job.setOutputValueClass(classOf[Mutation])
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])

      // This is a workaround for SPARK-21549. After it is fixed, the snippet can be removed.
      val jobConfig = job.getConfiguration
      val tempDir = Utils.createTempDir()
      if (jobConfig.get("mapreduce.output.fileoutputformat.outputdir") == null) {
        jobConfig.set("mapreduce.output.fileoutputformat.outputdir", tempDir.getPath + "/outputDataset")
      }

      val rdd = data.rdd //df.queryExecution.toRdd

      rdd.mapPartitions(iter => {
        SHCCredentialsManager.processShcToken(serializedToken)
        iter.map(convertToDelete(catalog.getRowKey))
      }).saveAsNewAPIHadoopDataset(jobConfig)
    }
  }

  private def convertToDelete(rkFields: Seq[Field] )(row: Row): (ImmutableBytesWritable, Delete) = {
    val rkIdxedFields = rkFields.map{ case x =>
      (schema.fieldIndex(x.colName), x)
    }
    val colsIdxedFields = schema
      .fieldNames.filter(_.equalsIgnoreCase("rowtimestamp")).map(x => (schema.fieldIndex(x), catalog.getField(x)))
    val coder: SHCDataType = catalog.shcTableCoder

    // construct bytes for row key
    val rBytes: Array[Byte] =
      if (isComposite()) {
        val rowBytes = coder.encodeCompositeRowKey(rkIdxedFields, row)

        val rLen = rowBytes.foldLeft(0) { case (x, y) =>
          x + y.length
        }
        val rBytes = new Array[Byte](rLen)
        var offset = 0
        rowBytes.foreach { x =>
          System.arraycopy(x, 0, rBytes, offset, x.length)
          offset += x.length
        }
        rBytes
      } else {
        rkIdxedFields.map { case (x, y) =>
          SHCDataTypeFactory.create(y).toBytes(row(x))
        }.head
      }

    // add timestamp if defined for whole table
    val delete: Delete = new Delete(rBytes, row.getLong(colsIdxedFields.head._1))
    (new ImmutableBytesWritable, delete)
  }
}
