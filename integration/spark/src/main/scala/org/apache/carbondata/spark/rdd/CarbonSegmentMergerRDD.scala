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
package org.apache.carbondata.spark.rdd

import scala.collection.JavaConverters._

import org.apache.spark.Partition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.CarbonMergerMapping
import org.apache.spark.util.CollectionAccumulator

import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.MergeResult
import org.apache.carbondata.util.Utils

class CarbonSegmentMergerRDD[K, V](
    @transient private val ss: SparkSession,
    mergeResult: MergeResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    carbonMergerMapping: CarbonMergerMapping,
    segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]])
  extends CarbonMergerRDD(ss,
    mergeResult,
    carbonLoadModel,
    carbonMergerMapping,
    segmentMetaDataAccumulator) {

  override def internalGetPartitions: Array[Partition] = {
    val partitions = super.internalGetPartitions
    val mergedPartition = Utils.mergeSplit(partitions
      .toList
      .asInstanceOf[List[CarbonSparkPartition]]
      .asJava,
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable)
    mergedPartition.asScala.toArray
  }
}
