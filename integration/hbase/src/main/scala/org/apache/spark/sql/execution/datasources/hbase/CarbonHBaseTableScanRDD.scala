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

import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources.Filter

class CarbonHBaseTableScanRDD(relation: HBaseRelation,
    requiredColumns: Array[String],
    filters: Array[Filter],
    allRequiredColumns: Array[String]) extends HBaseTableScanRDD(relation, requiredColumns, filters) {

  val isTimeStampColumnRequired = allRequiredColumns.filter(f=>f.equalsIgnoreCase("rowtimestamp")).length > 0
  override def buildRow(fields: Seq[Field], result: Result): Row = {
    val row = super.buildRow(fields, result)
    val finalRow = if (isTimeStampColumnRequired) {
      Row.fromSeq(row.asInstanceOf[GenericRow].values :+ result.rawCells()(0).getTimestamp)
    } else {
      row
    }
    finalRow
  }
}
