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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionSet, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.hbase.{CarbonHBaseRelation, HBaseRelation, HBaseTableCatalog}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}

import org.apache.carbondata.core.indexstore.PrunedSegmentInfo
import org.apache.carbondata.core.statusmanager.FileFormat
import org.apache.carbondata.core.util.{ByteUtil, CarbonUtil}
import org.apache.carbondata.hbase.HBaseConstants._

class HBaseFormatBasedHandler extends ExternalFormatHandler {

  /**
   * Generates the RDD using the spark file format.
   */
  override def getRDDForExternalSegments(plan: LogicalPlan,
      format: FileFormat,
      prunedSegmentInfo: List[PrunedSegmentInfo],
      l: LogicalRelation,
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      supportBatch: Boolean): (RDD[InternalRow], Boolean) = {
    val externalSchema = CarbonUtil.getExternalSchema(l
      .relation
      .asInstanceOf[CarbonDatasourceHadoopRelation]
      .identifier)

    var params = Map(
      HBaseTableCatalog.tableCatalog -> externalSchema.getQuerySchema)
    if (!prunedSegmentInfo.head.isIgnoreTimeStamp) {
      val segmentFile = prunedSegmentInfo.head.getSegmentFile
      val  timestamp = segmentFile.getSegmentMetaDataInfo
        .getSegmentColumnMetaDataInfoMap.get(CARBON_HABSE_ROW_TIMESTAMP_COLUMN)
      if (timestamp != null) {
        val info = ByteUtil.toLong(timestamp.getColumnMinValue,
          0,
          ByteUtil.SIZEOF_LONG) + 1
        params = params + (HBaseRelation.MIN_STAMP -> info.toString,
          HBaseRelation.MAX_STAMP -> Long.MaxValue.toString)
      }
    }

    val hBaseRelation = new CarbonHBaseRelation(params, Option.empty)(l.relation.sqlContext)
    val hbasePlan = plan transform {
      case lR:LogicalRelation if lR.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation] =>
        LogicalRelation(hBaseRelation)
    }
    val rowKeyColFamily = externalSchema.getParam(CABON_HBASE_ROWKEY_COLUMN_FAMILY)
    val hbaseOutput = hbasePlan.collect{
      case l:LogicalRelation => l
    }.head.output.filter(f=> !f.name.startsWith(rowKeyColFamily))

    val updatedExp = hbasePlan.transformAllExpressions {
      case attr: AttributeReference if !attr.name.contains(".") =>
        hbaseOutput
          .find(a => a.name.split("\\.")(1).equals(attr.name)).get
    }
    val filterSet = ExpressionSet(filters.map{ f =>
      f.transform{
        case attr: AttributeReference if !attr.name.contains(".") =>
          hbaseOutput
            .find(a => a.name.split("\\.")(1).equals(attr.name)).get
      }
    })
    val filterAttributes = filterSet.flatMap(_.references)
    val projectionAttributes = ExpressionSet(updatedExp.output).flatMap(_.references)
    val leftOutProjects = filterAttributes -- filterAttributes.intersect(projectionAttributes)

    val withProject = updatedExp match {
      case l: LogicalRelation =>
        Project(l.output.filterNot(_.name.startsWith(rowKeyColFamily)), l)
      case f@Filter(c, l:LogicalRelation) =>
        Filter(c, Project(l.output.filterNot(_.name.startsWith(rowKeyColFamily)), l))
      case Filter(c, p: Project) =>
        Filter(c, Project((p.output ++ leftOutProjects.seq).filterNot(_.name.startsWith(rowKeyColFamily)), l))
      case Project(c, f: Filter) =>
        Project((c ++ leftOutProjects.seq).filterNot(_.name.startsWith(rowKeyColFamily)), f)
      case others => others
    }

    val hbaseRdd = DataSourceStrategy(l.relation.sqlContext.conf).apply(withProject).head.execute()
    (hbaseRdd, false)
  }
}
