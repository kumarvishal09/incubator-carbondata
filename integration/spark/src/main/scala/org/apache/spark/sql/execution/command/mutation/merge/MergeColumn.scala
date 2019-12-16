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

package org.apache.spark.sql.execution.command.mutation.merge

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

object MergeColumn {
  def apply(expr: Expression): MergeColumn = new MergeColumn(expr)

  def apply(column: Column): MergeColumn = new MergeColumn(column.expr)
}

class MergeColumn(override val expr: Expression) extends Column(expr) {
  override def equals(that: Any): Boolean = {
    if (that != null && that.isInstanceOf[Column]) {
      val col = that.asInstanceOf[Column]
      col.expr.semanticEquals(expr)
    } else {
      false
    }
  }

  override def hashCode: Int = {
    expr.semanticHash()
  }
}