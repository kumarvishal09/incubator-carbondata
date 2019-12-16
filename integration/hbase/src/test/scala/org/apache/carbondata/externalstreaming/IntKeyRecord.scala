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
package org.apache.carbondata.externalstreaming

import java.text.SimpleDateFormat
import java.util.Date

case class IntKeyRecord(
    col0: Integer,
    col1: String,
    col2: Int)

object IntKeyRecord {
  def apply(i: Int): IntKeyRecord = {
    IntKeyRecord(
      i,
      s"String$i extra",
      i + 10)
  }
}

case class MultiDataTypeKeyRecordAllType(
    col0: Integer,
    col1: String,
    var col2: Long,
    col3: Double,
    col4: String,
    col5: Double,
    col6: Double,
    col7: Boolean,
    col8: Short,
    col9: Long,
    col10: Double)

object MultiDataTypeKeyRecordGenerator {
  val formatter = new SimpleDateFormat("dd-MM-yyyy")
  def apply(i: Int): MultiDataTypeKeyRecordAllType = {
    MultiDataTypeKeyRecordAllType(
      i,
      s"String$i extra",
      10,
      10.5,
      formatter.format(System.currentTimeMillis()),
      75.5 * i,
      75.5 * i,
      true,
      i.toShort,
      System.currentTimeMillis(),
      75.05 * i)
  }
}



