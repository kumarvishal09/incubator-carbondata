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

package org.apache.carbondata.hbase;

import org.apache.carbondata.core.util.annotations.CarbonProperty;

public interface HBaseConstants {
  /**
   * hbase conf file path
   */
  @CarbonProperty(dynamicConfigurable = false) String CARBON_HBASE_CONF_FILE_PATH =
      "carbon.hbase.conf.file.path";

  String DOT = ".";

  String BACKTICK = "`";

  String CARBON_HBASE_FORMAT_NAME = "hbase";

  String CARBON_HABSE_ROW_TIMESTAMP_COLUMN = "rowtimestamp";

  String CABON_HBASE_DEFAULT_COLUMN_FAMILY = "defaultColumnFamily";

  String CABON_HBASE_ROWKEY_COLUMN_FAMILY = "rowKeyColumnFamily";

  String CARBON_HBASE_OPERATION_TYPE_COLUMN = "operation_type_column";

  String CARBON_HBASE_INSERT_OPERATION_VALUE = "insert_operation_value";

  String CARBON_HBASE_UPDATE_OPERATION_VALUE = "update_operation_value";

  String CARBON_HBASE_DELETE_OPERATION_VALUE = "delete_operation_value";
}
