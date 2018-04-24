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
package org.apache.carbondata.mv.tool.vo;

import java.util.Map;

/**
 * Table details class
 */
public class TableDetailsVo {

  private String tableName;

  private String createTableStmt;

  private Map<String, ColStats> colsStatsMap;

  private boolean isFactTable;

  private TblStatsVo tblStatsVo;

  public TableDetailsVo(String tableName, boolean isFactTable, String createTableStmt,
      TblStatsVo tblStatsVo, Map<String, ColStats> colsStatsMap) {
    this.tableName = tableName;
    this.createTableStmt = createTableStmt;
    this.colsStatsMap = colsStatsMap;
    this.isFactTable = isFactTable;
    this.tblStatsVo = tblStatsVo;
  }

  public String getTableName() {
    return tableName;
  }

  public String getCreateTableStmt() {
    return createTableStmt;
  }

  public Map<String, ColStats> getColumnStats() {
    return colsStatsMap;
  }

  public boolean isFactTable() {
    return isFactTable;
  }

  public TblStatsVo getTblStatsVo() {
    return tblStatsVo;
  }
}
