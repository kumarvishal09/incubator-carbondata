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
package org.apache.carbondata.hbase.schema;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.commons.lang3.StringUtils;

public class HBaseSchemaInfo {
  @Expose
  private LinkedHashMap<String, Map<String, Object>> columns;
  @Expose
  private String rowkey;
  @Expose
  private Map<String, String> table;
  private String keyDistribution;
  private String bucketsCount;

  public static String addIncrementalColumn(String writeCat) {
    Gson gson = new Gson();
    HBaseSchemaInfo HBaseSchemaInfo = gson.fromJson(writeCat, HBaseSchemaInfo.class);
    Map<String, Object> field = new HashMap<>();
    field.put("col", "rowtimestamp");
    field.put("cf", "cfrowtimestamp");
    field.put("type", "bigint");
    HBaseSchemaInfo.columns.put("rowtimestamp", field);
    if (StringUtils.isBlank(HBaseSchemaInfo.keyDistribution)) {
      return gson.toJson(HBaseSchemaInfo);
    } else {
      gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
      return gson.toJson(HBaseSchemaInfo);
    }
  }

  public LinkedHashMap<String, Map<String, Object>> getColumns() {
    return columns;
  }
}
