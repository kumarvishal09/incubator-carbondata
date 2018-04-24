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
package org.apache.carbondata.mv.tool.reader;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.mv.tool.constants.MVAdvisiorConstants;
import org.apache.carbondata.mv.tool.vo.ColStats;
import org.apache.carbondata.mv.tool.vo.TableDetailsVo;
import org.apache.carbondata.mv.tool.vo.TblStatsVo;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

/**
 * Json file reader utility class
 */
public class JsonFileReader {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(JsonFileReader.class.getName());

  private JsonFileReader() {

  }

  /**
   *
   * @param filePath table stats file path
   * @return table details
   * @throws IOException
   */
  public static List<TableDetailsVo> readTableDetails(String filePath) throws IOException {
    JsonParser parser = new JsonParser();
    List<TableDetailsVo> tblDetailsVo = new ArrayList<>();
    try(FileReader reader = new FileReader(filePath)) {
      JsonElement parse = parser.parse(new JsonReader(reader));
      Iterator<JsonElement> iterator = parse.getAsJsonArray().iterator();
      while (iterator.hasNext()) {
        JsonElement next = iterator.next();
        JsonObject object = ((JsonObject) next);
        if (null == object.get(MVAdvisiorConstants.JSON_TABLENAME) || null == object
            .get(MVAdvisiorConstants.JSON_ISFACTTABLE) || null == object
            .get(MVAdvisiorConstants.JSON_SCHEMA) || null == object
            .get(MVAdvisiorConstants.JSON_TABLESTATS) || null == object
            .get(MVAdvisiorConstants.JSON_COLSTATS)) {
          throw new RuntimeException("Invalid JSON File");
        }
        Iterator<JsonElement> colstats =
            object.get(MVAdvisiorConstants.JSON_COLSTATS).getAsJsonArray().iterator();
        Map<String, ColStats> colStatsMap = new HashMap<>();
        while (colstats.hasNext()) {
          JsonObject next1 = (JsonObject) colstats.next();
          if (null == next1.get(MVAdvisiorConstants.JSON_COLNAME) || null == next1
              .get(MVAdvisiorConstants.JSON_NDV)) {
            throw new RuntimeException("Invalid File columnName or ndv cannot be empty");
          }
          String columName = next1.get(MVAdvisiorConstants.JSON_COLNAME).getAsString();
          ColStats stats = new ColStats(next1.get(MVAdvisiorConstants.JSON_NDV).getAsLong());
          if(null!= next1.get(MVAdvisiorConstants.JSON_MAXVALUE)) {
            stats.setMaxValue(next1.get(MVAdvisiorConstants.JSON_MAXVALUE).getAsString());
          }
          if(null != next1.get(MVAdvisiorConstants.JSON_MINVALUE)) {
            stats.setMinValue(next1.get(MVAdvisiorConstants.JSON_MINVALUE).getAsString());
          }
          if (null != next1.get(MVAdvisiorConstants.JSON_NULLCOUNT)) {
            stats.setNullCount(next1.get(MVAdvisiorConstants.JSON_NULLCOUNT).getAsInt());
          }
          if (null != next1.get(MVAdvisiorConstants.JSON_MAXLENGTH)) {
            stats.setMaxLength(next1.get(MVAdvisiorConstants.JSON_MAXLENGTH).getAsInt());
          }
          colStatsMap.put(columName, stats);
        }
        JsonObject tblStats =
            ((JsonObject) object.get(MVAdvisiorConstants.JSON_TABLESTATS).getAsJsonArray().get(0));
        TblStatsVo tblStatsVo =
            new TblStatsVo(tblStats.get(MVAdvisiorConstants.JSON_TABLESIZE).getAsLong(),
                tblStats.get(MVAdvisiorConstants.JSON_ROWCOUNT).getAsLong());
        TableDetailsVo tableDetailsVo =
            new TableDetailsVo(object.get(MVAdvisiorConstants.JSON_TABLENAME).getAsString(),
                object.get(MVAdvisiorConstants.JSON_ISFACTTABLE).getAsBoolean(),
                object.get(MVAdvisiorConstants.JSON_SCHEMA).getAsString(), tblStatsVo,
                colStatsMap);
        tblDetailsVo.add(tableDetailsVo);
      }
    } catch(IOException e) {
       LOGGER.error(e, "Problem while reading");
       throw e;
    }
    return tblDetailsVo;
  }

  /**
   * Method to read query sql file
   * @param querySqlFile query sql file
   * @return List of query string
   * @throws IOException exception while reading
   */
  public static List<String> readQuerySql(String querySqlFile) throws IOException {
    JsonParser parser = new JsonParser();
    List<String> querySqls = new ArrayList<>();
    try(FileReader reader = new FileReader(querySqlFile)) {
      JsonElement parse = parser.parse(new JsonReader(reader));
      Iterator<JsonElement> iterator = parse.getAsJsonArray().iterator();
      while (iterator.hasNext()) {
        JsonElement next = iterator.next();
        JsonObject object = ((JsonObject) next);
        querySqls.add(object.get(MVAdvisiorConstants.JSON_SQL).getAsString());
      }
    } catch(IOException e) {
      LOGGER.error(e, "Problem while reading");
      throw e;
    }
    return querySqls;
  }
}
