package org.apache.carbondata.presto.hbase.metadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HbaseMetastoreUtil {


  public static HbaseCarbonTable getHbaseTable(String catalog) throws IOException {

    JsonFactory factory = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper(factory);
    Map<String, Object> jsonNodeMap =
        mapper.readValue(catalog, new TypeReference<Map<String, Object>>() {});
    Map<String, String> tableMeta = (Map)jsonNodeMap.get("table");
    String nSpace = tableMeta.getOrDefault("namespace", "default");
    String tName = tableMeta.get("name");
    String tCoder = tableMeta.getOrDefault("tableCoder", "Phoenix");
    HbaseCarbonTable.RowKey rKey = new HbaseCarbonTable.RowKey((String) jsonNodeMap.get("rowkey"));

    HashMap<String, HbaseColumn> sMap = new LinkedHashMap<>();
    Map<String, Map<String, String>> colMeta = (Map)jsonNodeMap.get("columns");
    for (Map.Entry<String, Map<String, String>> entry: colMeta.entrySet()) {
      Map<String, String> column = entry.getValue();
      int len = Integer.parseInt(column.getOrDefault("length", "-1"));
      sMap.put(entry.getKey(), new HbaseColumn(entry.getKey(), column.getOrDefault("cf", "rowkey"), column.get("col"), tCoder, len, column.get("type")));
    }
    rKey.init(sMap);
    return new HbaseCarbonTable(nSpace, tName, rKey, sMap, tCoder);
  }

}
