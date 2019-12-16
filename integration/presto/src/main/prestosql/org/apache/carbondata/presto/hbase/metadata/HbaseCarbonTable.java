package org.apache.carbondata.presto.hbase.metadata;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HbaseCarbonTable {

  private String namespace;
  private String name;
  private RowKey row;
  private HashMap<String, HbaseColumn> sMap;
  private String tCoder;

  @JsonCreator
  public HbaseCarbonTable(
      @JsonProperty("namespace") String namespace,
      @JsonProperty("name") String name,
      @JsonProperty("row") RowKey row,
      @JsonProperty("sMap") HashMap<String, HbaseColumn> sMap,
      @JsonProperty("tCoder") String tCoder) {
    this.namespace = namespace;
    this.name = name;
    this.row = row;
    this.sMap = sMap;
    this.tCoder = tCoder;
  }
  @JsonProperty
  public String getNamespace() {
    return namespace;
  }
  @JsonProperty
  public String getName() {
    return name;
  }
  @JsonProperty
  public RowKey getRow() {
    return row;
  }
  @JsonProperty
  public HashMap<String, HbaseColumn> getsMap() {
    return sMap;
  }
  @JsonProperty
  public String gettCoder() {
    return tCoder;
  }

  public static class RowKey {

    private String[] keys;

    private HbaseColumn[] hbaseColumns;


    public RowKey(String key) {
      this.keys = key.split(":");
    }
    @JsonCreator
    public RowKey(@JsonProperty("keys") String[] keys,
        @JsonProperty("hbaseColumns") HbaseColumn[] hbaseColumns) {
      this.keys = keys;
      this.hbaseColumns = hbaseColumns;
    }

    public void init(HashMap<String, HbaseColumn> sMap) {
      hbaseColumns = new HbaseColumn[keys.length];
      int i = 0;
      for (String key : keys) {
        for(HbaseColumn col: sMap.values()) {
          if (col.getCol().equalsIgnoreCase(key) && col.getCf().equalsIgnoreCase("rowkey")) {
            hbaseColumns[i++] = col;
          }
        }
      }
    }
    @JsonCreator
    public HbaseColumn[] getHbaseColumns() {
      return hbaseColumns;
    }
    @JsonCreator
    public String[] getKeys() {
      return keys;
    }

    public int length() {
      int len = 0;
      for (HbaseColumn hbaseColumn : hbaseColumns) {
        if (hbaseColumn.getLen() == -1) {
          len += 256;
        } else {
          len += hbaseColumn.getLen();
        }
      }
      return len;
    }

  }

}
