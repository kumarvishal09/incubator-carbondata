package org.apache.carbondata.presto.hbase.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HbaseColumn {

  private String colName;
  private String colNameWithoutCf;
  private String cf;
  private String col;
  private String fCoder;
  private int len = -1;
  private String type;

  @JsonCreator
  public HbaseColumn(@JsonProperty("colName") String colName,
      @JsonProperty("cf") String cf,
      @JsonProperty("col") String col,
      @JsonProperty("fCoder") String fCoder,
      @JsonProperty("len") int len,
      @JsonProperty("type") String type) {
    this.colName = colName;
    this.cf = cf;
    this.col = col;
    this.fCoder = fCoder;
    this.len = len;
    this.type = type;
    this.colNameWithoutCf = colName.substring(cf.length()+1);
  }

  @JsonProperty
  public String getColName() {
    return colName;
  }
  public String getColNameWithoutCf() {
    return colNameWithoutCf;
  }
  @JsonProperty
  public String getCf() {
    return cf;
  }
  @JsonProperty
  public String getCol() {
    return col;
  }
  @JsonProperty
  public String getfCoder() {
    return fCoder;
  }
  @JsonProperty
  public int getLen() {
    return len;
  }
  @JsonProperty
  public String getType() {
    return type;
  }
}
