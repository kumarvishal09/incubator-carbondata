package org.apache.carbondata.index;

public class SegmentSelector {
  private String includeSegmentIdString;
  private String excludeSegmentIdString;

  public String getIncludeSegmentIdString() {
    return includeSegmentIdString;
  }

  public void setIncludeSegmentIdString(String includeSegmentIdString) {
    this.includeSegmentIdString = includeSegmentIdString;
  }

  public String getExcludeSegmentIdString() {
    return excludeSegmentIdString;
  }

  public void setExcludeSegmentIdString(String excludeSegmentIdString) {
    this.excludeSegmentIdString = excludeSegmentIdString;
  }
}
