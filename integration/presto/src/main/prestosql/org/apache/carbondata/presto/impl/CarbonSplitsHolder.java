package org.apache.carbondata.presto.impl;

import java.util.List;

import org.apache.carbondata.presto.hbase.split.HBaseSplit;

import io.prestosql.spi.connector.ConnectorSplit;

public class CarbonSplitsHolder {

  private List<CarbonLocalMultiBlockSplit> multiBlockSplits;

  private List<HBaseSplit> extraSplits;

  public CarbonSplitsHolder(List<CarbonLocalMultiBlockSplit> multiBlockSplits,
      List<HBaseSplit> extraSplits) {
    this.multiBlockSplits = multiBlockSplits;
    this.extraSplits = extraSplits;
  }

  public List<CarbonLocalMultiBlockSplit> getMultiBlockSplits() {
    return multiBlockSplits;
  }

  public List<HBaseSplit> getExtraSplits() {
    return extraSplits;
  }
}
