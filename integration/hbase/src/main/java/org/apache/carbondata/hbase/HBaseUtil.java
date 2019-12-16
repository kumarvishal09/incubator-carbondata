package org.apache.carbondata.hbase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.segmentmeta.SegmentColumnMetaDataInfo;
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo;
import org.apache.carbondata.core.util.ByteUtil;

public class HBaseUtil {

  private HBaseUtil() {

  }

  public static String updateColumnFamily(String columnFamilyName, String columnName) {
    return HBaseConstants.BACKTICK + columnFamilyName + HBaseConstants.DOT + columnName
        + HBaseConstants.BACKTICK;
  }

  public static SegmentMetaDataInfo getHBaseSegmentMetadataInfo(Set<String> minmaxColumnId,
      SegmentMetaDataInfo currentLoadSegmentMetaDataInfo, long maxTimeStamp) {
    Map<String, SegmentColumnMetaDataInfo> columnMinMaxInfos = new LinkedHashMap<>();
    currentLoadSegmentMetaDataInfo.getSegmentColumnMetaDataInfoMap().entrySet().forEach(entry -> {
      if (minmaxColumnId.contains(entry.getKey())) {
        columnMinMaxInfos.put(entry.getKey(),
            new SegmentColumnMetaDataInfo(entry.getValue().isSortColumn(),
                entry.getValue().getColumnMaxValue(), "NOTUSEDMAX".getBytes(),
                entry.getValue().isColumnDrift()));
      } else {
        columnMinMaxInfos.put(entry.getKey(),
            new SegmentColumnMetaDataInfo(entry.getValue().isSortColumn(),
            new byte[0], new byte[0],
            entry.getValue().isColumnDrift()));
      }
    });
    columnMinMaxInfos.put("rowtimestamp", new SegmentColumnMetaDataInfo(false, ByteUtil.toBytes(maxTimeStamp), new byte[0],false));
    return new SegmentMetaDataInfo(columnMinMaxInfos);
  }
}
