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
package org.apache.carbondata.core.indexstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.executer.FilterExecutor;
import org.apache.carbondata.core.scan.filter.executer.MinMaxPruneMetadata;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.segmentmeta.SegmentColumnMetaDataInfo;
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

public class SegmentPrunerImpl implements SegmentPruner {

  private static final Logger LOGGER = LogServiceFactory.getLogService(SegmentPrunerImpl.class.getName());

  @Override
  public List<PrunedSegmentInfo> pruneSegment(CarbonTable table, Expression filterExp,
      String[] inputSegments, String[] excludeSegment) {
    List<Segment> allSegments;
    try {
      final Set<String> excludeSegmentList = new HashSet<>(Arrays.asList(excludeSegment));
      final Set<String> inputSegmentList = new HashSet<>(Arrays.asList(inputSegments));
      final ReadCommittedScope readCommittedScope =
          new TableStatusReadCommittedScope(table.getAbsoluteTableIdentifier(),
              FileFactory.getConfiguration());
      final LoadMetadataDetails[] segmentList = readCommittedScope.getSegmentList();
      final Stream<LoadMetadataDetails> successSegmentStream = Stream.of(segmentList).filter(loadMetadataDetails ->
          (loadMetadataDetails.getSegmentStatus().equals(SegmentStatus.SUCCESS)
              || loadMetadataDetails.getSegmentStatus().equals(SegmentStatus.LOAD_PARTIAL_SUCCESS)));
      final String[] allSegmentNames = successSegmentStream.map(LoadMetadataDetails::getLoadName).toArray(String[]::new);
      final String logString = String.format("%s-All Segment List:%s-Include segment list:%s- Exclude Segment List: %s",
          table.getTableUniqueName(), Arrays.toString(allSegmentNames),
          Arrays.toString(inputSegments), Arrays.toString(excludeSegment));
      LOGGER.info(logString);
      List<LoadMetadataDetails> successLoadMetadataDetails =
          Stream.of(readCommittedScope.getSegmentList()).filter(loadMetadataDetail ->
              (loadMetadataDetail.getSegmentStatus().equals(SegmentStatus.SUCCESS)
                  || loadMetadataDetail.getSegmentStatus()
                  .equals(SegmentStatus.LOAD_PARTIAL_SUCCESS)) && (excludeSegmentList.isEmpty()
                  || !excludeSegmentList.contains(loadMetadataDetail.getLoadName())) && (
                  inputSegmentList.isEmpty() || inputSegmentList
                      .contains(loadMetadataDetail.getLoadName()))).collect(Collectors.toList());
      if (!inputSegmentList.isEmpty()) {
        successLoadMetadataDetails.addAll(Stream.of(readCommittedScope.getSegmentList()).filter(
            loadMetadataDetail -> (loadMetadataDetail.getSegmentStatus()
                .equals(SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS)
                || loadMetadataDetail.getSegmentStatus().equals(SegmentStatus.INSERT_IN_PROGRESS)
                && inputSegmentList.contains(loadMetadataDetail.getLoadName())))
            .collect(Collectors.toList()));
      }
      allSegments = successLoadMetadataDetails.stream().map(
          successDetails -> new Segment(successDetails.getLoadName(),
              successDetails.getSegmentFile(), readCommittedScope, successDetails))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    String[] allSegmentNames =
        allSegments.stream().map(Segment::getSegmentNo).toArray(String[]::new);
    final List<PrunedSegmentInfo> prunedSegmentInfos = allSegments.stream().map(segment -> {
      SegmentFileStore.SegmentFile segmentFile;
      try {
        segmentFile = SegmentFileStore.readSegmentFile(
            CarbonTablePath.getSegmentFilePath(table.getTablePath(), segment.getSegmentFileName()));
        if (Objects.nonNull(segmentFile)) {
          segment.setSegmentMetaDataInfo(segmentFile.getSegmentMetaDataInfo());
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      SegmentMetaDataInfo segmentMetaDataInfo = segment.getSegmentMetaDataInfo();
      if (null == segmentMetaDataInfo || segmentMetaDataInfo.getSegmentColumnMetaDataInfoMap()
          .isEmpty()) {
        return new PrunedSegmentInfo(segment, segmentFile);
      }
      if (null == filterExp) {
        return new PrunedSegmentInfo(segment, segmentFile);
      }
      return getPrunedSegment(segment, table, filterExp, segmentMetaDataInfo, segmentFile);
    }).filter(Objects::nonNull).collect(Collectors.toList());
    final String[] prunedSegments = prunedSegmentInfos.stream()
        .map(prunedSegmentInfo -> prunedSegmentInfo.getSegment().getSegmentNo()).toArray(String[]::new);
    LOGGER.info(String.format("FilterExp:%s-All Segments: %s-Pruned Segment%s", Objects.isNull(filterExp),
        Arrays.toString(allSegmentNames), Arrays.toString(prunedSegments)));
    return prunedSegmentInfos;
  }

  private PrunedSegmentInfo getPrunedSegment(Segment segment, CarbonTable carbonTable,
      Expression filter, SegmentMetaDataInfo segmentMetaDataInfo,
      SegmentFileStore.SegmentFile segmentFile) {
    Map<String, SegmentColumnMetaDataInfo> segmentColumnMetaDataInfoMap =
        segmentMetaDataInfo.getSegmentColumnMetaDataInfoMap();
    int length = segmentColumnMetaDataInfoMap.size();
    // Add columnSchemas based on the columns present in segment
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    byte[][] min = new byte[length][];
    byte[][] max = new byte[length][];
    boolean[] minMaxFlag = new boolean[length];
    int index = 0;
    // get current columnSchema list for the table
    Map<String, ColumnSchema> tableColumnSchemas =
        carbonTable.getTableInfo().getFactTable().getListOfColumns().stream()
            .collect(Collectors.toMap(ColumnSchema::getColumnUniqueId, ColumnSchema::clone));
    // fill min,max and columnSchema values
    for (Map.Entry<String, SegmentColumnMetaDataInfo> columnMetaData : segmentColumnMetaDataInfoMap
        .entrySet()) {
      ColumnSchema columnSchema = tableColumnSchemas.get(columnMetaData.getKey());
      if (null != columnSchema) {
        updateColumnSchemaForRestructuring(columnMetaData, columnSchema);
        columnSchemas.add(columnSchema);
        min[index] = columnMetaData.getValue().getColumnMinValue();
        max[index] = columnMetaData.getValue().getColumnMaxValue();
        minMaxFlag[index] = min[index].length != 0 && max[index].length != 0;
        index++;
      }
    }
    // get segmentProperties using created columnSchemas list
    SegmentProperties segmentProperties = SegmentPropertiesAndSchemaHolder.getInstance()
        .addSegmentProperties(carbonTable, columnSchemas, segment.getSegmentNo())
        .getSegmentProperties();
    FilterResolverIntf resolver =
        new IndexFilter(segmentProperties, carbonTable, filter).getResolver();
    // prepare filter executor using datmapFilter resolver
    FilterExecutor filterExecutor =
        FilterUtil.getFilterExecutorTree(resolver, segmentProperties, null, null, false);
    // check if block has to be pruned based on segment minmax
    if (filterExecutor
        .isScanRequired(new MinMaxPruneMetadata(max, min, minMaxFlag, segment.isCarbonSegment()))
        .isEmpty()) {
      return null;
    }
    return new PrunedSegmentInfo(segment, segmentFile);
  }

  private void updateColumnSchemaForRestructuring(
      Map.Entry<String, SegmentColumnMetaDataInfo> columnMetaData, ColumnSchema columnSchema) {
    // get segment sort column and column drift info
    boolean isSortColumnInSegment = columnMetaData.getValue().isSortColumn();
    boolean isColumnDriftInSegment = columnMetaData.getValue().isColumnDrift();
    if (null != columnSchema.getColumnProperties()) {
      // get current sort column and column drift info from current columnSchema
      String isSortColumn =
          columnSchema.getColumnProperties().get(CarbonCommonConstants.SORT_COLUMNS);
      String isColumnDrift =
          columnSchema.getColumnProperties().get(CarbonCommonConstants.COLUMN_DRIFT);
      if (null != isSortColumn) {
        if (isSortColumn.equalsIgnoreCase("true") && !isSortColumnInSegment) {
          // Unset current column schema column properties
          modifyColumnSchemaForSortColumn(columnSchema, isColumnDriftInSegment, isColumnDrift,
              false);
        } else if (isSortColumn.equalsIgnoreCase("false") && isSortColumnInSegment) {
          // set sort column to true in current column schema column properties
          modifyColumnSchemaForSortColumn(columnSchema, isColumnDriftInSegment, isColumnDrift,
              true);
        }
      } else {
        modifyColumnSchemaForSortColumn(columnSchema, isColumnDriftInSegment, isColumnDrift, false);
      }
    }
  }

  private void modifyColumnSchemaForSortColumn(ColumnSchema columnSchema, boolean columnDrift,
      String isColumnDrift, boolean isSortColumnInSegment) {
    if (!isSortColumnInSegment) {
      if (null != isColumnDrift && isColumnDrift.equalsIgnoreCase("true") && !columnDrift) {
        columnSchema.setDimensionColumn(false);
      }
      columnSchema.setSortColumn(false);
      columnSchema.getColumnProperties().clear();
    } else {
      // modify column schema, if current columnSchema is changed
      columnSchema.setSortColumn(true);
      if (!columnSchema.isDimensionColumn()) {
        columnSchema.setDimensionColumn(true);
        columnSchema.getColumnProperties().put(CarbonCommonConstants.COLUMN_DRIFT, "true");
      }
      columnSchema.getColumnProperties().put(CarbonCommonConstants.SORT_COLUMNS, "true");
    }
  }
}
