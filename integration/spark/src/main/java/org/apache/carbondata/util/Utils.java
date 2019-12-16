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

package org.apache.carbondata.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.spark.rdd.CarbonSparkPartition;

import org.apache.log4j.Logger;

public class Utils {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(Utils.class.getName());

  public static List<CarbonSparkPartition> mergeSplit(List<CarbonSparkPartition> allPartition,
      CarbonTable carbonTable) {
    if (allPartition.isEmpty()) {
      return allPartition;
    }
    long configuredSize = carbonTable.getBlockSizeInMB() * 1024L * 1024L;
    int minThreshold;
    try {
      minThreshold = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_MIN_THREASHOLD_FOR_SEGMENT_MERGER,
              CarbonCommonConstants.CARBON_MIN_THREASHOLD_FOR_SEGMENT_MERGER_DEFAULT));
    } catch (NumberFormatException exp) {
      minThreshold =
          Integer.parseInt(CarbonCommonConstants.CARBON_MIN_THREASHOLD_FOR_SEGMENT_MERGER_DEFAULT);
    }
    if (!carbonTable.isHivePartitionTable()) {
      return mergeSplitBasedOnSize(allPartition, configuredSize, minThreshold);
    }

    Map<PartitionSpec, List<CarbonSparkPartition>> partitionSpecToPartitionMap = new HashMap<>();
    for (CarbonSparkPartition carbonSparkPartition : allPartition) {
      List<CarbonSparkPartition> carbonSparkPartitions =
          partitionSpecToPartitionMap.get(carbonSparkPartition.partitionSpec().get());
      if (null == carbonSparkPartitions) {
        carbonSparkPartitions = new ArrayList<>();
        partitionSpecToPartitionMap
            .put(carbonSparkPartition.partitionSpec().get(), carbonSparkPartitions);
      }
      carbonSparkPartitions.add(carbonSparkPartition);
    }

    Iterator<Map.Entry<PartitionSpec, List<CarbonSparkPartition>>> iterator =
        partitionSpecToPartitionMap.entrySet().iterator();

    List<List<CarbonSparkPartition>> rejectedList = new ArrayList<>();
    List<List<CarbonSparkPartition>> selectedList = new ArrayList<>();

    int totalNumberOfPartition = partitionSpecToPartitionMap.size();
    while (iterator.hasNext()) {
      Map.Entry<PartitionSpec, List<CarbonSparkPartition>> entry = iterator.next();
      List<CarbonSparkPartition> carbonSparkPartitions =
          mergeSplitBasedOnSize(entry.getValue(), configuredSize, minThreshold);
      if (carbonSparkPartitions.isEmpty()) {
        rejectedList.add(entry.getValue());
      } else {
        selectedList.add(carbonSparkPartitions);
      }
    }

    boolean isRejectPresent = rejectedList.size() > 0;
    if (isRejectPresent) {
      int rejectedPercentage = (rejectedList.size() / totalNumberOfPartition) * 100;
      if (rejectedPercentage > minThreshold) {
        return new ArrayList<>();
      }
    }

    selectedList.addAll(rejectedList);
    List<CarbonSparkPartition> collect =
        selectedList.stream().flatMap(List::stream).collect(Collectors.toList());
    List<CarbonSparkPartition> result = new ArrayList<>();
    int rddId = allPartition.get(0).rddId();
    int counter = 0;
    for (CarbonSparkPartition carbonSparkPartition : collect) {
      result.add(new CarbonSparkPartition(rddId, counter++, carbonSparkPartition.multiBlockSplit(),
          carbonSparkPartition.partitionSpec()));
    }
    return result;
  }

  private static List<CarbonSparkPartition> mergeSplitBasedOnSize(
      List<CarbonSparkPartition> allPartition, long configuredSize, int minThreshold) {
    List<CarbonSparkPartition> result = new ArrayList<>();
    List<CarbonInputSplit> allSplits = new ArrayList<>();
    for (CarbonSparkPartition sparkPartition : allPartition) {
      allSplits.addAll(sparkPartition.multiBlockSplit().getAllSplits());
    }
    long totalSize = 0;
    long minSizeToConsider = (configuredSize * 80) / 100;
    long fileExceedingThreshold = 0;
    for (CarbonInputSplit split : allSplits) {
      totalSize += split.getLength();
      if (split.getLength() >= minSizeToConsider) {
        fileExceedingThreshold++;
      }
    }
    if (configuredSize > totalSize) {
      Set<String> locations = new HashSet<>();
      for (CarbonInputSplit split : allSplits) {
        try {
          locations.addAll(Arrays.asList(split.getLocations()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      result.add(new CarbonSparkPartition(0, 0,
          new CarbonMultiBlockSplit(allSplits, locations.toArray(new String[locations.size()])),
          allPartition.get(0).partitionSpec()));
      return result;
    }
    int actualMinThreshold = (allSplits.size() * minThreshold) / 100;
    if (fileExceedingThreshold >= actualMinThreshold) {
      return new ArrayList<>();
    }
    long noOfGroup = totalSize / configuredSize;
    long leftOver = totalSize % configuredSize;
    long newConfiguredSize = configuredSize + (long) (configuredSize * .20);
    int runningIndex = 0;
    long currentSize;
    List<List<CarbonInputSplit>> splitsGroup = new ArrayList<>();
    List<Set<String>> splitLocations = new ArrayList<>();
    for (int i = 0; i < noOfGroup; i++) {
      currentSize = 0;
      List<CarbonInputSplit> splits = new ArrayList<>();
      Set<String> locations = new HashSet<>();
      for (int j = runningIndex; j < allSplits.size(); j++) {
        splits.add(allSplits.get(j));
        currentSize += allSplits.get(j).getLength();
        try {
          locations.addAll(Arrays.asList(allSplits.get(j).getLocations()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        runningIndex++;
        if (currentSize >= newConfiguredSize) {
          break;
        }
      }
      splitsGroup.add(splits);
      splitLocations.add(locations);
      if (runningIndex >= allSplits.size()) {
        break;
      }
    }
    if (leftOver > 0 && runningIndex < allSplits.size()) {
      int startIndex = 0;
      for (int i = runningIndex; i < allSplits.size(); i++) {
        splitsGroup.get(startIndex).add(allSplits.get(i));
        try {
          splitLocations.get(startIndex).addAll(Arrays.asList(allSplits.get(i).getLocations()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        startIndex++;
        if (startIndex >= splitsGroup.size()) {
          startIndex = 0;
        }
      }
    }

    int counter = 0;
    for (List<CarbonInputSplit> splits : splitsGroup) {
      String[] loc =
          splitLocations.get(counter).toArray(new String[splitLocations.get(counter).size()]);
      result.add(new CarbonSparkPartition(allPartition.get(0).rddId(), counter++,
          new CarbonMultiBlockSplit(splits, loc),
          allPartition.get(0).partitionSpec()));
    }
    return result;
  }
}
