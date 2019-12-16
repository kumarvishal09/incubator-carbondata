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
package org.apache.carbondata.hbase.segmentpruner;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.indexstore.PrunedSegmentInfo;
import org.apache.carbondata.core.indexstore.SegmentPrunerImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;

import org.apache.log4j.Logger;

public class OpenTableSegmentPruner extends SegmentPrunerImpl {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(OpenTableSegmentPruner.class.getName());

  @Override
  public List<PrunedSegmentInfo> pruneSegment(CarbonTable table, Expression filterExp,
      String[] inputSegments, String[] excludeSegment) {
    List<PrunedSegmentInfo> prunedSegmentInfos =
        super.pruneSegment(table, filterExp, inputSegments, excludeSegment);
    List<PrunedSegmentInfo> hbase = prunedSegmentInfos.stream().filter(
        seg -> seg.getSegment().getLoadMetadataDetails().getFileFormat().toString()
            .equalsIgnoreCase("hbase")).collect(Collectors.toList());
    if (hbase.size() > 0) {
      LOGGER.info("Selected Segments: " + hbase);
      hbase.get(0).setIgnoreTimeStamp(true);
      return hbase;
    } else {
      return prunedSegmentInfos;
    }
  }
}
