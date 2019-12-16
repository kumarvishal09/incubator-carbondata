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

import java.util.Objects;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

import org.apache.log4j.Logger;

public class SegmentPrunerFactory {
  private static final Logger LOG =
      LogServiceFactory.getLogService(SegmentPrunerFactory.class.getName());
  public static final SegmentPrunerFactory INSTANCE = new SegmentPrunerFactory();
  private SegmentPrunerFactory() {
  }
  public SegmentPruner getSegmentPruner(CarbonTable carbonTable) {
    SegmentPruner segmentPruner = null;
    String customPruner =
        carbonTable.getTableInfo().getFactTable().getTableProperties().get("custom.pruner");
    if (Objects.nonNull(customPruner)) {
      try {
         segmentPruner = (SegmentPruner) Class.forName(customPruner).newInstance();
      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
        throw new RuntimeException(e);
      }
    }
    if (segmentPruner == null) {
      segmentPruner = new SegmentPrunerImpl();
    }
    LOG.info("Segment pruner instantiated : " + segmentPruner.getClass().getName());
    return segmentPruner;
  }
}
