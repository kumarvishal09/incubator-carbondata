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

package org.apache.carbondata.hadoop;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.carbondata.core.index.IndexStoreManager;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.transaction.TransactionAction;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.events.OperationListenerBus;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.processing.loading.events.LoadEvents;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

public class PartitionTransactionAction implements TransactionAction {

  private CarbonLoadModel carbonLoadModel;

  private LoadMetadataDetails loadMetadataDetails;

  private String uuid;

  private OperationContext operationContext;

  private Configuration configuration;

  private String updatedTimeStamp;

  private List<Segment> updatedSegments;

  private List<Segment> deletedSegments;

  private ICarbonLock segmentLock;

  private boolean isTransactionFlow;

  private boolean isOverwriteTable;

  private CarbonTable table;

  private boolean isCleanupDone;

  public PartitionTransactionAction(CarbonLoadModel carbonLoadModel,
      LoadMetadataDetails loadMetadataDetails, String uuid, OperationContext operationContext,
      Configuration configuration, String updatedTimeStamp, List<Segment> updatedSegments,
      List<Segment> deletedSegments, ICarbonLock segmentLock, boolean isTransactionFlow,
      boolean isOverwriteTable) {
    this.carbonLoadModel = carbonLoadModel;
    this.loadMetadataDetails = loadMetadataDetails;
    this.uuid = uuid;
    this.operationContext = operationContext;
    this.configuration = configuration;
    this.updatedTimeStamp = updatedTimeStamp;
    this.updatedSegments = updatedSegments;
    this.deletedSegments = deletedSegments;
    this.segmentLock = segmentLock;
    this.isTransactionFlow = isTransactionFlow;
    this.isOverwriteTable = isOverwriteTable;
    table = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
  }

  public PartitionTransactionAction(CarbonLoadModel carbonLoadModel,
      LoadMetadataDetails loadMetadataDetails) {
    this.carbonLoadModel = carbonLoadModel;
    this.loadMetadataDetails = loadMetadataDetails;
  }

  @Override
  public void commit() throws Exception {
    try {
      if (loadMetadataDetails.getSegmentStatus() == SegmentStatus.MARKED_FOR_DELETE
          || loadMetadataDetails.getSegmentStatus() == SegmentStatus.LOAD_FAILURE) {
        throw new Exception("Failed to commit transaction:");
      }
      CarbonLoaderUtil
          .recordNewLoadMetadata(loadMetadataDetails, carbonLoadModel, false, false, uuid,
              deletedSegments, updatedSegments, false);
      if (operationContext != null) {
        operationContext.setProperty("current.segmentfile", loadMetadataDetails.getSegmentFile());
      }
      commitJobFinal();
    } finally {
      if (!isCleanupDone && isOverwriteTable) {
        IndexStoreManager.getInstance().clearIndex(table.getAbsoluteTableIdentifier());
        // Clean the overwriting segments if any.
        SegmentFileStore.cleanSegments(table, null, false);
        isCleanupDone = true;
      }
      if (isTransactionFlow && null != segmentLock) {
        segmentLock.unlock();
      }
    }
  }

  private void commitJobFinal() throws IOException {
    CarbonTable carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
    if (operationContext != null) {
      LoadEvents.LoadTablePostStatusUpdateEvent postStatusUpdateEvent =
          new LoadEvents.LoadTablePostStatusUpdateEvent(carbonLoadModel);
      try {
        OperationListenerBus.getInstance()
            .fireEvent(postStatusUpdateEvent, operationContext);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    String updateTime = configuration.get(CarbonTableOutputFormat.UPDATE_TIMESTAMP, updatedTimeStamp);
    String segmentsToBeDeleted =
        configuration.get(CarbonTableOutputFormat.SEGMENTS_TO_BE_DELETED, "");
    List<Segment> segmentDeleteList = Collections.emptyList();
    if (!segmentsToBeDeleted.trim().isEmpty()) {
      segmentDeleteList = Segment.toSegmentList(segmentsToBeDeleted.split(","), null);
    }
    boolean isUpdateStatusFileUpdateRequired =
        (configuration.get(CarbonTableOutputFormat.UPDATE_TIMESTAMP) != null);
    if (updateTime != null) {
      //TODO CHECK updateTime OR updatedTimeStamp TIME REQUIRED FOR UPDATTION
      CarbonUpdateUtil.updateTableMetadataStatus(Collections.singleton(carbonLoadModel.getSegment()),
          carbonTable, updatedTimeStamp, true,
          isUpdateStatusFileUpdateRequired, segmentDeleteList);
    }
  }

  @Override
  public void rollback() throws Exception {
    try {
      if (loadMetadataDetails.getSegmentStatus() == SegmentStatus.MARKED_FOR_DELETE
          || loadMetadataDetails.getSegmentStatus() == SegmentStatus.LOAD_FAILURE) {
        return;
      }
      CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel);
      CarbonLoaderUtil.runCleanupForPartition(carbonLoadModel);
    } finally {
      if (!isCleanupDone && isOverwriteTable) {
        IndexStoreManager.getInstance().clearIndex(table.getAbsoluteTableIdentifier());
        // Clean the overwriting segments if any.
        SegmentFileStore.cleanSegments(table, null, false);
        isCleanupDone = true;
      }
    }
  }

  public void recordUpdateDetails(long updateTime, Segment[] deletedSegments,
      boolean loadAsANewSegment) {
    String currentUpdateTime = configuration.get(CarbonTableOutputFormat.UPDATE_TIMESTAMP, null);
    if (null != currentUpdateTime) {
      configuration.set(CarbonTableOutputFormat.UPDATE_TIMESTAMP, String.valueOf(updateTime));
    }
    String segmentToBeDeleted =
        configuration.get(CarbonTableOutputFormat.SEGMENTS_TO_BE_DELETED, null);
    if (null != segmentToBeDeleted) {
      String deletedSegmentId = String
          .join(",", Stream.of(deletedSegments).map(Segment::getSegmentNo).toArray(String[]::new));
      configuration.get(CarbonTableOutputFormat.SEGMENTS_TO_BE_DELETED, deletedSegmentId);
    }
  }

  public String getTransactionTableName() {
    return carbonLoadModel.getDatabaseName() + "." + carbonLoadModel.getTableName();
  }
}
