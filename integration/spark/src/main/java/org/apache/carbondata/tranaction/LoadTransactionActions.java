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

package org.apache.carbondata.tranaction;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.transaction.TransactionAction;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory;
import org.apache.carbondata.view.MVManagerInSpark;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.command.UpdateTableModel;

public class LoadTransactionActions implements TransactionAction {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LoadTransactionActions.class.getName());

  private SparkSession sparkSession;

  private CarbonLoadModel carbonLoadModel;

  private LoadMetadataDetails loadMetadataDetails;

  private boolean overwriteTable;

  private String uuid;

  private String segmentFileName;

  private OperationContext operationContext;

  private Configuration configuration;

  private UpdateTableModel tableModel;

  private boolean isValidSegment;

  private ICarbonLock segmentLock;

  public LoadTransactionActions(SparkSession sparkSession, CarbonLoadModel carbonLoadModel,
      LoadMetadataDetails loadMetadataDetails, boolean overwriteTable, String uuid,
      UpdateTableModel tableModel, String segmentFileName, OperationContext operationContext,
      Configuration configuration, boolean isValidSegment, ICarbonLock segmentLock) {
    this.sparkSession = sparkSession;
    this.carbonLoadModel = carbonLoadModel;
    this.loadMetadataDetails = loadMetadataDetails;
    this.overwriteTable = overwriteTable;
    this.uuid = uuid;
    this.segmentFileName = segmentFileName;
    this.operationContext = operationContext;
    this.configuration = configuration;
    this.tableModel = tableModel;
    this.isValidSegment = isValidSegment;
    this.segmentLock = segmentLock;
  }

  public void commit() throws Exception {
    try {
      SegmentStatus segmentStatus = loadMetadataDetails.getSegmentStatus();
      if ((segmentStatus == SegmentStatus.MARKED_FOR_DELETE
          || segmentStatus == SegmentStatus.LOAD_FAILURE) && isValidSegment) {
        throw new Exception("Failed to commit transaction:");
      }
      boolean isDone;
      if (tableModel != null) {
        isDone = CarbonLoaderUtil
            .writeTableStatus(carbonLoadModel, loadMetadataDetails, overwriteTable,
                tableModel.loadAsNewSegment(), Arrays.asList(tableModel.deletedSegments()), uuid,
                tableModel.updatedTimeStamp());
      } else {
        isDone = CarbonLoaderUtil
            .writeTableStatus(carbonLoadModel, loadMetadataDetails, overwriteTable, false,
                new ArrayList<>(), uuid);
      }
      if (!isDone) {
        String errorMessage = "Dataload failed due to failure in table status updation for"
            + " ${carbonLoadModel.getTableName}";
        LOGGER.error(errorMessage);
        throw new Exception(errorMessage);
      } else {
        MVManagerInSpark.disableMVOnTable(sparkSession,
            carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable(), overwriteTable);
      }
      CarbonDataRDDFactory
          .handlePostEvent(carbonLoadModel, operationContext, uuid, segmentFileName, true,
              loadMetadataDetails.getSegmentStatus());
    } finally {
      segmentLock.unlock();
    }
  }

  public void rollback() throws Exception {
    SegmentStatus segmentStatus = loadMetadataDetails.getSegmentStatus();
    if (segmentStatus == SegmentStatus.MARKED_FOR_DELETE
        || segmentStatus == SegmentStatus.LOAD_FAILURE) {
      return;
    }
    CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid);
    //TODO check the rollback scenario
    CarbonLoaderUtil
        .deleteSegmentForFailure(carbonLoadModel, Integer.parseInt(carbonLoadModel.getSegmentId()));
    // delete corresponding segment file from metadata
    String segmentFile =
        CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath()) + "/"
            + segmentFileName;
    FileFactory.deleteFile(segmentFile);
  }

  public String getTransactionSegment() {
    return carbonLoadModel.getSegmentId();
  }

  public String getTransactionTableName() {
    return carbonLoadModel.getDatabaseName() + "." + carbonLoadModel.getTableName();
  }

  public void recordUpdateDetails(long updateTime, Segment[] deletedSegments,
      boolean loadAsANewSegment) {
    if (tableModel != null) {
      tableModel.updatedTimeStamp_$eq(updateTime);
      tableModel.deletedSegments_$eq(deletedSegments);
      tableModel.loadAsNewSegment_$eq(loadAsANewSegment);
    }
  }
}
