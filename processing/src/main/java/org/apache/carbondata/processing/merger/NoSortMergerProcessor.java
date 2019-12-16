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

package org.apache.carbondata.processing.merger;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.processing.exception.SliceMergerException;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerColumnar;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.CarbonFactHandler;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.log4j.Logger;

public class NoSortMergerProcessor extends AbstractResultProcessor {

  private CarbonFactHandler dataHandler;
  private SegmentProperties segprop;
  private CarbonLoadModel loadModel;
  private PartitionSpec partitionSpec;

  CarbonColumn[] noDicAndComplexColumns;

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(RowResultMergerProcessor.class.getName());

  public NoSortMergerProcessor(String tableName, SegmentProperties segProp,
      String[] tempStoreLocation, CarbonLoadModel loadModel, CompactionType compactionType,
      PartitionSpec partitionSpec) throws IOException {
    this.segprop = segProp;
    this.partitionSpec = partitionSpec;
    this.loadModel = loadModel;
    CarbonDataProcessorUtil.createLocations(tempStoreLocation);

    String carbonStoreLocation;
    if (partitionSpec != null) {
      carbonStoreLocation =
          partitionSpec.getLocation().toString() + CarbonCommonConstants.FILE_SEPARATOR + loadModel
              .getFactTimeStamp() + ".tmp";
    } else {
      carbonStoreLocation = CarbonDataProcessorUtil
          .createCarbonStoreLocation(loadModel.getCarbonDataLoadSchema().getCarbonTable(),
              loadModel.getSegmentId());
    }
    CarbonFactDataHandlerModel carbonFactDataHandlerModel = CarbonFactDataHandlerModel
        .getCarbonFactDataHandlerModel(loadModel,
            loadModel.getCarbonDataLoadSchema().getCarbonTable(), segProp, tableName,
            tempStoreLocation, carbonStoreLocation);
    setDataFileAttributesInModel(loadModel, compactionType, carbonFactDataHandlerModel);
    carbonFactDataHandlerModel.setCompactionFlow(true);
    carbonFactDataHandlerModel.setSegmentId(loadModel.getSegmentId());
    carbonFactDataHandlerModel.setBucketId(loadModel.getBucketId());
    this.noDicAndComplexColumns = carbonFactDataHandlerModel.getNoDictAndComplexColumns();
    dataHandler = new CarbonFactDataHandlerColumnar(carbonFactDataHandlerModel);
  }

  @Override
  public boolean execute(List<RawResultIterator> unsortedResultIteratorList,
      List<RawResultIterator> sortedResultIteratorList) throws Exception {
    boolean isDataPresent = false;
    boolean mergeStatus;
    try {
      for (RawResultIterator rawResultIterator : unsortedResultIteratorList) {
        while (rawResultIterator.hasNext()) {
          Object[] next = rawResultIterator.next();
          if (null == next) {
            rawResultIterator.close();
            continue;
          }
          if (!isDataPresent) {
            dataHandler.initialise();
            isDataPresent = true;
          }
          // get the mdkey
          addRow(next);
        }
        rawResultIterator.close();
      }
      if (isDataPresent) {
        this.dataHandler.finish();
      }
      mergeStatus = true;
    } catch (Exception e) {
      LOGGER.error(e.getLocalizedMessage(), e);
      throw e;
    } finally {
      try {
        if (isDataPresent) {
          this.dataHandler.closeHandler();
        }
        if (partitionSpec != null) {
          SegmentFileStore.writeSegmentFile(loadModel.getTablePath(), loadModel.getTaskNo(),
              partitionSpec.getLocation().toString(), loadModel.getFactTimeStamp() + "",
              partitionSpec.getPartitions());
        }
      } catch (CarbonDataWriterException | IOException e) {
        throw e;
      }
    }
    return mergeStatus;
  }

  /**
   * Below method will be used to add sorted row
   *
   * @throws SliceMergerException
   */
  private void addRow(Object[] carbonTuple) throws SliceMergerException {
    CarbonRow row = WriteStepRowUtil.fromMergerRow(carbonTuple, segprop, noDicAndComplexColumns);
    try {
      this.dataHandler.addDataToStore(row);
    } catch (CarbonDataWriterException e) {
      throw new SliceMergerException("Problem in merging the slice", e);
    }
  }

  @Override
  public void close() {
    // close data handler
    if (null != dataHandler) {
      dataHandler.closeHandler();
    }
  }
}
