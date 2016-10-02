/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.carbon.metadata.blocklet;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;

/**
 * class to store the information about the blocklet
 */
public class BlockletInfo implements Serializable {

  /**
   * serialization id
   */
  private static final long serialVersionUID = 1873135459695635381L;

  /**
   * Number of rows in this blocklet
   */
  private int numberOfRows;

  /**
   * to store the index like min max and start and end key of each column of the blocklet
   */
  private BlockletIndex blockletIndex;

  private List<Long> dimensionDataChunkOffsets;

  private List<Long> measureDataChunkOffsets;

  /**
   * @return the numberOfRows
   */
  public int getNumberOfRows() {
    return numberOfRows;
  }

  /**
   * @param numberOfRows the numberOfRows to set
   */
  public void setNumberOfRows(int numberOfRows) {
    this.numberOfRows = numberOfRows;
  }

  /**
   * @return the blockletIndex
   */
  public BlockletIndex getBlockletIndex() {
    return blockletIndex;
  }

  /**
   * @param blockletIndex the blockletIndex to set
   */
  public void setBlockletIndex(BlockletIndex blockletIndex) {
    this.blockletIndex = blockletIndex;
  }

  public List<Long> getDimensionDataChunkOffsets() {
    return dimensionDataChunkOffsets;
  }

  public void setDimensionDataChunkOffsets(List<Long> dimensionDataChunkOffsets) {
    this.dimensionDataChunkOffsets = dimensionDataChunkOffsets;
  }

  public List<Long> getMeasureDataChunkOffsets() {
    return measureDataChunkOffsets;
  }

  public void setMeasureDataChunkOffsets(List<Long> measureDataChunkOffsets) {
    this.measureDataChunkOffsets = measureDataChunkOffsets;
  }
}
