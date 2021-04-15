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

package org.apache.carbondata.core.scan.filter.executer;

import java.io.IOException;
import java.util.BitSet;

import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;

public class OrFilterExecutorImpl implements FilterExecutor {

  private FilterExecutor leftExecutor;
  private FilterExecutor rightExecutor;

  public OrFilterExecutorImpl(FilterExecutor leftExecutor, FilterExecutor rightExecutor) {
    this.leftExecutor = leftExecutor;
    this.rightExecutor = rightExecutor;
  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws FilterUnsupportedException, IOException {
    BitSetGroup leftFilters = leftExecutor.applyFilter(rawBlockletColumnChunks, false);
    BitSetGroup rightFilters = rightExecutor.applyFilter(rawBlockletColumnChunks, false);
    leftFilters.or(rightFilters);
    rawBlockletColumnChunks.setBitSetGroup(leftFilters);
    return leftFilters;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks)
      throws FilterUnsupportedException, IOException {
    BitSet leftFilters = leftExecutor.prunePages(rawBlockletColumnChunks);
    BitSet rightFilters = rightExecutor.prunePages(rawBlockletColumnChunks);
    leftFilters.or(rightFilters);
    return leftFilters;
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax)
      throws FilterUnsupportedException, IOException {
    return leftExecutor.applyFilter(value, dimOrdinalMax) ||
        rightExecutor.applyFilter(value, dimOrdinalMax);
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    BitSet leftFilters = leftExecutor.isScanRequired(blockMaxValue, blockMinValue, isMinMaxSet);
    BitSet rightFilters = rightExecutor.isScanRequired(blockMaxValue, blockMinValue, isMinMaxSet);
    leftFilters.or(rightFilters);
    return leftFilters;
  }

  @Override
  public BitSet isScanRequired(MinMaxPruneMetadata minMaxPruneMetadata) {
    BitSet leftFilters = leftExecutor.isScanRequired(minMaxPruneMetadata);
    BitSet rightFilters = rightExecutor.isScanRequired(minMaxPruneMetadata);
    leftFilters.or(rightFilters);
    return leftFilters;
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) throws IOException {
    leftExecutor.readColumnChunks(rawBlockletColumnChunks);
    rightExecutor.readColumnChunks(rawBlockletColumnChunks);
  }
}
