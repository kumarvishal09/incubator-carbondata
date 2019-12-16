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

public class MinMaxPruneMetadata {
  private byte[][] blockMaxValue;
  private byte[][] blockMinValue;
  private boolean[] isMinMaxSet;
  private boolean maxValidationRequired;

  public MinMaxPruneMetadata(byte[][] blockMaxValue, byte[][] blockMinValue, boolean[] isMinMaxSet,
      boolean maxValidationRequired) {
    this.blockMaxValue = blockMaxValue;
    this.blockMinValue = blockMinValue;
    this.isMinMaxSet = isMinMaxSet;
    this.maxValidationRequired = maxValidationRequired;
  }

  public byte[][] getBlockMaxValue() {
    return blockMaxValue;
  }

  public byte[][] getBlockMinValue() {
    return blockMinValue;
  }

  public boolean[] getIsMinMaxSet() {
    return isMinMaxSet;
  }

  public boolean isMaxValidationRequired() {
    return maxValidationRequired;
  }
}
