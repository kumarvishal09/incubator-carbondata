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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.transaction.TransactionAction;
import org.apache.carbondata.events.OperationContext;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class CompactionTransactionAction implements TransactionAction {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CompactionTransactionAction.class.getName());
  private SparkSession sparkSession;

  private CarbonLoadModel carbonLoadModel;

  private OperationContext operationContext;

  public CompactionTransactionAction(SparkSession sparkSession, CarbonLoadModel carbonLoadModel,
      OperationContext operationContext) {
    this.sparkSession = sparkSession;
    this.carbonLoadModel = carbonLoadModel;
    this.operationContext = operationContext;
  }

  @Override
  public void commit() throws Exception {
    CarbonDataRDDFactory.runCompaction(sparkSession, carbonLoadModel, operationContext);
  }
}
