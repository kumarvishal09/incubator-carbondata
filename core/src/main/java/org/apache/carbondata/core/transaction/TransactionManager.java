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

package org.apache.carbondata.core.transaction;

import org.apache.carbondata.core.datamap.Segment;

public class TransactionManager implements TransactionHandler<Object> {

  private static final TransactionManager INSTANCE = new TransactionManager();

  private TransactionHandler transactionHandler;

  private TransactionManager() {
    try {
      Class classDefinition =
          Class.forName("org.apache.carbondata.tranaction.SessionTransactionManager");
      transactionHandler = (TransactionHandler) classDefinition.newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  public static TransactionHandler getInstance() {
    return INSTANCE;
  }

  @Override
  public String startTransaction(Object transactionObj) {
    if (null == transactionHandler) {
      throw new RuntimeException("Failed to load Transaction manager class");
    }
    return transactionHandler.startTransaction(transactionObj);
  }

  @Override
  public String startTransaction(Object transactionObj, String tableName) {
    if (null == transactionHandler) {
      throw new RuntimeException("Failed to load Transaction manager class");
    }
    return transactionHandler.startTransaction(transactionObj, tableName);
  }

  @Override
  public void commitTransaction(String transactionId) {
    this.transactionHandler.commitTransaction(transactionId);
  }

  @Override
  public void rollbackTransaction(String transactionId) {
    this.transactionHandler.rollbackTransaction(transactionId);
  }

  @Override
  public void recordTransactionAction(String transactionId, TransactionAction transactionAction,
      TransactionActionType transactionActionType) {
    this.transactionHandler
        .recordTransactionAction(transactionId, transactionAction, transactionActionType);
  }

  @Override
  public String getTransactionId(Object transactionObj, String tableUniqueName) {
    return this.transactionHandler.getTransactionId(transactionObj, tableUniqueName);
  }

  @Override
  public String getTransactionId(Object transactionObj) {
    return this.transactionHandler.getTransactionId(transactionObj);
  }

  @Override
  public TransactionHandler getTransactionManager() {
    return this.transactionHandler;
  }

  @Override
  public void unRegisterTableForTransaction(Object transactionObj, String tableNameString) {
    this.transactionHandler.unRegisterTableForTransaction(transactionObj, tableNameString);
  }

  @Override
  public String getAndSetCurrentTransactionSegment(String transactionId, String tableNameString) {
    return this.transactionHandler
        .getAndSetCurrentTransactionSegment(transactionId, tableNameString);
  }

  @Override
  public String getCurrentTransactionSegment(String transactionId, String tableNameString) {
    return this.transactionHandler.getCurrentTransactionSegment(transactionId, tableNameString);
  }

  @Override
  public void recordUpdateDetails(String transactionId, String fullTableName, long updateTime,
      Segment[] deletedSegments, boolean loadAsANewSegment) {
    this.transactionHandler
        .recordUpdateDetails(transactionId, fullTableName, updateTime, deletedSegments,
            loadAsANewSegment);
  }
}
