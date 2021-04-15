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

import org.apache.carbondata.core.index.Segment;

/**
 * Interface for doing any operation based on transaction.
 * so operation will be succeed only if it is committed.
 *
 * @param <T> transaction can be done on any level
 *            carbonTable, session.
 */
public interface TransactionHandler<T> {

  /**
   * interface for starting any transaction
   *
   * @param transactionObj object on which transaction is going on.
   *                       it can be a carbontable object, session object or any other
   * @return transaction id
   */
  String startTransaction(T transactionObj);

  /**
   * interface for starting any transaction
   *
   * @param transactionObj object on which transaction is going on.
   *                       it can be a carbontable object, session object or any other
   * @return transaction id
   */
  String startTransaction(T transactionObj, String tableName);

  /**
   * used for committing the transaction for committing id
   *
   * @param transactionId
   */
  void commitTransaction(String transactionId);

  /**
   * rollback if any failures while committing the transaction
   *
   * @param transactionId
   */
  void rollbackTransaction(String transactionId);

  void recordTransactionAction(String transactionId, TransactionAction transactionAction,
      TransactionActionType transactionActionType);

  String getTransactionId(T transactionObj, String tableUniqueName);

  String getTransactionId(T transactionObj);

  TransactionHandler getTransactionManager();

  void unRegisterTableForTransaction(T transactionObj, String tableNameString);

  String getAndSetCurrentTransactionSegment(String transactionId, String tableNameString);

  String getCurrentTransactionSegment(String transactionId, String tableNameString);

  void recordUpdateDetails(String transactionId, String fullTableName, long updateTime,
      Segment[] deletedSegments, boolean loadAsANewSegment);
}
