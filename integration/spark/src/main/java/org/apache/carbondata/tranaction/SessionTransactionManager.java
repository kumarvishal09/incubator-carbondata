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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.transaction.TransactionAction;
import org.apache.carbondata.core.transaction.TransactionActionType;
import org.apache.carbondata.core.transaction.TransactionHandler;

import org.apache.spark.sql.SparkSession;

public class SessionTransactionManager implements TransactionHandler<SparkSession> {

  private Map<String, TransactionObj> transactionIdToSessionMap;

  private Map<TransactionObj, String> sessionToTransactionIdMap;

  private Map<String, Queue<TransactionAction>> openTransactionActions;

  private Map<String, Queue<TransactionAction>> closedTransactionActions;

  private Map<String, Queue<TransactionAction>> perfTransactionAction;

  private Map<TransactionObj, Set<String>> transactionalTableNames;

  private Map<String, Map<String, String>> transactionSegmentMap;

  public SessionTransactionManager() {
    this.transactionIdToSessionMap = new ConcurrentHashMap<>();
    this.sessionToTransactionIdMap = new ConcurrentHashMap<>();
    this.openTransactionActions = new ConcurrentHashMap<>();
    this.closedTransactionActions = new ConcurrentHashMap<>();
    this.perfTransactionAction = new ConcurrentHashMap<>();
    this.transactionalTableNames = new HashMap<>();
    this.transactionSegmentMap = new HashMap<>();
  }

  @Override
  public String startTransaction(SparkSession sparkSession) {
    return startTransaction(sparkSession, "");
  }

  @Override
  public String startTransaction(SparkSession sparkSession, String tableName) {
    TransactionObj transactionObj = new TransactionObj(sparkSession, tableName);
    String transactionId = sessionToTransactionIdMap.get(transactionObj);
    if (null != transactionId) {
      throw new RuntimeException(
          "Single transaction is supported on Session." + " Commit old transaction first.");
    }
    transactionId = UUID.randomUUID().toString();
    transactionIdToSessionMap.put(transactionId, transactionObj);
    sessionToTransactionIdMap.put(transactionObj, transactionId);
    return transactionId;
  }

  @Override
  public void commitTransaction(String transactionId) {
    if (null == this.transactionIdToSessionMap.get(transactionId)) {
      throw new RuntimeException("No current transaction is running on this session");
    }
    Queue<TransactionAction> openTransactions = openTransactionActions.get(transactionId);
    Queue<TransactionAction> closeTransactions = closedTransactionActions.get(transactionId);
    if (null == closeTransactions) {
      closeTransactions = new ArrayDeque<>();
      closedTransactionActions.put(transactionId, closeTransactions);
    }
    if (null != openTransactions) {
      while (!openTransactions.isEmpty()) {
        TransactionAction poll = openTransactions.poll();
        try {
          poll.commit();
          closeTransactions.add(poll);
        } catch (Exception e) {
          closeTransactions.add(poll);
          throw new RuntimeException(e);
        }
      }
    }
    Queue<TransactionAction> perfTransactionActions = perfTransactionAction.get(transactionId);
    if (null != perfTransactionActions) {
      while (!perfTransactionActions.isEmpty()) {
        TransactionAction poll = perfTransactionActions.poll();
        try {
          poll.commit();
        } catch (Exception e) {
          //IGNORE
        }
      }
    }
    openTransactionActions.remove(transactionId);
    closedTransactionActions.remove(transactionId);
    sessionToTransactionIdMap.remove(transactionIdToSessionMap.remove(transactionId));
    transactionSegmentMap.remove(transactionId);
  }

  @Override
  public void rollbackTransaction(String transactionId) {
    if (null == this.transactionIdToSessionMap.get(transactionId)) {
      throw new RuntimeException("No current transaction is running on this session");
    }
    Queue<TransactionAction> openTransactions = openTransactionActions.get(transactionId);
    Queue<TransactionAction> closeTransactions = closedTransactionActions.get(transactionId);
    if (null != closeTransactions) {
      while (!closeTransactions.isEmpty()) {
        try {
          closeTransactions.poll().rollback();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (openTransactions != null) {
      while (!openTransactions.isEmpty()) {
        try {
          openTransactions.poll().rollback();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    openTransactionActions.remove(transactionId);
    closedTransactionActions.remove(transactionId);
    perfTransactionAction.remove(transactionId);
    sessionToTransactionIdMap.remove(transactionIdToSessionMap.remove(transactionId));
    transactionSegmentMap.remove(transactionId);
  }

  @Override
  public void recordTransactionAction(String transactionId, TransactionAction transactionAction,
      TransactionActionType transactionActionType) {
    if (null == this.transactionIdToSessionMap.get(transactionId)) {
      throw new RuntimeException("No current transaction is running on this session");
    }
    if (transactionAction instanceof PrePrimingAction) {
      return;
    }
    if (transactionActionType == TransactionActionType.COMMIT_SCOPE) {
      Queue<TransactionAction> transactionActions = openTransactionActions.get(transactionId);
      if (null == transactionActions) {
        transactionActions = new ArrayDeque<>();
        openTransactionActions.put(transactionId, transactionActions);
      }
      transactionActions.add(transactionAction);
    } else {
      Queue<TransactionAction> transactionActions = perfTransactionAction.get(transactionId);
      if (null == transactionActions) {
        transactionActions = new ArrayDeque<>();
        perfTransactionAction.put(transactionId, transactionActions);
      }
      transactionActions.add(transactionAction);
    }
  }

  @Override
  public String getTransactionId(SparkSession session, String tableUniqueName) {
    TransactionObj transactionObj = new TransactionObj(session, "");
    if (null == this.sessionToTransactionIdMap.get(transactionObj)) {
      transactionObj = new TransactionObj(session, tableUniqueName);
      if(null == this.sessionToTransactionIdMap.get(transactionObj)) {
        return null;
      }
    }
    Set<String> tableList = transactionalTableNames.get(transactionObj);
    if (null != tableList && tableList.contains(tableUniqueName.toLowerCase(Locale.getDefault()))) {
      return null;
    }
    return sessionToTransactionIdMap.get(transactionObj);
  }

  @Override
  public String getTransactionId(SparkSession session) {
    TransactionObj transactionObj = new TransactionObj(session, "");
    return sessionToTransactionIdMap.get(transactionObj);
  }

  @Override
  public TransactionHandler getTransactionManager() {
    return this;
  }

  @Override
  public void unRegisterTableForTransaction(SparkSession sparkSession, String tableListString) {
    TransactionObj transactionObj = new TransactionObj(sparkSession, "");
    String transactionId = sessionToTransactionIdMap.get(transactionObj);
    if (null == transactionId) {
      throw new RuntimeException("No current transaction is running on this session");
    }
    Set<String> tableNames =
        transactionalTableNames.computeIfAbsent(transactionObj, k -> new HashSet<>());
    String[] split = tableListString.split(",");
    for (String str : split) {
      tableNames.add(str.trim().toLowerCase(Locale.getDefault()));
    }
  }

  @Override
  public String getAndSetCurrentTransactionSegment(String transactionId, String tableNameString) {
    if (null == this.transactionIdToSessionMap.get(transactionId)) {
      throw new RuntimeException("No current transaction is running on this session");
    }
    Map<String, String> transactionSegment =
        this.transactionSegmentMap.computeIfAbsent(transactionId, k -> new HashMap<>());
    String currentTransactionSegment = transactionSegment.get(tableNameString);
    if (null == currentTransactionSegment) {
      Queue<TransactionAction> transactionActions = openTransactionActions.get(transactionId);
      if (null != transactionActions) {
        for (TransactionAction transactionAction : transactionActions) {
          if (transactionAction instanceof LoadTransactionActions) {
            if (transactionAction.getTransactionTableName().equalsIgnoreCase(tableNameString)) {
              transactionSegment.put(tableNameString, transactionAction.getTransactionSegment());
              return transactionAction.getTransactionSegment();
            }
          }
        }
      }
    }
    return currentTransactionSegment;
  }

  @Override
  public String getCurrentTransactionSegment(String transactionId, String tableNameString) {
    Map<String, String> tableToCurrentSegment = transactionSegmentMap.get(transactionId);
    if (null != tableToCurrentSegment) {
      return tableToCurrentSegment.get(tableNameString);
    }
    return "*";
  }

  @Override
  public void recordUpdateDetails(String transactionId, String fullTableName, long updateTime,
      Segment[] deletedSegments, boolean loadAsANewSegment) {
    if (Objects.isNull(this.transactionIdToSessionMap.get(transactionId))) {
      throw new RuntimeException("No current transaction is running on this session");
    }
    Queue<TransactionAction> transactionActions = openTransactionActions.get(transactionId);
    if (null != transactionActions) {
      for (TransactionAction transactionAction : transactionActions) {
        if (transactionAction.getTransactionTableName().equals(fullTableName)) {
          transactionAction.recordUpdateDetails(updateTime, deletedSegments, loadAsANewSegment);
        }
      }
    }
  }

  private static class TransactionObj {
    private SparkSession sparkSession;
    private String tableName;
    private TransactionObj(SparkSession sparkSession, String tableName) {
      this.sparkSession = sparkSession;
      this.tableName = tableName;
    }

    public SparkSession getSparkSession() {
      return sparkSession;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TransactionObj that = (TransactionObj) o;
      return sparkSession.equals(that.sparkSession) && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sparkSession, tableName);
    }
  }
}
