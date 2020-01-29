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

import java.util.List;
import java.util.Objects;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

/**
 * Below class will be used o store the details
 * about the transaction currently running on a table
 */
public class TransactionDetail {
  /**
   * running transaction id
   */
  private String transactionId;
  /**
   * table on which transaction is running
   */
  private CarbonTable carbonTable;

  /**
   * list of all running transaction started before
   * starting this transaction
   */
  private List<TransactionDetail> transactionDetailList;

  /**
   * table status file name created during this transaction
   */
  private String tableStatusPath;

  public TransactionDetail(String transactionId, CarbonTable carbonTable,
      List<TransactionDetail> transactionDetailList, String tableStatusPath) {
    this.transactionId = transactionId;
    this.carbonTable = carbonTable;
    this.transactionDetailList = transactionDetailList;
    this.tableStatusPath = tableStatusPath;
  }

  public String getTransactionId() {
    return transactionId;
  }

  public CarbonTable getCarbonTable() {
    return carbonTable;
  }

  public List<TransactionDetail> getTransactionDetailList() {
    return transactionDetailList;
  }

  public String getTableStatusPath() {
    return tableStatusPath;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TransactionDetail that = (TransactionDetail) o;
    return Objects.equals(transactionId, that.transactionId);
  }

  @Override public int hashCode() {

    return Objects.hash(transactionId);
  }
}
