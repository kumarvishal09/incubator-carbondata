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

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

public interface TransactionManager {

  /**
   * Below method will open and start the transaction, all the concrete implementation
   * will have its own open transaction implementation, for example:
   * in case of update and delete it will create one table status file based on transaction id
   * tablestatus_<transactionid>
   *
   * @return transaction details.
   */
  TransactionDetail openTransaction(CarbonTable carbonTable);

  /**
   * Below method will be used to commit the transaction.
   * Example: in case of update/delete it will commit by updating the
   * table status.
   *
   * @param transactionDetail transaction details for which need to commit
   */
  void commitTransaction(CarbonTable carbonTable, TransactionDetail transactionDetail)
      throws TransactionFailedException;
}
