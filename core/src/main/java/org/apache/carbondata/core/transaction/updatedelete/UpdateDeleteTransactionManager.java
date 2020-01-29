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
package org.apache.carbondata.core.transaction.updatedelete;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.transaction.TransactionDetail;
import org.apache.carbondata.core.transaction.TransactionFailedException;
import org.apache.carbondata.core.transaction.TransactionManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Below class will be used
 */
public class UpdateDeleteTransactionManager implements TransactionManager {

  public static final UpdateDeleteTransactionManager INSTANCE =
      new UpdateDeleteTransactionManager();

  /**
   * Map for list of transaction running on table
   */
  private Map<CarbonTable, Map<String, TransactionDetail>> tableTransactionMap;

  private UpdateDeleteTransactionManager() {
    this.tableTransactionMap = new ConcurrentHashMap<>();
  }

  @Override public TransactionDetail openTransaction(CarbonTable carbonTable) {
    TransactionDetail transactionDetail = null;
    synchronized (carbonTable) {
      String uuid = UUID.randomUUID().toString();
      final String tableStatusFilePathWithUUID = CarbonTablePath
          .getTableStatusFilePathWithUUID(carbonTable.getAbsoluteTableIdentifier().getTablePath(),
              uuid);
      final Map<String, TransactionDetail> allCurrentTransactionOnTable =
          getAllCurrentTransactionOnTable(carbonTable);
      List<TransactionDetail> transactionDetails = new ArrayList<>();
      final Iterator<Map.Entry<String, TransactionDetail>> iterator =
          allCurrentTransactionOnTable.entrySet().iterator();
      while(iterator.hasNext()) {
        transactionDetails.add(iterator.next().getValue());
      }
      transactionDetail =
          new TransactionDetail(uuid, carbonTable, transactionDetails,
              tableStatusFilePathWithUUID);
    }
    return transactionDetail;
  }

  private Map<String, TransactionDetail> getAllCurrentTransactionOnTable(CarbonTable carbonTable) {
    return tableTransactionMap.get(carbonTable);
  }

  /**
   * Below method will be used to commit the table status file for the transaction.
   * Committing here means it check for list of carbon data file which is updated
   * in the earlier transaction, if same carbondata file is being updated it will
   * fail the current transaction. if it is different file then it will acquire the lock on table
   * update status file and then it will update the details
   * @param transactionDetail transaction details for which need to commit
   * @throws TransactionFailedException
   */
  @Override public void commitTransaction(CarbonTable table, TransactionDetail transactionDetail) throws
      TransactionFailedException {
    SegmentUpdateStatusManager segmentUpdateStatusManager = new SegmentUpdateStatusManager(table);
    final SegmentUpdateDetails[] updateStatusDetails =
        segmentUpdateStatusManager.getUpdateStatusDetails();

  }
}
