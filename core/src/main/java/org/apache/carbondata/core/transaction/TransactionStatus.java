package org.apache.carbondata.core.transaction;

public enum TransactionStatus {
  OPEN,
  INPROGRESS,
  FAILED,
  CLOSED;
}
