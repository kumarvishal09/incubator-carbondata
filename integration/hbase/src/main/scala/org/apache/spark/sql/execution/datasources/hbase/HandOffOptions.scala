package org.apache.spark.sql.execution.datasources.hbase

class HandOffOptions extends Serializable {

  private var graceTimeInMillis: Long = 1
  private var deleteRows: Boolean = true
  private var filterJoinPushLimit:Int = 1000

  def getGraceTimeInMillis = this.graceTimeInMillis

  def getDeleteRows = this.deleteRows

  def getFilterJoinPushLimit = this.filterJoinPushLimit

  def setGraceTimeInMillis(graceTimeInMillis: Long): HandOffOptions = {
    this.graceTimeInMillis = graceTimeInMillis
    this
  }

  def setDeleteRows(deleteRows: Boolean): HandOffOptions = {
    this.deleteRows = deleteRows
    this
  }

  def setFilterJoinPushLimit(filterJoinPushLimit: Int): HandOffOptions = {
    this.filterJoinPushLimit = filterJoinPushLimit
    this
  }
}

