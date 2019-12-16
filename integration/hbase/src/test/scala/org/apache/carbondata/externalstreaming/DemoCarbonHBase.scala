package org.apache.carbondata.externalstreaming

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.spark.sql.execution.command.management.CarbonAddExternalStreamingSegmentCommand
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog, HandOffOptions, HandoffHbaseSegmentCommand, SparkHBaseConf}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll


class DemoCarbonHBase extends QueryTest with BeforeAndAfterAll {

  var htu: HBaseTestingUtility = _
  var loadTimestamp:Long = 0
  var hBaseConfPath:String = _
  val cat =
    s"""{
       |"table":{"namespace":"default", "name":"streamingtable", "tableCoder":"PrimitiveType"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
       |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
       |"col2":{"cf":"cf2", "col":"col2", "type":"int"}
       |}
       |}""".stripMargin

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS historytable")
    htu = new HBaseTestingUtility()
    htu.startMiniCluster(1)
    SparkHBaseConf.conf = htu.getConfiguration
    hBaseConfPath = s"$integrationPath/hbase/src/test/resources/hbase-site-local.xml"
  }

  test("Carbon HBase Demo") {
    /**
     * Writing data to habse
     */
    writeDataToHBase(0, 10)
    /**
     * Create carbon table
     */
    createCarbonHistoryTable()
    /**
     * Select records from newly created carbon table
     */
    selectFromCarbonTable()

    /**
     * register hbase to carbon
     */
    registerHBaseTableToCarbonWithSchema()

    /**
     * Select records from newly created carbon table
     */
    selectFromCarbonTable()
    /**
     * hand off
     */
    handOff()

    /**
     * Select records from newly created carbon table
     */
    selectFromCarbonTable()

    /**
     * Writing data to habse
     */
    writeDataToHBase(10, 20)
    /**
     * Select records from newly created carbon table
     */
    selectFromCarbonTable()
  }

  def writeDataToHBase(start:Int, end:Int): Unit = {
    val data = (start until end).map { i =>
      IntKeyRecord(i)
    }
    import sqlContext.implicits._
    val shcExampleTableOption = Map(HBaseTableCatalog.tableCatalog -> cat,
      HBaseTableCatalog.newTable -> "5", HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath)
    sqlContext.sparkContext.parallelize(data).toDF.write.options(shcExampleTableOption)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }

  def createCarbonHistoryTable(): Unit = {
    sql("create table historytable (col0 int, col1 String, col2 int) stored as carbondata")
  }

  def selectFromCarbonTable() : Unit = {
    sql("select * from historytable").show()
  }

  def registerHBaseTableToCarbonWithSchema(): Unit = {
    var options = Map("format" -> "HBase")
    CarbonAddExternalStreamingSegmentCommand(Some("default"), "historytable", cat, None, options).processMetadata(
      sqlContext.sparkSession)
  }

  def handOff(): Unit = {
    HandoffHbaseSegmentCommand(None, "historytable", Option.empty, new HandOffOptions().setGraceTimeInMillis(0)).run(sqlContext.sparkSession)
  }

  override def afterAll(): Unit = {
    htu.shutdownMiniCluster()
  }
}
