package org.apache.carbondata.externalstreaming

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.command.management.CarbonAddExternalStreamingSegmentCommand
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog, HandOffOptions, HandoffHbaseSegmentCommand, SparkHBaseConf}
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Ignore
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hbase.HBaseConstants

@Ignore
class TestHBaseScdStreaming extends QueryTest with BeforeAndAfterAll {
  var htu: HBaseTestingUtility = _
  var loadTimestamp: Long = 0
  var hBaseConfPath: String = _

  val handoffCat =
    s"""{
       |"table":{"namespace":"default", "name":"SCD", "tableCoder":"Phoenix"},
       |"rowkey":"id",
       |"columns":{
       |"rowkey.id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"cf2.id":{"cf":"cf2", "col":"id", "type":"string"},
       |"cf2.name":{"cf":"cf2", "col":"name", "type":"string"},
       |"cf2.c_name":{"cf":"cf2", "col":"c_name", "type":"string"},
       |"cf2.quantity":{"cf":"cf2", "col":"quantity", "type":"int"},
       |"cf2.price":{"cf":"cf2", "col":"price", "type":"int"},
       |"cf2.operation_type":{"cf":"cf2", "col":"operation_type", "type":"String"}
       |}
       |}""".stripMargin

  val queryCat =
    s"""{
       |"table":{"namespace":"default", "name":"SCD", "tableCoder":"Phoenix"},
       |"rowkey":"id",
       |"columns":{
       |"rowkey.id":{"cf":"rowkey", "col":"id", "type":"string"},
       |"cf2.id":{"cf":"cf2", "col":"id", "type":"string"},
       |"cf2.name":{"cf":"cf2", "col":"name", "type":"string"},
       |"cf2.c_name":{"cf":"cf2", "col":"c_name", "type":"string"},
       |"cf2.quantity":{"cf":"cf2", "col":"quantity", "type":"int"},
       |"cf2.price":{"cf":"cf2", "col":"price", "type":"int"},
       |"cf2.operation_type":{"cf":"cf2", "col":"operation_type", "type":"String"}
       |}
       |}""".stripMargin


  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS scdhbaseCarbon")
    htu = new HBaseTestingUtility()
    htu.startMiniCluster(1)
    SparkHBaseConf.conf = htu.getConfiguration
    hBaseConfPath = s"$integrationPath/hbase/src/test/resources/hbase-site-local.xml"
    val shcExampleTableOption = Map(HBaseTableCatalog.tableCatalog -> handoffCat,
      HBaseTableCatalog.newTable -> "5", HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath)
    val frame = generateData(10)
    frame.write
      .options(shcExampleTableOption)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    CarbonProperties.getInstance()
      .addProperty(HBaseConstants.CARBON_HBASE_CONF_FILE_PATH, hBaseConfPath)
    sql(
      "create table scdhbaseCarbon(id String, name String, c_name string, quantity int, price " +
      "int, operation_type string) stored as carbondata TBLPROPERTIES" +
      "('custom.pruner' = 'org.apache.carbondata.hbase.segmentpruner.OpenTableSegmentPruner') ")
    var options = Map("format" -> "HBase")
    options = options + ("operation_type_column" -> "operation_type")
    options = options + ("insert_operation_value" -> "insert")
    options = options + ("update_operation_value" -> "update")
    options = options + ("delete_operation_value" -> "delete")
    options = options + ("defaultColumnFamily" -> "cf2")
    options = options + ("rowKeyColumnFamily" -> "rowkey")
    options = options + ("MinMaxColumns" -> "id")
    CarbonAddExternalStreamingSegmentCommand(Some("default"),
      "scdhbaseCarbon",
      queryCat,
      Some(handoffCat),
      options).processMetadata(
      sqlContext.sparkSession)
  }

  def withCatalog(cat: String, timestamp: Long): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  test("test handoff segment") {
    val prevRows = sql("select * from scdhbaseCarbon").collect()
    val columns = new Array[String](1)
    columns(0) = "id"
    HandoffHbaseSegmentCommand(None, "scdhbaseCarbon", Some(columns), new HandOffOptions().setDeleteRows(false).setGraceTimeInMillis(0)).run(sqlContext
      .sparkSession)
    checkAnswer(sql("select * from scdhbaseCarbon"), prevRows)
    checkAnswer(sql("select * from scdhbaseCarbon where segmentid(1)"), prevRows)
    val frame = generateFullCDC(10, 2, 2, 1, 2)
    val l = System.currentTimeMillis()
    val shcExampleTableOption = Map(HBaseTableCatalog.tableCatalog -> handoffCat,
      HBaseTableCatalog.newTable -> "5",
      HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath,
      HBaseRelation.TIMESTAMP -> l.toString)
    frame.write
      .options(shcExampleTableOption)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    HandoffHbaseSegmentCommand(None, "scdhbaseCarbon", Some(columns), new HandOffOptions().setDeleteRows(false).setGraceTimeInMillis(0).setFilterJoinPushLimit(0)).run(
      sqlContext
        .sparkSession)
    assert(sql("select count(*) from scdhbaseCarbon where excludesegmentId(4)").collectAsList()
             .get(0)
             .get(0) == 18)
    assert(
      sql("select * from scdhbaseCarbon where excludesegmentId(4) and id='id3'").collectAsList()
        .size() == 1)
    assert(
      sql("select * from scdhbaseCarbon where excludesegmentId(4) and id='id4'").collectAsList()
        .size() == 1)
    assert(
      sql("select * from scdhbaseCarbon where excludesegmentId(4) and operation_type='update'").collectAsList()

        .size() == 2)
    assert(
      sql("select * from scdhbaseCarbon where excludesegmentId(4) and operation_type='insert'").collectAsList()
        .size() == 16)
  }

  def generateData(numOrders: Int = 10): DataFrame = {
    import sqlContext.implicits._
    sqlContext.sparkContext
      .parallelize(1 to numOrders, 4)
      .map { x => ("id" + x, "id" + x, s"order$x", s"customer$x", x * 10, x * 75, "insert")
      }
      .toDF("rowkey.id",
        "cf2.id",
        "cf2.name",
        "cf2.c_name",
        "cf2.quantity",
        "cf2.price",
        "cf2.operation_type")
  }

  def generateFullCDC(
      numOrders: Int,
      numUpdatedOrders: Int,
      newState: Int,
      oldState: Int,
      numNewOrders: Int
  ): DataFrame = {
    import sqlContext.implicits._
    val ds1 = sqlContext.sparkContext
      .parallelize(numNewOrders + 1 to (numOrders), 4)
      .map { x =>
        if (x <= numNewOrders + numUpdatedOrders) {
          ("id" + x, "id" + x, s"order$x", s"customer$x", x * 10, x * 75, "update")
        } else {
          ("id" + x, "id" + x, s"order$x", s"customer$x", x * 10, x * 75, "insert")
        }
      }
      .toDF("rowkey.id",
        "cf2.id",
        "cf2.name",
        "cf2.c_name",
        "cf2.quantity",
        "cf2.price",
        "cf2.operation_type")
    val ds2 = sqlContext.sparkContext.parallelize(1 to numNewOrders, 4)
      .map { x => ("newid" + x, "newid" + x, s"order$x", s"customer$x", x * 10, x * 75, "insert")
      }.toDS().toDF()
    ds1.union(ds2)
  }

  override def afterAll(): Unit = {
    //    sql("DROP TABLE IF EXISTS scdhbaseCarbon")
    htu.shutdownMiniCluster()
  }

  class HBaseTestingUtilityCus(path: String) extends HBaseTestingUtility {
    override def getRandomDir: Path = {
      println("HHHHH : " + path)
      new Path(path)
    }

    @throws[Exception]
    override def shutdownMiniCluster(): Unit = {
      println("Shutting down minicluster")
      this.shutdownMiniHBaseCluster()
      this.shutdownMiniDFSCluster()
      this.shutdownMiniZKCluster()
//      this.cleanupTestDir
      println("Minicluster is down")
    }
  }

}
