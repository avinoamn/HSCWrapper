package github.avinoamn.HSCWrapper.utils

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseTestingUtility, StartMiniClusterOption, TableName}
import org.apache.spark.SparkContext

object HBaseTestUtils {
  val htu: HBaseTestingUtility = new HBaseTestingUtility()

  def createHBaseContext(sparkContext: SparkContext): Unit = {
    new HBaseContext(sparkContext, htu.getConfiguration)
  }

  def startMiniCluster(): Unit = {
    htu.startMiniZKCluster(1, 2181)
    htu.startMiniHBaseCluster(StartMiniClusterOption.builder.numMasters(1).numRegionServers(2).build)
  }

  def shutdownMiniCluster(): Unit = {
    htu.cleanupTestDir()
    htu.shutdownMiniHBaseCluster()
    htu.shutdownMiniZKCluster()
  }

  def createTable(name: String, cf: Array[String]): Unit = {
    htu.createTable(TableName.valueOf(name), cf)
  }

  def deleteTable(tableName: String): Unit = {
    deleteTable(TableName.valueOf(tableName))
  }

  def deleteTable(tableName: TableName): Unit = {
    htu.deleteTableIfAny(tableName)
  }

  def deleteAllTables(): Unit = {
    htu
      .getAdmin
      .listTableNames()
      .foreach(deleteTable)
  }
}
