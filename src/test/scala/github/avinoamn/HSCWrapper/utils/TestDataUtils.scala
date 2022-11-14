package github.avinoamn.HSCWrapper.utils

import github.avinoamn.HSCWrapper.HSCWrapper.{read, write}
import github.avinoamn.HSCWrapper.models.{HBColumn, HBTable}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import TestConsts.{t1TableName, t2TableName, t3TableName, timestamp}

case class StringKeyRecord(
  rowkey: String,
  a: String,
  b: String,
  i: Int,
  z: String)

case class IntKeyRecord(
  rowkey: Int,
  a: String,
  b: String,
  i: Int,
  z: String)

case class TypesRecord(
  rowkey: String,
  `c:binary`: Array[Byte],
  `c:boolean`: Boolean,
  `c:byte`: Byte,
  `c:short`: Short,
  `c:int`: Int,
  `c:long`: Long,
  `c:float`: Float,
  `c:double`: Double,
  `c:date`: Long,
  `c:timestamp`: Long,
  `c:string`: String)

object TestDataUtils {
  def writeTestDataToHBase()(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val t1Records: Seq[StringKeyRecord] = Seq(
      StringKeyRecord("get1", "foo1", "1", 1, null),
      StringKeyRecord("get2", "foo2", "4", 4, "FOO"),
      StringKeyRecord("get3", "foo3", "8", 8, null),
      StringKeyRecord("get4", "foo4", "10", 10, "BAR"),
      StringKeyRecord("get5", "foo5", "8", 8, null)
    )
    val t1InputDS = spark.sparkContext.parallelize(t1Records).toDS()
    write(t1InputDS, t1TableName, Array("rowkey", "c:a", "c:b", "c:i", "c:z"))

    val t2Records: Seq[IntKeyRecord] = Seq(
      IntKeyRecord(1, "foo1", "1", 1, null),
      IntKeyRecord(2, "foo2", "4", 4, "FOO"),
      IntKeyRecord(3, "foo3", "8", 8, null),
      IntKeyRecord(4, "foo4", "10", 10, "BAR"),
      IntKeyRecord(5, "foo5", "8", 8, null)
    )
    val t2InputDS = spark.sparkContext.parallelize(t2Records).toDS()
    write(t2InputDS, t2TableName, Array("rowkey", "c:a", "c:b", "c:i", "c:z"))

    val t3Records: Seq[TypesRecord] = Seq(
      TypesRecord(
        "row",
        Array(1.toByte, 2.toByte, 3.toByte),
        true,
        127.toByte,
        32767.toShort,
        1000000,
        10000000000L,
        0.5f,
        0.125,
        timestamp,
        timestamp,
        "string"
      )
    )
    val t3InputDS = spark.sparkContext.parallelize(t3Records).toDS()
    write(t3InputDS, t3TableName, Array("rowkey", "c:binary", "c:boolean", "c:byte", "c:short", "c:int", "c:long", "c:float", "c:double", "c:date", "c:timestamp", "c:string"))
  }

  def readTestDatafromHbaseAndRegisterTempTables()(implicit spark: SparkSession): Unit = {
    val t1HBTable = HBTable(t1TableName)
    val t1HBColumns = Array(
      HBColumn("rowkey", "KEY_FIELD", StringType),
      HBColumn("c:a", "A_FIELD", StringType),
      HBColumn("c:b", "B_FIELD", StringType))

    val t1DF = read(t1HBTable, t1HBColumns)
    t1DF.registerTempTable("hbaseTable1")

    val t2HBTable = HBTable(t2TableName)
    val t2HBColumns = Array(
      HBColumn("rowkey", "KEY_FIELD", IntegerType),
      HBColumn("c:a", "A_FIELD", StringType),
      HBColumn("c:b", "B_FIELD", StringType))

    val t2DF = read(t2HBTable, t2HBColumns)
    t2DF.registerTempTable("hbaseTable2")

    val t3HBTable = HBTable(t3TableName)
    val t3HBColumns = Array(
      HBColumn("rowkey", "KEY_FIELD", StringType),
      HBColumn("c:binary", "BINARY_FIELD", BinaryType),
      HBColumn("c:boolean", "BOOLEAN_FIELD", BooleanType),
      HBColumn("c:byte", "BYTE_FIELD", ByteType),
      HBColumn("c:short", "SHORT_FIELD", ShortType),
      HBColumn("c:int", "INT_FIELD", IntegerType),
      HBColumn("c:long", "LONG_FIELD", LongType),
      HBColumn("c:float", "FLOAT_FIELD", FloatType),
      HBColumn("c:double", "DOUBLE_FIELD", DoubleType),
      HBColumn("c:date", "DATE_FIELD", DateType),
      HBColumn("c:timestamp", "TIMESTAMP_FIELD", TimestampType),
      HBColumn("c:string", "STRING_FIELD", StringType))

    val t3DF = read(t3HBTable, t3HBColumns)
    t3DF.registerTempTable("hbaseTestMapping")
  }
}
