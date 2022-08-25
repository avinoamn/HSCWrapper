package github.avinoamn.HSCWrapper.models

import github.avinoamn.HSCWrapper.utils.ColumnsUtils.{getHBColumnFamilyAndQualifier, getHBColumnType}
import github.avinoamn.HSCWrapper.utils.Consts.ROWKEY_COLUMN_FAMILY
import org.apache.spark.sql.types.{DataType, StringType}

case class HBColumn(columnName: String, columnFamily: String, columnQualifier: String, columnType: String)

/*
* Factory object/method that expects a valid HBase column name (column family and qualifier), and
* it's Spark `DataType` (`StringType` as a default value). And creates with them an instance of
* the `HBColumn` case class.
* */
object HBColumn {
  def apply(columnName: String, dataType: DataType=StringType): HBColumn = {
    val (columnFamily, columnQualifier) = getHBColumnFamilyAndQualifier(columnName)
    val columnType = getHBColumnType(dataType)

    HBColumn(
      if (columnFamily == ROWKEY_COLUMN_FAMILY) columnFamily else columnName,
      columnFamily,
      columnQualifier,
      columnType)
  }
}
