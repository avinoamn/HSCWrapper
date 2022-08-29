package github.avinoamn.HSCWrapper.models

import github.avinoamn.HSCWrapper.utils.ColumnsUtils.{getHBColumnFamilyAndQualifier, getHBColumnType}
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * HBase column details (for building the catalog).
 *
 * @param columnName HBase column name alias in Spark `DataFrame`
 * @param columnFamily HBase column family
 * @param columnQualifier HBase column qualifier
 * @param columnType Type name of the column's `DataType`
 */
case class HBColumn(columnName: String, columnFamily: String, columnQualifier: String, columnType: String)

object HBColumn {
  /**
   * Factory method for the `HBColumn` case class.
   *
   * @param columnName HBase column name (column family and qualifier)
   * @param dfColumnName HBase column name alias in Spark `DataFrame`
   * @param dataType Spark `DataType` of the HBase column
   * @return Instance of `HBColumn` case class
   */
  def apply(columnName: String, dfColumnName: String="", dataType: DataType=StringType): HBColumn = {
    val (columnFamily, columnQualifier) = getHBColumnFamilyAndQualifier(columnName)
    val columnType = getHBColumnType(dataType)

    HBColumn(
      if (dfColumnName.nonEmpty) dfColumnName else columnName,
      columnFamily,
      columnQualifier,
      columnType)
  }
}
