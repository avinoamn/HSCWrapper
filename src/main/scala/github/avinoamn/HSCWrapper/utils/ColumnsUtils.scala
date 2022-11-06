package github.avinoamn.HSCWrapper.utils

import github.avinoamn.HSCWrapper.models.HBColumn
import github.avinoamn.HSCWrapper.utils.Consts.{ROWKEY_COLUMN_FAMILY, ROWKEY_QUALIFIER, ROWKEY_QUALIFIERS_SEPERATOR, ROWKEY_QUALIFIER_REGEX}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ColumnsUtils {
  /**
   * Drops From Spark `DataFrame` every row that all it's columns (except rowkey column) values
   * are `null` or undefined.
   *
   * We use this function before writing to HBase because it cannot accept rows that all
   * their columns are `null` or undefined.
   */
  def dropNullRows(df: DataFrame, hbColumns: Array[HBColumn]): DataFrame = {
    val notRowkeyColumns = getNotRowkeyColumns(hbColumns)
    df.na.drop("all", notRowkeyColumns)
  }

  /**
   * From array of HBase columns names, return array subset of all the columns
   * not matching the HBase "rowkey" column.
   */
  def getNotRowkeyColumns(hbColumns: Array[HBColumn]): Array[String] = {
    hbColumns
      .filterNot(_.columnFamily.equals(ROWKEY_COLUMN_FAMILY))
      .map(_.columnName)
  }

  /**
   * From array of `HBColumn`s, return column matching the HBase "rowkey" column.
   * If "rowkey" column does not exist, return `null`.
   */
  def getRowkeyColumns(hbColumns: Array[HBColumn]): Array[HBColumn] = {
    hbColumns
      .filter(_.columnFamily.equals(ROWKEY_COLUMN_FAMILY))
  }

  def getRowkeyQualifiersString(rowkeyHBColumns: Array[HBColumn]): String = {
    rowkeyHBColumns
      .map(_.columnQualifier)
      .sortBy(_.substring(3).toInt)
      .mkString(ROWKEY_QUALIFIERS_SEPERATOR)
  }

  /**
   * Gets an array of columns names, and creates `HBColumn` for each column name in
   * the array with the default Spark type `StringType`.
   */
  def getHBColumns(columns: Array[String]): Array[HBColumn] = {
    columns.map(column => HBColumn(column))
  }

  /**
   * Gets an array of `StructField`s, and creates `HBColumn` for each `StructField` in the array .
   */
  def getHBColumns(dfSchemaFields: Array[StructField]): Array[HBColumn] = {
    dfSchemaFields.map(dfSchemaField => HBColumn(dfSchemaField.name, dataType=dfSchemaField.dataType))
  }

  /**
   * Extracts HBase column family and qualifier from HBase column name.
   *
   * Examples:
   *   "rowkey" => ("rowkey", "key")
   *   "rowkey:key1" => ("rowkey", "key1")
   *   "cf:col" => ("cf", "col")
   */
  def getHBColumnFamilyAndQualifier(columnName: String): (String, String) = {
    columnName.split(":").toSeq match {
      case ROWKEY_COLUMN_FAMILY +: Nil => (ROWKEY_COLUMN_FAMILY, ROWKEY_QUALIFIER)
      case ROWKEY_COLUMN_FAMILY +: qualifier +: Nil if qualifier.matches(ROWKEY_QUALIFIER_REGEX) => (ROWKEY_COLUMN_FAMILY, qualifier)
      case columnFamily +: columnQualifier +: Nil if columnFamily != ROWKEY_COLUMN_FAMILY => (columnFamily, columnQualifier)
      case _ => throw new Exception(s"Invalid HBase column: '${columnName}'")
    }
  }
}
