package github.avinoamn.HSCWrapper.utils

object Consts {
  val ROWKEY_COLUMN_FAMILY = "rowkey"
  val ROWKEY_QUALIFIER = "key"
  val ROWKEY_QUALIFIER_REGEX = s"/key[1-9]*/"
  val ROWKEY_QUALIFIERS_SEPERATOR = ":"

  val NEW_TABLE_REGIONS_NUMBER = "5"
}
