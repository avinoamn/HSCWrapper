package github.avinoamn.HSCWrapper.models

import github.avinoamn.HSCWrapper.utils.Consts.NEW_TABLE_REGIONS_NUMBER

/**
 * HBase column details (for building the catalog).
 *
 * @param name HBase table name
 * @param namespace HBase table namespace
 */
case class HBTable(name: String, namespace: String="default", newTableRegionsNumber: String=NEW_TABLE_REGIONS_NUMBER)
