package github.avinoamn.HSCWrapper.models

/**
 * HBase column details (for building the catalog).
 *
 * @param name HBase table name
 * @param namespace HBase table namespace
 */
case class HBTable(name: String, namespace: String="default")
