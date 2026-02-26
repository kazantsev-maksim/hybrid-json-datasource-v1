package org.apache.spark.sql.hybrid.json.datasource

package object mongo {
  object FieldsName {
    val COMMIT_MILLIS = "commitMillis"
    val SCHEMA_REF    = "schemaRef"
    val COLUMN_STATS  = "columnStats"
    val NAME          = "name"
    val MIN           = "min"
    val MAX           = "max"
  }

  object TablesName {
    val SCHEMA_INDEX = "schema_index"
    val FILE_INDEX   = "file_index"
  }
}
