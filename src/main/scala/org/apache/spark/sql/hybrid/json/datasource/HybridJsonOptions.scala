package org.apache.spark.sql.hybrid.json.datasource

private[sql] class HybridJsonOptions(params: Map[String, String]) extends Serializable {

  val objectName: String  = getOrThrow("objectName")
  val filepath: String    = getOrThrow("filepath")

  val database: String    = params.getOrElse("database", "index_store")
  val schemaIndex: String = params.getOrElse("schemaIndex", "schema_index")
  val fileIndex: String   = params.getOrElse("fileIndex", "file_index")

  val mongoUri: String =
    Option(System.getenv("MONGO_URI")).getOrElse("mongodb://localhost:27017")

  private def getOrThrow(option: String): String =
    params.getOrElse(option, throw new IllegalStateException(s"Option: `$option` must be specified"))
}
