package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions.{ FILEPATH, OBJECT_NAME }

case class HybridJsonOptions(params: Map[String, String], mongoUri: String) {

  def get(option: String): String =
    params.getOrElse(option, throw new IllegalArgumentException(s"Parameter: `$option` must be specified"))

  def objectName(): String = get(OBJECT_NAME)

  def path(): String = get(FILEPATH)
}

object HybridJsonOptions {
  private val MONGO_LOCALHOST_URI = "mongodb://localhost:27017"

  val OBJECT_NAME = "objectName"
  val FILEPATH    = "filepath"

  def apply(params: Map[String, String]): HybridJsonOptions = {
    val mongoUri = Option(System.getenv("MONGO_URI")).getOrElse(MONGO_LOCALHOST_URI)
    new HybridJsonOptions(params, mongoUri)
  }
}
