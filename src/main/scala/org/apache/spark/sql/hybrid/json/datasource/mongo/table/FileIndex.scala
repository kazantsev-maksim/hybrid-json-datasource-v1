package org.apache.spark.sql.hybrid.json.datasource.mongo.table

private[sql] case class FileIndex(
  objectName: String,
  filepath: String,
  commitMillis: Long,
  columnStats: Map[String, (Int, Int)])
