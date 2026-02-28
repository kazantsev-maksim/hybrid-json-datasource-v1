package org.apache.spark.sql.hybrid.json.datasource.mongo.table

private[sql] case class SchemaIndex(objectName: String, schemaRef: String, commitMillis: Long)
