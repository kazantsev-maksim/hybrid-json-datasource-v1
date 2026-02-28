package org.apache.spark.sql.hybrid.json.datasource.mongo.codec

import org.mongodb.scala.bson.Document

trait Encoder[D] {
  def encode(document: D): Document
}
