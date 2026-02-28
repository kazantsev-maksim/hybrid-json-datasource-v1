package org.apache.spark.sql.hybrid.json.datasource.mongo.codec

import org.mongodb.scala.bson.Document

trait Decoder[D] {
  def decode(document: Document): Option[D]
}
