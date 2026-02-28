package org.apache.spark.sql.hybrid.json.datasource.mongo

import org.apache.spark.sql.hybrid.json.datasource.Syntax._
import org.apache.spark.sql.hybrid.json.datasource.mongo.codec.{ Decoder, Encoder }
import org.mongodb.scala.{ Document, MongoClient => NativeMongoClient }

import java.io.Closeable

private[sql] case class MongoClient(client: NativeMongoClient, database: String, tableName: String) extends Closeable {

  def insert[D: Encoder](document: D): Unit = {
    client
      .getDatabase(database)
      .getCollection(tableName)
      .insertOne(implicitly[Encoder[D]].encode(document))
      .head()
      .await()
  }

  def findByObjectName[D: Decoder](objectName: String): Seq[D] = {
    client
      .getDatabase(database)
      .getCollection(tableName)
      .find(Document("object_name" -> objectName))
      .toFuture()
      .await()
      .flatMap(implicitly[Decoder[D]].decode)
  }

  def close(): Unit = {
    client.close()
  }
}

object MongoClient {

  def apply(mongoUri: String, database: String, tableName: String): MongoClient = {
    new MongoClient(NativeMongoClient(mongoUri), database, tableName)
  }
}
