package org.apache.spark.sql.hybrid.json.datasource.mongo

import org.apache.spark.sql.hybrid.json.datasource.Syntax._
import org.apache.spark.sql.hybrid.json.datasource.mongo.MongoClient.DATABASE_NAME
import org.mongodb.scala.{ Document, FindObservable, MongoClient => NativeMongoClient }

import java.io.Closeable

case class MongoClient(client: NativeMongoClient) extends Closeable {

  def insertOne(table: String, entity: Document): Unit = {
    client
      .getDatabase(DATABASE_NAME)
      .getCollection(table)
      .insertOne(entity)
      .head()
      .await()
  }

  def find(table: String, expr: Document): FindObservable[Document] = {
    client
      .getDatabase(DATABASE_NAME)
      .getCollection(table)
      .find(expr)
  }

  override def close(): Unit = {
    client.close()
  }
}

object MongoClient {
  private final val DATABASE_NAME = "index_store"

  def apply(mongoUri: String): MongoClient = MongoClient(NativeMongoClient(mongoUri))
}
