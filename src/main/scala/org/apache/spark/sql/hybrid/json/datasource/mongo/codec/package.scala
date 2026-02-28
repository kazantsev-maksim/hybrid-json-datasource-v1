package org.apache.spark.sql.hybrid.json.datasource.mongo

import org.apache.spark.sql.hybrid.json.datasource.mongo.table.{ FileIndex, SchemaIndex }
import org.mongodb.scala.bson.Document

import scala.collection.JavaConverters.asScalaBufferConverter

package object codec {

  implicit val fileIndexEncoder: Encoder[FileIndex] = new Encoder[FileIndex] {
    def encode(document: FileIndex): Document = {
      val columnStats = document.columnStats.map {
        case (fieldName, (min, max)) =>
          Document("field_name" -> fieldName, "min" -> min, "max" -> max)
      }.toSeq
      Document(
        "object_name"   -> document.objectName,
        "filepath"      -> document.filepath,
        "commit_millis" -> document.commitMillis,
        "column_stats"  -> columnStats
      )
    }
  }

  implicit val fileIndexDecoder: Decoder[FileIndex] = new Decoder[FileIndex] {
    def decode(document: Document): Option[FileIndex] = {
      for {
        objectName   <- document.get("object_name").map(_.asString().getValue)
        filepath     <- document.get("filepath").map(_.asString().getValue)
        commitMillis <- document.get("commit_millis").map(_.asNumber().longValue())
        columnStats <- document
                        .get("column_stats")
                        .map(_.asArray())
                        .map(_.getValues.asScala.map(_.asDocument()).flatMap(decodeColumnStats(_)))
      } yield FileIndex(objectName, filepath, commitMillis, columnStats.toMap)
    }

    private def decodeColumnStats(document: Document): Option[(String, (Int, Int))] = {
      for {
        name <- document.get("field_name").map(_.asString().getValue)
        max  <- document.get("max").map(_.asNumber().intValue())
        min  <- document.get("min").map(_.asNumber().intValue())
      } yield {
        name -> (max, min)
      }
    }
  }

  implicit val schemaIndexEncoder: Encoder[SchemaIndex] = new Encoder[SchemaIndex] {
    def encode(document: SchemaIndex): Document = {
      Document(
        "object_name"   -> document.objectName,
        "schema_ref"    -> document.schemaRef,
        "commit_millis" -> document.commitMillis
      )
    }
  }

  implicit val schemaIndexDecoder: Decoder[SchemaIndex] = new Decoder[SchemaIndex] {
    def decode(document: Document): Option[SchemaIndex] = {
      for {
        objectName   <- document.get("object_name").map(_.asString().getValue)
        schemaRef    <- document.get("schema_ref").map(_.asString().getValue)
        commitMillis <- document.get("commit_millis").map(_.asNumber().longValue())
      } yield {
        SchemaIndex(objectName, schemaRef, commitMillis)
      }
    }
  }
}
