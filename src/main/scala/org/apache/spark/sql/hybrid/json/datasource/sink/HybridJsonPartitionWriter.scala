package org.apache.spark.sql.hybrid.json.datasource.sink

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions.{FILEPATH, OBJECT_NAME}
import org.apache.spark.sql.hybrid.json.datasource.io.FileIO
import org.apache.spark.sql.hybrid.json.datasource.mongo.TablesName.FILE_INDEX
import org.apache.spark.sql.hybrid.json.datasource.mongo.{FieldsName, MongoClient}
import org.apache.spark.sql.hybrid.json.datasource.parser.RowParser
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.util.Utils
import org.mongodb.scala.bson.Document

import java.nio.file.Paths
import java.util.UUID
import scala.util.{Failure, Success, Try}

case class HybridJsonPartitionWriter(schema: StructType, options: HybridJsonOptions) extends Logging {

  def write(partition: Iterator[InternalRow]): Unit = {
    val (parsingPartition, statsPartition) = partition.duplicate
    val parser                             = new RowParser(schema)
    val json                               = parser.toJsonString(parsingPartition)
    val fullOutputPath                     = Paths.get(options.path(), s"${UUID.randomUUID().toString}.json").toString
    Try {
      FileIO.unsafeWriteFile(fullOutputPath, json)
    } match {
      case Failure(ex) =>
        log.error(s"Write partition: $fullOutputPath finished with exception: ${ex.getMessage}")
        throw ex
      case Success(_) =>
        val columnsStatsFieldsName = schema.filter(_.dataType == IntegerType).map(_.name)
        val columnStats = statsPartition
          .map(_.toSeq(schema))
          .map(_.filter(_.isInstanceOf[Int]).map(_.asInstanceOf[Int]))
          .toSeq
          .transpose
          .map(col => (col.min, col.max))
          .zip(columnsStatsFieldsName)
          .map {
            case ((min, max), name) => Document(FieldsName.NAME -> name, FieldsName.MIN -> min, FieldsName.MAX -> max)
          }
        Utils.tryWithResource(MongoClient(options.mongoUri))(
          _.insertOne(
            FILE_INDEX,
            Document(
              OBJECT_NAME              -> options.objectName(),
              FILEPATH                 -> fullOutputPath,
              FieldsName.COMMIT_MILLIS -> System.currentTimeMillis(),
              FieldsName.COLUMN_STATS  -> columnStats
            )
          )
        )
    }
  }
}
