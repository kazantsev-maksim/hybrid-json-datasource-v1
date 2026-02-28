package org.apache.spark.sql.hybrid.json.datasource.sink

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions
import org.apache.spark.sql.hybrid.json.datasource.io.FileIO
import org.apache.spark.sql.hybrid.json.datasource.mongo.MongoClient
import org.apache.spark.sql.hybrid.json.datasource.mongo.codec._
import org.apache.spark.sql.hybrid.json.datasource.mongo.table.FileIndex
import org.apache.spark.sql.hybrid.json.datasource.parser.RowParser
import org.apache.spark.sql.types.{ IntegerType, StructType }
import org.apache.spark.util.Utils

import java.nio.file.Paths
import java.util.UUID
import scala.util.{ Failure, Success, Try }

private[sql] case class HybridJsonPartitionWriter(schema: StructType, options: HybridJsonOptions) extends Logging {

  def write(partition: Iterator[InternalRow]): Unit = {
    val (forParsing, forStats) = partition.duplicate
    val parser                 = new RowParser(schema)
    val json                   = parser.toJsonString(forParsing)
    val fullOutputPath         = Paths.get(options.filepath, s"${UUID.randomUUID().toString}.json").toString
    Try {
      FileIO.unsafeWriteFile(fullOutputPath, json)
    } match {
      case Failure(ex) =>
        log.error(s"Write partition: $fullOutputPath finished with error: ${ex.getMessage}")
        throw ex
      case Success(_) =>
        val columnStatsFieldNames = schema.filter(_.dataType == IntegerType).map(_.name)
        val columnStats = columnStatsFieldNames
          .zip(
            forStats
              .map(_.toSeq(schema))
              .map(_.filter(_.isInstanceOf[Int]).map(_.asInstanceOf[Int]))
              .toSeq
              .transpose
              .map(field => (field.min, field.max))
          )
          .toMap
        Utils.tryWithResource(MongoClient(options.mongoUri, options.database, options.schemaIndex))(
          _.insert(
            FileIndex(options.objectName, fullOutputPath, System.currentTimeMillis(), columnStats)
          )
        )
    }
  }
}
