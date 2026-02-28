package org.apache.spark.sql.hybrid.json.datasource.source

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{ Offset, Source }
import org.apache.spark.sql.hybrid.json.datasource.mongo.MongoClient
import org.apache.spark.sql.hybrid.json.datasource.mongo.table.FileIndex
import org.apache.spark.sql.hybrid.json.datasource.rdd.HybridJsonStreamRDD
import org.apache.spark.sql.hybrid.json.datasource.{ HybridJsonOffset, HybridJsonOptions, HybridJsonPartition }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.util.Utils

private[sql] class HybridJsonStreamSource(val schema: StructType, options: HybridJsonOptions)
    extends Source
    with Logging {
  private val spark: SparkSession = SparkSession.active

  def getOffset: Option[Offset] = {
    Utils
      .tryWithResource(MongoClient(options.mongoUri, options.database, options.fileIndex))(
        _.findByObjectName[FileIndex](options.objectName)
      )
      .map(_.commitMillis)
      .sortWith(_ < _)
      .headOption
      .map(HybridJsonOffset)
  }

  def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val endOffsetCommitMillis = end.json().toLong // unsafe casting, can throw NumberFormatException
    val files = start match {
      case Some(start) =>
        val startOffsetCommitMillis = start.json().toLong // unsafe casting, can throw NumberFormatException
        Utils
          .tryWithResource(MongoClient(options.mongoUri, options.database, options.fileIndex))(
            _.findByObjectName[FileIndex](options.objectName)
          )
          .filter { fileIndex =>
            startOffsetCommitMillis < fileIndex.commitMillis && fileIndex.commitMillis < endOffsetCommitMillis
          }
      case _ =>
        Utils.tryWithResource(MongoClient(options.mongoUri, options.database, options.fileIndex))(
          _.findByObjectName[FileIndex](options.objectName)
        )
    }
    val partitions = files.zipWithIndex.map {
      case (fileIndex, partitionId) =>
        HybridJsonPartition(partitionId, fileIndex.filepath, fileIndex.commitMillis, fileIndex.columnStats)
    }.toArray
    val rdd = new HybridJsonStreamRDD(partitions, schema)
    spark.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  def stop(): Unit = log.info(s"Stop hybrid-json stream source")
}
