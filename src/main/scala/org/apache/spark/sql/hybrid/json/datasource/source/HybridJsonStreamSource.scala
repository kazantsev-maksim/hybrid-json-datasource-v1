package org.apache.spark.sql.hybrid.json.datasource.source

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions.OBJECT_NAME
import org.apache.spark.sql.hybrid.json.datasource.Syntax._
import org.apache.spark.sql.hybrid.json.datasource.mongo.FieldsName._
import org.apache.spark.sql.hybrid.json.datasource.mongo.MongoClient
import org.apache.spark.sql.hybrid.json.datasource.mongo.TablesName.FILE_INDEX
import org.apache.spark.sql.hybrid.json.datasource.rdd.{HybridJsonStreamRDD, buildPartition}
import org.apache.spark.sql.hybrid.json.datasource.{HybridJsonOffset, HybridJsonOptions}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.Utils
import org.mongodb.scala.Document
import org.mongodb.scala.model.Filters.{and, gt, lte}
import org.mongodb.scala.model.Sorts.{descending, orderBy}

class HybridJsonStreamSource(val schema: StructType, options: HybridJsonOptions) extends Source with Logging {
  val spark: SparkSession = SparkSession.active

  def getOffset: Option[Offset] = {
    Utils
      .tryWithResource(MongoClient(options.mongoUri))(
        _.find(FILE_INDEX, Document(OBJECT_NAME -> options.objectName()))
          .sort(orderBy(descending(COMMIT_MILLIS)))
          .map(_.get(COMMIT_MILLIS).map(_.asNumber().longValue()))
          .head()
          .await()
      )
      .map(HybridJsonOffset)
  }

  def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val endOffsetCommitMillis = end.json().toLong // unsafe casting, can throw NumberFormatException
    val files = start match {
      case Some(start) =>
        val startOffsetCommitMillis = start.json().toLong // unsafe casting, can throw NumberFormatException
        Utils
          .tryWithResource(MongoClient(options.mongoUri))(
            _.find(FILE_INDEX, Document(OBJECT_NAME -> options.objectName()))
              .filter(and(gt(COMMIT_MILLIS, startOffsetCommitMillis), lte(COMMIT_MILLIS, endOffsetCommitMillis)))
              .toFuture()
              .await()
          )
      case _ =>
        Utils.tryWithResource(MongoClient(options.mongoUri))(
          _.find(FILE_INDEX, Document(OBJECT_NAME -> options.objectName()))
            .toFuture()
            .await()
        )
    }
    val partitions = files.zipWithIndex.map { case (fileIndex, partitionId) => buildPartition(partitionId, fileIndex) }
      .flatMap(_.toSeq)
      .toArray
    val rdd = new HybridJsonStreamRDD(partitions, schema)
    spark.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  def stop(): Unit = log.info(s"Stop hybrid-json stream source")
}
