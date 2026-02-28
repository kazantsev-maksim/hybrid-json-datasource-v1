package org.apache.spark.sql.hybrid.json.datasource.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.json.datasource.mongo.MongoClient
import org.apache.spark.sql.hybrid.json.datasource.mongo.table.FileIndex
import org.apache.spark.sql.hybrid.json.datasource.{ HybridJsonOptions, HybridJsonPartition }
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.{ Partition, TaskContext }

private[sql] case class HybridJsonBatchRDD(schema: StructType, filters: Array[Filter], options: HybridJsonOptions)
    extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {

  def compute(partition: Partition, context: TaskContext): Iterator[InternalRow] = {
    computePartition(partition, schema, filters)
  }

  def getPartitions: Array[Partition] = {
    Utils
      .tryWithResource(MongoClient(options.mongoUri, options.database, options.fileIndex))(
        _.findByObjectName[FileIndex](options.objectName)
      )
      .zipWithIndex
      .map {
        case (fileIndex, partitionId) =>
          HybridJsonPartition(partitionId, fileIndex.filepath, fileIndex.commitMillis, fileIndex.columnStats)
      }
      .toArray
  }
}
