package org.apache.spark.sql.hybrid.json.datasource.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions.OBJECT_NAME
import org.apache.spark.sql.hybrid.json.datasource.Syntax._
import org.apache.spark.sql.hybrid.json.datasource.mongo.MongoClient
import org.apache.spark.sql.hybrid.json.datasource.mongo.TablesName.FILE_INDEX
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.{ Partition, TaskContext }
import org.mongodb.scala.Document

case class HybridJsonBatchRDD(schema: StructType, filters: Array[Filter], options: HybridJsonOptions)
    extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {

  def compute(partition: Partition, context: TaskContext): Iterator[InternalRow] = {
    computePartition(partition, schema, filters)
  }

  def getPartitions: Array[Partition] = {
    Utils
      .tryWithResource(MongoClient(options.mongoUri))(
        _.find(FILE_INDEX, Document(OBJECT_NAME -> options.objectName()))
          .toFuture()
          .await()
      )
      .zipWithIndex
      .map { case (fileIndex, partitionId) => buildPartition(partitionId, fileIndex) }
      .flatMap(_.toSeq)
      .toArray
  }
}
