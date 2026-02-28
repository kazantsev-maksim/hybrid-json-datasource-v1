package org.apache.spark.sql.hybrid.json.datasource.rdd

import org.apache.spark.{ Partition, TaskContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonPartition
import org.apache.spark.sql.types.StructType

private[sql] class HybridJsonStreamRDD(partitions: Array[HybridJsonPartition], schema: StructType)
    extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {

  def compute(partition: Partition, context: TaskContext): Iterator[InternalRow] = {
    computePartition(partition, schema, Array.empty)
  }

  def getPartitions: Array[Partition] = partitions.asInstanceOf[Array[Partition]]
}
