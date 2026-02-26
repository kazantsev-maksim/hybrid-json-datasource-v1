package org.apache.spark.sql.hybrid.json.datasource.source

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions
import org.apache.spark.sql.hybrid.json.datasource.rdd.HybridJsonBatchRDD
import org.apache.spark.sql.sources.{ BaseRelation, Filter, PrunedFilteredScan }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ Row, SQLContext, SparkSession }

case class HybridJsonRelation(schema: StructType, options: HybridJsonOptions)
    extends BaseRelation
    with PrunedFilteredScan {
  def sqlContext: SQLContext = SparkSession.active.sqlContext

  override def needConversion: Boolean = false

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val requiredSchema = schema.filter(field => requiredColumns.contains(field.name))
    val rdd            = HybridJsonBatchRDD(StructType.apply(requiredSchema), filters, options)
    rdd.asInstanceOf[RDD[Row]]
  }
}
