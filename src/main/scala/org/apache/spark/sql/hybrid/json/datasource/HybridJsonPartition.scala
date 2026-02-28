package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.Partition

private[sql] case class HybridJsonPartition(
  index: Int,
  filepath: String,
  commitMillis: Long,
  columnStats: Map[String, (Int, Int)])
    extends Partition
