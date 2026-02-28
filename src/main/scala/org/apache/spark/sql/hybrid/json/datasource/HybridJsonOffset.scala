package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.sql.execution.streaming.Offset

private[sql] case class HybridJsonOffset(commitMillis: Long) extends Offset {
  def json(): String = commitMillis.toString
}
