package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.sql.catalyst.json.{ JSONOptions => SparkJsonOptions }
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import java.util.TimeZone

package object parser {
  val emptySparkJsonOptions =
    new SparkJsonOptions(CaseInsensitiveMap(Map.empty), TimeZone.getDefault.getID)
}
