package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.Partition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hybrid.json.datasource.parser.JsonParser
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.io.Source.fromFile

package object rdd {

  def computePartition(partition: Partition, schema: StructType, filters: Array[Filter]): Iterator[InternalRow] = {
    val jsonPartition = partition.asInstanceOf[HybridJsonPartition]
    val columnStats   = jsonPartition.columnStats
    val shouldReadFile = filters.forall {
      case EqualTo(attribute, value) =>
        columnStats.get(attribute).exists {
          case (max, min) => value.asInstanceOf[Int] <= max && value.asInstanceOf[Int] >= min
        }
      case GreaterThan(attribute, value) =>
        columnStats.get(attribute).exists {
          case (max, _) => value.asInstanceOf[Int] < max
        }
      case GreaterThanOrEqual(attribute, value) =>
        columnStats.get(attribute).exists {
          case (max, _) => value.asInstanceOf[Int] <= max
        }
      case LessThan(attribute, value) =>
        columnStats.get(attribute).exists {
          case (_, min) => value.asInstanceOf[Int] > min
        }
      case LessThanOrEqual(attribute, value) =>
        columnStats.get(attribute).exists {
          case (_, min) => value.asInstanceOf[Int] >= min
        }
      case _ => true
    }
    if (shouldReadFile) {
      val fileRef = fromFile(jsonPartition.filepath)
      val parser  = new JsonParser(schema)
      parser.toRow(fileRef.getLines()).map { ir =>
        schema.headOption
          .filter(_.name == s"__commitMillis")
          .foreach(_ => ir.setLong(0, jsonPartition.commitMillis))
        ir
      }
    } else Iterator.empty
  }
}
