package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.sql.hybrid.json.datasource.parser.JsonParser
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class JsonParserSpec extends AnyFlatSpec with should.Matchers with TestSparkSession {

  val Schema: StructType = new StructType()
    .add("firstName", StringType)
    .add("lastName", StringType)
    .add("age", IntegerType)

  val RawUserJsonString: String = """ { "firstName": "Igor", "lastName": "Ivanov", "age": 31 } """

  "JsonParser" should s"parse $RawUserJsonString" in {
    val parser = new JsonParser(Schema)
    val row    = parser.toRow(Iterator(RawUserJsonString))
    val rdd    = spark.sparkContext.parallelize(row.toList)
    val user = spark
      .internalCreateDataFrame(rdd, Schema, isStreaming = false)
      .collect()
      .head

    user.getAs[String]("firstName") shouldBe "Igor"
    user.getAs[String]("lastName") shouldBe "Ivanov"
    user.getAs[Int]("age") shouldBe 31
  }
}
