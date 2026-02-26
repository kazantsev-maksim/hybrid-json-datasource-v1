package org.apache.spark.sql.hybrid.json.datasource.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions.OBJECT_NAME
import org.apache.spark.sql.hybrid.json.datasource.Syntax._
import org.apache.spark.sql.hybrid.json.datasource.io.FileIO
import org.apache.spark.sql.hybrid.json.datasource.mongo.TablesName.SCHEMA_INDEX
import org.apache.spark.sql.hybrid.json.datasource.mongo.{ FieldsName, MongoClient }
import org.apache.spark.util.Utils
import org.mongodb.scala.bson.Document
import org.mongodb.scala.model.Filters.equal

class HybridJsonSink(options: HybridJsonOptions) {

  def write(data: DataFrame): Unit = {
    val schema = data.schema
    FileIO.makeDirectory(options.path())
    val writer = HybridJsonPartitionWriter(schema, options)
    data.queryExecution.toRdd.foreachPartition(writer.write)
    val schemaRef = Utils.tryWithResource(MongoClient(options.mongoUri))(
      _.find(SCHEMA_INDEX, Document(OBJECT_NAME -> options.objectName()))
        .filter(equal(FieldsName.SCHEMA_REF, schema.asNullable.json))
        .headOption()
        .await()
    )
    if (schemaRef.isEmpty) {
      Utils.tryWithResource(MongoClient(options.mongoUri))(
        _.insertOne(
          SCHEMA_INDEX,
          Document(
            OBJECT_NAME              -> options.objectName(),
            FieldsName.SCHEMA_REF    -> schema.asNullable.json,
            FieldsName.COMMIT_MILLIS -> System.currentTimeMillis()
          )
        )
      )
    }
  }
}
