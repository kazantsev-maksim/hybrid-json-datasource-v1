package org.apache.spark.sql.hybrid.json.datasource.sink

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions
import org.apache.spark.sql.hybrid.json.datasource.io.FileIO
import org.apache.spark.sql.hybrid.json.datasource.mongo.MongoClient
import org.apache.spark.sql.hybrid.json.datasource.mongo.table.SchemaIndex
import org.apache.spark.util.Utils

private[sql] class HybridJsonSink(options: HybridJsonOptions) {

  def write(data: DataFrame): Unit = {
    val schema = data.schema
    FileIO.makeDirectory(options.filepath)
    val writer = HybridJsonPartitionWriter(schema, options)
    data.queryExecution.toRdd.foreachPartition(writer.write)
    val schemaRef = Utils
      .tryWithResource(MongoClient(options.mongoUri, options.database, options.fileIndex))(
        _.findByObjectName[SchemaIndex](options.objectName)
      )
      .find(_.schemaRef == schema.asNullable.json)
    if (schemaRef.isEmpty) {
      Utils.tryWithResource(MongoClient(options.mongoUri, options.database, options.schemaIndex))(
        _.insert(SchemaIndex(options.objectName, schema.asNullable.json, System.currentTimeMillis()))
      )
    }
  }
}
