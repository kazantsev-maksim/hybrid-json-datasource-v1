package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.sql.execution.streaming.{ Sink, Source }
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonProvider.{ emptyRelation, inferSchema }
import org.apache.spark.sql.hybrid.json.datasource.mongo.MongoClient
import org.apache.spark.sql.hybrid.json.datasource.mongo.table.SchemaIndex
import org.apache.spark.sql.hybrid.json.datasource.sink.HybridJsonSink
import org.apache.spark.sql.hybrid.json.datasource.source.{ HybridJsonRelation, HybridJsonStreamSource }
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{ LongType, StructType }
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode }
import org.apache.spark.util.Utils

class HybridJsonProvider
    extends CreatableRelationProvider
    with RelationProvider
    with DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider
    with SchemaRelationProvider {

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    params: Map[String, String],
    data: DataFrame
  ): BaseRelation = {
    val options = new HybridJsonOptions(params)
    new HybridJsonSink(options).write(data)
    emptyRelation
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String]): BaseRelation = {
    val options = new HybridJsonOptions(params)
    val schema  = inferSchema(options)
    HybridJsonRelation(schema, options)
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): BaseRelation = {
    val options = new HybridJsonOptions(params)
    HybridJsonRelation(schema, options)
  }

  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    params: Map[String, String]
  ): (String, StructType) = {
    val options    = new HybridJsonOptions(params)
    val readSchema = schema.getOrElse(inferSchema(options))
    providerName -> readSchema
  }

  override def createSink(
    sqlContext: SQLContext,
    params: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode
  ): Sink = {
    val options = new HybridJsonOptions(params)
    new Sink {
      override def addBatch(batchId: Long, data: DataFrame): Unit = new HybridJsonSink(options).write(data)
    }
  }

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    params: Map[String, String]
  ): Source = {
    val options = new HybridJsonOptions(params)
    new HybridJsonStreamSource(schema.getOrElse(inferSchema(options)), options)
  }

  override def shortName(): String = "hybrid-json"
}

object HybridJsonProvider {
  private val emptyRelation =
    new BaseRelation {
      def sqlContext: SQLContext = null
      def schema: StructType     = null
    }

  private def inferSchema(options: HybridJsonOptions): StructType = {
    val schemaRefs = Utils
      .tryWithResource(MongoClient(options.mongoUri, options.database, options.schemaIndex))(
        _.findByObjectName[SchemaIndex](options.objectName)
      )
      .map(_.schemaRef)
    val schema = new StructType().add(s"__commitMillis", LongType)
    schemaRefs
      .map(StructType.fromString)
      .foldLeft(schema) { case (acc, schema) => acc.merge(schema) }
  }
}
