package org.apache.spark.sql.hybrid.json.datasource

import org.apache.spark.sql.execution.streaming.{ Sink, Source }
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonOptions.OBJECT_NAME
import org.apache.spark.sql.hybrid.json.datasource.HybridJsonProvider.{ emptyRelation, inferSchema }
import org.apache.spark.sql.hybrid.json.datasource.Syntax.FutureOps
import org.apache.spark.sql.hybrid.json.datasource.mongo.FieldsName.{ COMMIT_MILLIS, SCHEMA_REF }
import org.apache.spark.sql.hybrid.json.datasource.mongo.MongoClient
import org.apache.spark.sql.hybrid.json.datasource.mongo.TablesName.SCHEMA_INDEX
import org.apache.spark.sql.hybrid.json.datasource.sink.HybridJsonSink
import org.apache.spark.sql.hybrid.json.datasource.source.{ HybridJsonRelation, HybridJsonStreamSource }
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{ LongType, StructType }
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode }
import org.apache.spark.util.Utils
import org.mongodb.scala.Document

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
    val options = HybridJsonOptions(params)
    new HybridJsonSink(options).write(data)
    emptyRelation
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String]): BaseRelation = {
    val options = HybridJsonOptions(params)
    val schema  = inferSchema(options)
    HybridJsonRelation(schema, options)
  }

  override def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): BaseRelation = {
    val options = HybridJsonOptions(params)
    HybridJsonRelation(schema, options)
  }

  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    params: Map[String, String]
  ): (String, StructType) = {
    val options    = HybridJsonOptions(params)
    val readSchema = schema.getOrElse(inferSchema(options))
    providerName -> readSchema
  }

  override def createSink(
    sqlContext: SQLContext,
    params: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode
  ): Sink = {
    val options = HybridJsonOptions(params)
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
    val options = HybridJsonOptions(params)
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
      .tryWithResource(MongoClient(options.mongoUri))(
        _.find(SCHEMA_INDEX, Document(OBJECT_NAME -> options.objectName()))
          .toFuture()
          .await()
      )
      .map(_.get(SCHEMA_REF).map(_.asString().getValue))
      .flatMap(_.toSeq)
    val initSchema = new StructType().add(s"__$COMMIT_MILLIS", LongType)
    schemaRefs.map(StructType.fromString).foldLeft(initSchema) {
      case (acc, schema) => acc.merge(schema)
    }
  }
}
