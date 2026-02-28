package org.apache.spark.sql.hybrid.json.datasource.parser

import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{ CreateJacksonParser, JacksonParser }
import org.apache.spark.sql.execution.datasources.FailureSafeParser
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

private[sql] class JsonParser(schema: StructType) {

  def toRow(input: Iterator[String]): Iterator[InternalRow] = {
    val jsonParser = new JacksonParser(schema, emptySparkJsonOptions)
    val parser = new FailureSafeParser[String](
      input =>
        jsonParser
          .parse(input, CreateJacksonParser.string(_: JsonFactory, _: String), (s: String) => UTF8String.fromString(s)),
      jsonParser.options.parseMode,
      schema,
      jsonParser.options.columnNameOfCorruptRecord
    )
    input.flatMap(parser.parse)
  }
}
