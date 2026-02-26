package org.apache.spark.sql.hybrid.json.datasource.io

import org.apache.spark.util.Utils

import java.io.{ BufferedWriter, File, FileWriter }

object FileIO {

  def makeDirectory(path: String): Unit = new File(path).mkdirs()

  def unsafeWriteFile(path: String, data: Iterator[String]): Unit = {
    val output = data.mkString("\n")
    val file   = new File(path)
    Utils.tryWithResource(new BufferedWriter(new FileWriter(file)))(_.write(output))
  }
}
