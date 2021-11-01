package org.novakorp.io

import jdk.nashorn.internal.objects.NativeArray.reduce
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{broadcast, col, from_json, lit, rand, trim, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.json4s.{DefaultFormats, Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, Formats, NoTypeHints}
import org.apache.hadoop.fs._



object Entry extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    val fecha_proceso:String = args(0)
    val entidad: String= args(1)
    val producto: String = args(2)
    val path: String= s"hdfs://nameservice1/user/admin/dev/qualia/03-ref/audiencias/${fecha_proceso}/audiencias_$entidad/salidaprod$producto/"
    val df = spark
      .read
      .option("delimiter",",")
      .option("header","true")
      .option("inferSchema","true")
      .csv(path)
    if(entidad == "ber")
    {
      df.limit(1877).write.option("header", "true").mode("overwrite").csv(s"hdfs://nameservice1/user/admin/dev/qualia/03-ref/audiencias/${fecha_proceso}/audiencias_${entidad}_cortadas/salidaprod$producto")

    }
    else if(entidad == "bsf")
    {
      df.limit(3123).write.option("header", "true").mode("overwrite").csv(s"hdfs://nameservice1/user/admin/dev/qualia/03-ref/audiencias/${fecha_proceso}/audiencias_${entidad}_cortadas/salidaprod$producto")
    }

  }
}

