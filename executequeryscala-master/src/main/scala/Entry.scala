package org.novakorp.io

import org.json4s.jackson.Serialization._
import org.json4s.jackson.Serialization
import org.json4s.{ Formats, NoTypeHints }
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import scala.io.StdIn.readLine
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession




object Entry {

  def main(args: Array[String]): Unit = {

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    implicit val formats2: DefaultFormats.type = DefaultFormats
     
try{
    val entrada = readLine().mkString("")
    println(s"entrada: ${entrada}")
    val jsonin = parse(entrada)
    println(s"JSON IN: ${jsonin}")

    val querytoexecute: String = jsonin.extract[PathArch].pathArch.toString

    Execute.insertarQuery(tabla_destino = args(0), querytoexecute, args(1), args(2))
}
catch {
      case e: Exception =>
              println("hubo un error en el codigo ")
              println(e)

    }
  }
}
