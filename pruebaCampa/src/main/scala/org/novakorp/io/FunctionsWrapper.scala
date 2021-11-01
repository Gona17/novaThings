package org.novakorp.io

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Formats, NoTypeHints}
import org.novakorp.io.Entry.TelefonoQualia

trait FunctionsWrapper extends SparkSessionWrapper {

  def createPhoneNumber: UserDefinedFunction = spark.udf.register("createPhoneNumber", (tel:String)=> {
    implicit val formats2: DefaultFormats.type = DefaultFormats
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    val telasJson = parse(tel).extract[TelefonoQualia]
    telasJson.codearea.concat(telasJson.telefono)
  })

  def addQualiaProd : UserDefinedFunction = spark.udf.register("addQualiaProd", (ramo:String)=> {
    val mapaProd = Map("VI" -> "vida", "HG" -> "hogar", "CP" -> "compraProtegida", "RA" -> "cajero", "RB" -> "bolso", "BI" -> "bici", "RT" -> "notebook")
    val qualiaprod = mapaProd.getOrElse(ramo,"producto no asignado")
    qualiaprod
  })

}
