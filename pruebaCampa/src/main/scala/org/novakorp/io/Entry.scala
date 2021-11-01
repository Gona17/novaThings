package org.novakorp.io

import org.apache.spark.sql.functions._


object Entry extends DataFrameWrapper with FunctionsWrapper {
  case class TelefonoQualia(codearea: String, telefono: String)

  def main(args: Array[String]): Unit = {

    val newmat = prosptel.withColumn("Contacto", when(col("referencia_padre") === "telefono", createPhoneNumber(col("dato"))).otherwise(col("dato"))).drop("dato")
      .join(qualiadf, prosptel.col("id_cliente_core").equalTo(qualiadf.col("id_cliente")))
      .select("id_prospecto",
        "id_cliente",
        "nro_cuit",
        "referencia_padre",
        "Contacto",
        "producto",
        "cod_ramo",
        "ramo",
        "n_cliente",
        "a_cliente",
        "entidad")
    //newmat.show()

    val colum_names = Seq("id_prospecto","nro_cuit","n_cliente","a_cliente","referencia_padre","Contacto","producto","id","entidad","ramo","productoQualia")// this is example define exact number of columns
    val columnas = List("id", "ramo")
    val mapaRamos = Map(0 -> "VI", 1 -> "HG", 2 -> "CP", 3 -> "RA", 4 -> "RB", 5 -> "BI", 6 -> "RT")
    val mapaProd = Map("VI" -> "vida", "HG" -> "hogar", "CP" -> "compraProtegida", "RA" -> "cajero", "RB" -> "bolso", "BI" -> "bici", "RT" -> "notebook")


    import spark.implicits._
    val dfRamos = mapaRamos.toSeq.toDF(columnas: _*)

    val finaldf = newmat.join(metadf, newmat.col("id_cliente").equalTo(metadf.col("id_cliente_core")), "left_anti")
      .join(broadcast(dfRamos), dfRamos.col("id").equalTo(newmat.col("producto")))
      .join(qualiadf, qualiadf.col("cod_ramo").equalTo("ramo"), "left_anti")
      .select(newmat.col("id_prospecto"),
        newmat.col("nro_cuit"),
        newmat.col("n_cliente"),
        newmat.col("a_cliente"),
        newmat.col("referencia_padre"),
        newmat.col("Contacto"),
        newmat.col("producto"),
        dfRamos.col("id"),
        //newmat.col("ramo"),
        newmat.col("entidad"),
        dfRamos.col("ramo")
      ).withColumn("productoQualia", addQualiaProd(col("ramo"))).dropDuplicates("id_prospecto")
    //finaldf.show()



    mapaProd.foreach(prod => {
      val df = finaldf
        .filter(trim(col("ramo")).equalTo(lit(prod._1)))
        .select(finaldf.columns.map(col): _*)
        .withColumn("productoQualia", lit(prod._2))
      df.toDF(colum_names:_*).limit(100).show(100)
      //df.toDF(colum_names:_*).limit(3500).repartition(col("ramo")).write.option("header","true").mode("overwrite").csv(s"hdfs://nameservice1/user/admin/dev/qualia/03-ref/audiencias_bsf/salidaprod${prod._2}")
      //df.show()
    }
    )
  }
}


