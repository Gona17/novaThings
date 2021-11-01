package org.novakorp.io
import org.apache.spark.sql.functions.{col, lit}


object Execute extends SparkSessionWrapper {

  def insertarQuery(tabla_destino:String, pathArch:String, fecha_proceso:String, condicion: String):Unit ={
    try{

      println(pathArch)
      var df = spark
        .read
        .option("delimiter", "|")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(pathArch)
      df.printSchema
      df = df.drop(col("fecha_proceso")).withColumn("fecha_proceso", lit(fecha_proceso))
      df.createOrReplaceTempView("tablanueva")

      spark.sql(s"insert overwrite table ${tabla_destino} partition (fecha_proceso) select * from tablanueva where ${condicion}")

    }
    catch{
      case e: Exception =>{
        println("hubo un error en el codigo del insert")
        println(e)
      }

    }

  }

}

