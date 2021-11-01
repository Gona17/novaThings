package org.novakorp.gp.tester

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_date}

class GenerateABTDataBER extends SparkSessionWrapper with DefinedFunctions {

  def generateDataFrame(): DataFrame = {

    val fecha_campania_df = spark.sql(Queries.fecha_campania)
    val fecha_campania = fecha_campania_df.first().get(0).toString

    var audiencias_clientes_df = spark.sql(Queries.q_audiencias_clientes_ber)
    audiencias_clientes_df = audiencias_clientes_df.where(audiencias_clientes_df("fecha_resultado") === fecha_campania)
                                                   .orderBy("id_cliente_core")

    val bcraRiesgo_df = spark.sql(Queries.q_bcra_riesgo)

    val bcraAntiguedad_df = bcraAntiguedad

    val consumos_df = spark.sql(Queries.consumos_ber)
                           .withColumn("fecha_transaccion_new", to_date(col("fecha_transaccion"),"yyyyMMdd"))
                           .withColumn("fecha_campania", to_date(lit(fecha_campania),"yyyyMMdd"))

    val consumos_categorias_30d_df = consumos_categorias_30d(consumos_df)
    val consumos_categorias_90d_df = consumos_categorias_90d(consumos_df)
    val consumos_categorias_180d_df = consumos_categorias_180d(consumos_df)

    val final_df = audiencias_clientes_df
      .join(bcraRiesgo_df
        ,audiencias_clientes_df.col("id_persona") === bcraRiesgo_df.col("id_persona")
        ,"left")
      .drop(bcraRiesgo_df.col("id_persona"))
      .join(bcraAntiguedad_df
        ,audiencias_clientes_df.col("id_persona") === bcraAntiguedad_df.col("id_persona")
        ,"left")
      .drop(bcraAntiguedad_df.col("id_persona"))
      .join(consumos_categorias_30d_df
        ,audiencias_clientes_df.col("id_cliente_core") === consumos_categorias_30d_df.col("id_cliente_core")
        ,"left")
      .drop(consumos_categorias_30d_df.col("id_cliente_core"))
      .join(consumos_categorias_90d_df
        ,audiencias_clientes_df.col("id_cliente_core") === consumos_categorias_90d_df.col("id_cliente_core")
        ,"left")
      .drop(consumos_categorias_90d_df.col("id_cliente_core"))
      .join(consumos_categorias_180d_df
        ,audiencias_clientes_df.col("id_cliente_core") === consumos_categorias_180d_df.col("id_cliente_core")
        ,"left")
      .drop(consumos_categorias_180d_df.col("id_cliente_core"))

    final_df

  }

  def generateCSV(df: DataFrame): Unit = {

    df.repartition(1)
      .write
      .mode("overwrite")
      .option("delimiter","|")
      .option("header","true")
      .csv("hdfs://nameservice1/tmp/femartinez/ber/abt")

  }

  def fillABT(df: DataFrame): Unit = {

    df.createOrReplaceTempView("abtTemp")
    spark.sql(""" INSERT OVERWRITE de_ber_3ref.abt_perfil_financiero AS
                          SELECT id_cliente_core
                               , sexo
                               , estado_civil
                               , generacion
                               , monto_presunto
                               , cliente_qualia
                               , ultimo_ramo
                               , valor_asegurado
                               , prima
                               , fecha_alta
                               , riesgo_crediticio_1m
                               , riesgo_crediticio_3m
                               , riesgo_crediticio_6m
                               , antiguedad_sistema_financiero
                               , ganancias_afip
                               , categoria_iva_afip
                               , categoria_monotributo_afip
                               , actividad_agrupada_afip
                               , integrante_societario_afip
                               , es_empleador_afip
                               , media_30
                               , media_90
                               , media_180
                               , operaciones_categoria1_30d
                               , operaciones_categoria2_30d
                               , operaciones_categoria3_30d
                               , operaciones_categoria4_30d
                               , operaciones_categoria5_30d
                               , operaciones_categoria6_30d
                               , operaciones_categoria7_30d
                               , operaciones_categoria8_30d
                               , operaciones_categoria9_30d
                               , operaciones_categoria10_30d
                               , operaciones_categoria11_30d
                               , operaciones_categoria12_30d
                               , operaciones_categoria13_30d
                               , operaciones_categoria14_30d
                               , operaciones_categoria15_30d
                               , operaciones_categoria16_30d
                               , operaciones_categoria1_90d
                               , operaciones_categoria2_90d
                               , operaciones_categoria3_90d
                               , operaciones_categoria4_90d
                               , operaciones_categoria5_90d
                               , operaciones_categoria6_90d
                               , operaciones_categoria7_90d
                               , operaciones_categoria8_90d
                               , operaciones_categoria9_90d
                               , operaciones_categoria10_90d
                               , operaciones_categoria11_90d
                               , operaciones_categoria12_90d
                               , operaciones_categoria13_90d
                               , operaciones_categoria14_90d
                               , operaciones_categoria15_90d
                               , operaciones_categoria16_90d
                               , operaciones_categoria1_180d
                               , operaciones_categoria2_180d
                               , operaciones_categoria3_180d
                               , operaciones_categoria4_180d
                               , operaciones_categoria5_180d
                               , operaciones_categoria6_180d
                               , operaciones_categoria7_180d
                               , operaciones_categoria8_180d
                               , operaciones_categoria9_180d
                               , operaciones_categoria10_180d
                               , operaciones_categoria11_180d
                               , operaciones_categoria12_180d
                               , operaciones_categoria13_180d
                               , operaciones_categoria14_180d
                               , operaciones_categoria15_180d
                               , operaciones_categoria16_180d
                            FROM abtTemp """)

  }

}
