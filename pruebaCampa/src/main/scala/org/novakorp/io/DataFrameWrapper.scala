package org.novakorp.io

import org.apache.spark.sql.DataFrame

trait DataFrameWrapper extends SparkSessionWrapper {

  //lazy val prosptel: DataFrame = spark.read.option("header","true").option("delimiter","|").csv("/home/agiannoni/Desktop/Qualia/Fase2/CampaniadorQualia/resources/prospectotel.csv")
  //lazy val prosptel: DataFrame = spark.read.option("header","true").option("delimiter","|").csv("/home/agiannoni/Desktop/Qualia/Fase2/CampaniadorQualia/resources/ContactoBerBancos.csv")
  //lazy val metadfber: DataFrame = spark.read.option("header","true").option("delimiter",",").csv("/home/agiannoni/Desktop/Qualia/Fase2/CampaniadorQualia/resources/meta4ber.csv")
  //lazy val qualiaber: DataFrame = spark.read.option("header","true").option("delimiter",",").csv("/home/agiannoni/Desktop/Qualia/Fase2/CampaniadorQualia/resources/clienteconsolidadoqualiaber.csv")
  //lazy val qualiaber: DataFrame = spark.read.option("header","true").option("delimiter",",").csv("/home/agiannoni/Desktop/Qualia/Fase2/CampaniadorQualia/resources/QualiaBerRamos.csv")
  //lazy val qualiaber: DataFrame = spark.read.option("header","true").option("delimiter",",").csv("/home/agiannoni/Desktop/Qualia/Fase2/CampaniadorQualia/resources/query-impala-166509.csv")

  def returnqueries(entidad:String): Array[String] = {
    val queryprosp =
      s"""select distinct
    ccp.id_cliente_core as id_cliente_core,
    ccp.id_cliente_core_protegido as id_prospecto,
    con.dato,
    con.referencia_padre,
    matriz.cat as categoria,
    matriz.producto as producto
    from de_${entidad}_3ref.rel_cliente_core_protegido ccp
      join de_${entidad}_2cur.cont_calificacion_detalle_r con
      on cast(con.id_cliente_original as bigint) = cast(ccp.id_cliente_core as bigint)
    join de_qua_3ref.cliente_categoria_producto matriz
      on matriz.id_prospecto = ccp.id_cliente_core_protegido
    where con.referencia_padre in ('telefono')
    and con.calificacion = 'conformado'
    and con.tipo_contexto = 'celular'
    and con.resultado_reglas not like '%Error%'
    """
    val meta4 = s"select * from de_${entidad}_2cur.dim_empleados_bancos"
    Array(queryprosp, meta4)
  }
  val qualia = s"""select distinct clco.id_cliente_core as id_cliente, clco.id_persona as nro_cuit, clco.ramo, clco.cod_ramo, clco.n_cliente, clco.a_cliente, clco.entidad from de_qua_2cur.dim_mov_cliente_consolidado clco where clco.entidad != 'dir' """
  lazy val queriesbsf: Array[String] = returnqueries("bsf")
  lazy val queriesber: Array[String] = returnqueries("ber")
  lazy val queriesbsj: Array[String] = returnqueries("bsj")
  lazy val queriesbsc: Array[String] = returnqueries("bsc")

  lazy val prosptelsf: DataFrame = spark.sql(queriesbsf(0))
  lazy val metadfbsf: DataFrame = spark.sql(queriesbsf(1))

  lazy val prosptelber: DataFrame = spark.sql(queriesber(0))
  lazy val metadfber: DataFrame = spark.sql(queriesber(1))

  lazy val prosptelbsj: DataFrame = spark.sql(queriesbsj(0))
  lazy val metadfbsj: DataFrame = spark.sql(queriesbsj(1))

  lazy val prosptelbsc: DataFrame = spark.sql(queriesbsc(0))
  lazy val metadfbsc: DataFrame = spark.sql(queriesbsc(1))

  val propstelArray: Array[DataFrame] = Array(/*prosptelsf,*/prosptelber)//,prosptelbsj,prosptelbsc)
  val metadfArray: Array[DataFrame] = Array(/*metadfbsf,*/metadfber)//,metadfbsj,metadfbsj)


  def createBigDF(dfs: Array[DataFrame]):DataFrame ={
    dfs.reduce(_.union(_))
  }
  val prosptel: DataFrame = createBigDF(propstelArray)
  val metadf: DataFrame = createBigDF(metadfArray)
  val qualiadf: DataFrame = spark.sql(qualia)

}
