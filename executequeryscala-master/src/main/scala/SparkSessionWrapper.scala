package org.novakorp.io
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {


 lazy val sc: SparkConf =  new SparkConf().setMaster("yarn").set("spark.executor.extraClassPath","/opt/cloudera/parcels/CDH-7.0.3-1.cdh7.0.3.p0.1635019/lib/spark/jars/hive-exec-1.21.2.7.0.3.0-79.jar").set("spark.jars","/nvkprocess/ScalaJars/sparkinsert/lib/hive-exec-3.1.2000.7.0.3.0-79.jar").set("spark.sql.autoBroadcastJoinThreshold","-1").set("spark.hadoop.hive.exec.dynamic.partition.mode","nonstrict").set("spark.hadoop.hive.exec.dynamic.partition","true").set("hive.enforce.sorting", "false").set("hive.enforce.bucketing", "false").set("spark.sql.warehouse.dir", "hdfs://nameservice1/user/admin/dev").set("spark.executor.memory", "4g").set("spark.executor.cores", "4").set("spark.cores.max", "4").set("spark.driver.memory","12g")
  lazy val spark: SparkSession = SparkSession.builder().config(conf = sc).enableHiveSupport().getOrCreate()


}


