import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("Simple Application")
  .master("local[*]")
  .getOrCreate()

val path = "/home/gonza/Documentos/gapp-local/matriz.csv/part-00000-87d44bf0-2e54-419e-87b9-0b764b94c6fd-c000.csv"

val df = spark
  .read
  .option("delimiter", ",")
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(path)

df.show()
val ndf2=df.rdd.map(_.toSeq.toList)
/*val ndf = df.select("scaled_0").rdd.map(r => r(0)).collect()*/
println(ndf2)