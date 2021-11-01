import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder
  .appName("Simple Application")
  .master("local[*]")
  .getOrCreate()


val path: String= "/home/gonza/Descargas/Spark-The-Definitive-Guide-master/data/flight-data/csv/2010-summary.csv"
val df = spark
  .read
  .option("delimiter",",")
  .option("header","true")
  .option("inferSchema","true")
  .csv(path)

df.registerTempTable("df")
val tab = spark.sql("select * from df")
tab.show()