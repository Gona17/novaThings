import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

val spark = SparkSession
  .builder
  .appName("Simple Application")
  .master("local[*]")
  .getOrCreate()

val path = "/home/gonza/Descargas/Pokemon.csv"

val df = spark
  .read
  .option("delimiter","|")
  .option("header","true")
  .option("inferSchema","true")
  .csv(path)

val newdf = df.withColumn("Legendary", lit("true"))
println(newdf.count())

