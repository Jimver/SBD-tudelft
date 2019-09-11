import org.apache.spark.sql.SparkSession

object RDD {
  def main(args: Array[String]) {
    val spark = SparkSession
	.builder
	.appName("Spark Scala Application template")
	.config("spark.master", "local")
	.getOrCreate()

    // ...

    spark.stop()
  }
}
