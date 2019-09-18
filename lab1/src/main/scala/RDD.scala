import org.apache.spark.sql.SparkSession

object PrintUtility {
    def print(data: String) = {
      println(data)
    }
    def print(data: Int) = {
      println(data + "")
    }
    def print(data: Long) = {
      println(data + "")
    }
}

object RDD {
  def main(args: Array[String]) {
    val data_file = "/home/jim/git/SBD-tudelft/lab1/data/segment/20150218230000.gkg.csv"


    val spark = SparkSession
    .builder
    .appName("Spark Scala Application template")
    .config("spark.master", "local")
    .getOrCreate()
    val sc = spark.sparkContext

    // Spark available from here
    val rdd = sc.textFile(data_file)
    val split = rdd.map((s: String) => s.split("\t"))
    val c = split.count()
    PrintUtility.print("Total: " + c)
    val filtered = split.filter(x => x.length >= 24)
    val good = filtered.count()
    PrintUtility.print("Good: " + good)
    
    val dateAndNames = filtered.map(s => {
      val namesAndCount = s(23).split(";")
      val names = namesAndCount.map(d => d.split(",")(0))
      (s(1), names)
    })
    val groupByDate = dateAndNames.groupByKey()
    val flattened = groupByDate.map(x => {
      val words = x._2.flatMap(q => q)
      (x._1, words)
    })
    val counted = flattened.map(x => {
      val counters = x._2.groupBy(l => l).map(t => (t._1, t._2.size))
      (x._1, counters)
    })

    val one = counted.take(10)

    one.foreach(x => {
      PrintUtility.print(x._1)
      x._2.foreach(e => {
        PrintUtility.print(e._1 + ", " + e._2)
      })
      PrintUtility.print("")
    })
    counted.coalesce(1).saveAsTextFile("newfile")
    spark.stop()
  }
}
