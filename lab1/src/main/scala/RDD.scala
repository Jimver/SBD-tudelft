import java.io.File
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.reflect.io.Directory

// Helper object for parsing the datetime
object DateUtility {
  val datetime_format: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
}

// Helper utility to print data in Spark
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
    // Get local index file
    val local_index_file = "data/local_index.txt"
    val source = Source.fromFile(local_index_file)
    // Read the lines
    val lines = source.getLines.toArray
    source.close()
    // Each entry in csv_names is now a (local) filepath to a csv file
    val csv_names = lines.map(s => {
      // Match only on local path so docker is also compatible
      val pattern = "data/segment.*".r
      pattern.findFirstIn(s) match {
        case Some(x) => x
        case None => println("Cannot match: " + s)
      }
    })
    csv_names.foreach(println)

    // Create the Spark session
    val spark = SparkSession
      .builder
      .appName("Spark Scala Application template")
      .config("spark.master", "local")
      .getOrCreate()
    // Get the spark context from the spark session
    val sc = spark.sparkContext

    // ---- Spark available from here ----

    // Read all csv files in the local_index file
    val rdd = sc.textFile(csv_names.mkString(","))

    // The csv is actually a TSV (tab separated value)
    val split = rdd.map((s: String) => s.split("\t"))

    // Get the total amount of entries and print it
    val c = split.count()
    PrintUtility.print("Total: " + c)

    // Filter on entries with more than or equal to 24 columns for bad data filtering
    val filtered = split.filter(x => x.length >= 24)
    val good = filtered.count()

    // Print out the good/invalid entry count
    PrintUtility.print("Good: " + good)
    PrintUtility.print("Invalid: " + (c - good))

    // Extract the date and allnames columns at index 1 and 23 respectively
    val dateAndNames = filtered.map(s => {
      // Parse the datetime and convert to date
      val ld = LocalDateTime.parse(s(1), DateUtility.datetime_format).toLocalDate
      // Parse the allnames string to a list of "Name,Int" strings
      val namesAndCount = s(23).split(";")
      // Only take the "Name" from each "Name,Int" string
      val names = namesAndCount.map(d => d.split(",")(0))
      (ld, names)
    })

    // Group by date
    val groupByDate = dateAndNames.groupByKey()

    // Flatten the allnames that were combined from the group by action
    val flattened = groupByDate.map(x => {
      val words = x._2.flatten
      (x._1, words)
    })

    // Count the allnames and filter out the `Type ParentCategory` topic because it is not an actual topic
    val counted = flattened.map(x => {
      // Count the names
      val counters = x._2.groupBy(l => l).map(t => (t._1, t._2.size)).toList
      // Filter on `Type ParentCategory` and take the 10 highest occurring allnames
      val sorted = counters.filter(_._1 != "Type ParentCategory").sortWith((a, b) => a._2 > b._2).take(10)
      (x._1, sorted)
    })

    // Remove the export directory if it exists
    val directory = new Directory(new File("export_rdd"))
    directory.deleteRecursively()

    // Export final RDD to disk
    counted.coalesce(1).saveAsTextFile("export_rdd")

    spark.stop()
    // ---- Spark unavailable from here ----
  }
}
