import java.io.File
import java.time.chrono.ChronoLocalDate
import java.time.{LocalDate, LocalDateTime}
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
  def print(data: String): Unit = {
    println(data)
  }

  def print(data: Int): Unit = {
    println(data + "")
  }

  def print(data: Long): Unit = {
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

    // ---- Spark available from here ----

    // Get the spark context from the spark session
    val sc = spark.sparkContext

    // Set LOG level to ERROR
    sc.setLogLevel("ERROR")

    // Start time
    val start = System.currentTimeMillis()

    // Read all csv files in the local_index file
    val rdd = sc.textFile(csv_names.mkString(","))

    // The csv is actually a TSV (tab separated value)
    val split = rdd.map((s: String) => s.split("\t"))

    // Get the total amount of entries and print it
//    val c = split.count()
//    PrintUtility.print("Total: " + c)

    // Filter on entries with more than or equal to 24 columns for bad data filtering
    val filtered = split.filter(x => x.length >= 24)

    // Print out the good/invalid entry count
//    val good = filtered.count()
//    PrintUtility.print("Good: " + good)
//    PrintUtility.print("Invalid: " + (c - good))

    // Extract the date and allnames columns at index 1 and 23 respectively
    val dateAndNames = filtered.map(s => {
      // Parse the datetime and convert to date
      val ld = LocalDateTime.parse(s(1), DateUtility.datetime_format).toLocalDate
      // Parse the allnames string to a list of "Name,Int" strings
      val namesAndCount = s(23).split(";")
      // Only take the "Name" from each "Name,Int" string
      val names = namesAndCount.map(d => d.split(",")(0))
      // Take the distinct words to prevent double counting the same topic int the same article.
      // The reason distinct is done in scala instead of spark's distinct() is because there aren't that many
      // names per GKG record (around 10-20), therefore it is faster to just do this operation in scala
      // than flattening out each individual name as a separate record and calling distinct() on them.
      val uniqueNames = names.distinct
      (ld, uniqueNames)
    })

    // Flatten the names so there is a row for each date, name combination
    val flattened = dateAndNames.flatMap(r => r._2.map(n => (r._1, n)))

    // Filter out the topic `Type ParentCategory` because it is not a valid topic
    val filterInvalidTopic = flattened.filter(_._2 != "Type ParentCategory")

    // Group by date and name
    val groupByDateAndName = filterInvalidTopic.groupBy(r => (r._1, r._2))

    // Count the same (date, name) tuples
    val countNames = groupByDateAndName.map(p => (p._1._1, p._1._2, p._2.size))

    // Sort the (date,name,count) rows descending on count
    val sorted = countNames.sortBy(_._3, ascending = false)

    // Group by date
    val groupByDate = sorted.groupBy(_._1)

    // Take the top then names for each date
    val topTenNames = groupByDate.map(r => (r._1, r._2.map(n => (n._2, n._3)).take(10)))

    // Performant date ordering comparator: https://stackoverflow.com/a/50395814
    implicit val localDateOrdering: Ordering[LocalDate] =
      Ordering.by(identity[ChronoLocalDate])
    // Sort by date ascending
    val sortByDate = topTenNames.sortBy(r => r._1, ascending = true)

    // Final RDD
    val finalRDD = topTenNames.coalesce(1)

    // Collect the results
    val collected = finalRDD.collect()

    // End time
    val end = System.currentTimeMillis()

    // Print time taken
    PrintUtility.print("Time taken: " + (end - start)/1000.0f + "")

    // Remove the export directory if it exists
    val directory = new Directory(new File("export_rdd"))
    directory.deleteRecursively()

    // Export final RDD to disk
    finalRDD.saveAsTextFile("export_rdd")

    // Print the results
    collected.foreach(r => PrintUtility.print(r._1.toString + ": " + r._2.mkString(",")))

    spark.stop()
    // ---- Spark unavailable from here ----
  }
}
