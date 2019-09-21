import java.io.File
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
import scala.reflect.io.Directory

// Case class for typed GKG record with Timestamp
case class GKGRecord(
                      GKGRECORDID: String,
                      DATE: Timestamp,
                      SourceCollectionIdentifier: Long,
                      SourceCommonName: String,
                      DocumentIdentifier: String,
                      Counts: String,
                      V2Counts: String,
                      Themes: String,
                      V2Themes: String,
                      Locations: String,
                      V2Locations: String,
                      Persons: String,
                      V2Persons: String,
                      Organizations: String,
                      V2Organizations: String,
                      V2Tone: String,
                      Dates: String,
                      GCAM: String,
                      SharingImage: String,
                      RelatedImages: String,
                      SocialImageEmbeds: String,
                      SocialVideoEmbeds: String,
                      Quotations: String,
                      AllNames: String,
                      Amounts: String,
                      TranslationInfo: String,
                      Extras: String
                    )

// Case class for typed GKG record with Date
case class GKGRecordDate(
                          GKGRECORDID: String,
                          DATE: Date,
                          SourceCollectionIdentifier: Long,
                          SourceCommonName: String,
                          DocumentIdentifier: String,
                          Counts: String,
                          V2Counts: String,
                          Themes: String,
                          V2Themes: String,
                          Locations: String,
                          V2Locations: String,
                          Persons: String,
                          V2Persons: String,
                          Organizations: String,
                          V2Organizations: String,
                          V2Tone: String,
                          Dates: String,
                          GCAM: String,
                          SharingImage: String,
                          RelatedImages: String,
                          SocialImageEmbeds: String,
                          SocialVideoEmbeds: String,
                          Quotations: String,
                          AllNames: String,
                          Amounts: String,
                          TranslationInfo: String,
                          Extras: String
                        )

// Case class for date and names
case class DateAndNames(
                         DATE: Date,
                         Names: Array[String]
                       )

// Case class for date and name
case class DateAndName(
                        DATE: Date,
                        Name: String
                      )

// Case class for date and name and count
case class DateAndNameCount(
                             DATE: Date,
                             Name: String,
                             Count: Long
                           )

// Case class for Name and count
case class NameCountPair(
                          topic: String,
                          count: Long
                        )

// Case class for date and list of NameCountPairs
case class DateAndNameCountPairs(
                                  data: Date,
                                  result: List[NameCountPair]
                                )

object Dataset {
  def main(args: Array[String]) {
    // The Spark dataframe schema
    val schema = StructType(
      Array(
        StructField("GKGRECORDID", StringType, nullable = false),
        StructField("DATE", TimestampType, nullable = false),
        StructField("SourceCollectionIdentifier", IntegerType, nullable = false),
        StructField("SourceCommonName", StringType, nullable = false),
        StructField("DocumentIdentifier", StringType, nullable = false),
        StructField("Counts", StringType, nullable = false),
        StructField("V2Counts", StringType, nullable = false),
        StructField("Themes", StringType, nullable = false),
        StructField("V2Themes", StringType, nullable = false),
        StructField("Locations", StringType, nullable = false),
        StructField("V2Locations", StringType, nullable = false),
        StructField("Persons", StringType, nullable = false),
        StructField("V2Persons", StringType, nullable = false),
        StructField("Organizations", StringType, nullable = false),
        StructField("V2Organizations", StringType, nullable = false),
        StructField("V2Tone", StringType, nullable = false),
        StructField("Dates", StringType, nullable = false),
        StructField("GCAM", StringType, nullable = false),
        StructField("SharingImage", StringType, nullable = false),
        StructField("RelatedImages", StringType, nullable = false),
        StructField("SocialImageEmbeds", StringType, nullable = false),
        StructField("SocialVideoEmbeds", StringType, nullable = false),
        StructField("Quotations", StringType, nullable = false),
        StructField("AllNames", StringType, nullable = false),
        StructField("Amounts", StringType, nullable = false),
        StructField("TranslationInfo", StringType, nullable = false),
        StructField("Extras", StringType, nullable = false)
      )
    )

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
        case None => ""
      }
    })
    csv_names.foreach(println)

    // Initialize spark
    val spark = SparkSession
      .builder
      .appName("Spark Scala Application template")
      .config("spark.master", "local")
      .getOrCreate()

    // ---- Spark available from here ----

    // Get the Spark SQL context
    val sqlContext = spark.sqlContext

    // Import implicits
    import sqlContext.implicits._

    // Read csv files into a single Dataset
    val df = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(schema)
      .option("timestampFormat", "yyyyMMddHHmmss")
      .load(csv_names: _*)
      .as[GKGRecord]

    // Count total records
    val total = df.count()
    PrintUtility.print("Total: " + total)

    // Filter on valid data with non null Allnames and date
    val filtered = df.filter(record => record.AllNames != null && record.DATE != null)

    // Count good records
    val good = filtered.count()
    PrintUtility.print("Good: " + good)
    PrintUtility.print("Invalid: " + (total - good))

    // Convert datetime to date
    val dates = filtered.map(r => {
      val date = Date.valueOf(r.DATE.toLocalDateTime.toLocalDate)
      GKGRecordDate(r.GKGRECORDID, date, r.SourceCollectionIdentifier, r.SourceCommonName,
        r.DocumentIdentifier, r.Counts, r.V2Counts, r.Themes, r.V2Themes, r.Locations, r.V2Locations, r.Persons,
        r.V2Persons, r.Organizations, r.V2Organizations, r.V2Tone, r.Dates, r.GCAM, r.SharingImage, r.RelatedImages,
        r.SocialImageEmbeds, r.SocialVideoEmbeds, r.Quotations, r.AllNames, r.Amounts, r.TranslationInfo, r.Extras)
    })

    // Get unique allnames
    val dateAndNames = dates.map(record => {
      // Parse the allnames string to a list of "Name,Int" strings
      val namesAndIndex = record.AllNames.split(";")
      // Only take the "Name" from each "Name,Int" string
      val names = namesAndIndex.map(_.split(",")(0))
      // The reason distinct is done in scala instead of spark is because there aren't that many
      // names per GKG record (around 10-20), therefore it is faster to just do this operation in scala
      // than flattening out each individual name as a separate record.
      val uniqueNames = names.distinct
      DateAndNames(record.DATE, uniqueNames)
    })

    // Flatten date and names to (date, name) pairs
    val dateAndName = dateAndNames.flatMap(r => r.Names.map(n => DateAndName(r.DATE, n)))

    // Remove names that are `Type ParentCategory` because it is an invalid topic
    val filteredDateAndName = dateAndName.filter(_.Name != "Type ParentCategory")

    // Group by (date, name)
    val dateAndNameKeys = filteredDateAndName.groupByKey(r => (r.DATE, r.Name))

    // Count the size of the values of the keys (so how many times (date, name) occurs)
    val countedDateAndNameKeys = dateAndNameKeys.count()

    // Flatten the key value (date, string) into two columns with the third being the count
    val flattened = countedDateAndNameKeys.map(p => DateAndNameCount(p._1._1, p._1._2, p._2))

    // Sort on count descending
    val sorted = flattened.sort($"Count".desc)

    // Group by date alone
    val groupByDate = sorted.groupByKey(_.DATE)

    // Convert the KeyValueGroupedDataSet to a normal (date, List[(name, count)]) Dataset
    val mapToList = groupByDate.mapGroups((key, group) => (key, group.map(d => NameCountPair(d.Name, d.Count)).toList))

    // Get the 10 highest topics from each date
    val topTenEachDate = mapToList.map(d => DateAndNameCountPairs(d._1, d._2.take(10)))

    // Remove the export directory if it exists
    val directory = new Directory(new File("export_dataset"))
    directory.deleteRecursively()

    // Export final dataset to disk
    topTenEachDate.write.mode(SaveMode.Overwrite).json("export_dataset")

    // Show result
    topTenEachDate.show(truncate = false)

    spark.stop()
    // ---- Spark unavailable from here ----
  }
}
