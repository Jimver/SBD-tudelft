package lab3

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Printed, Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.processor.PunctuationType

import scala.collection.JavaConversions._

object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-server:9092")
    p
  }

  // Using a `KeyValueStoreBuilder` to build a `KeyValueStore`.// Using a `KeyValueStoreBuilder` to build a `KeyValueStore`.
  val countStoreSupplier: StoreBuilder[KeyValueStore[String, String]] =
    Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore("inmemory-dates"),
      Serdes.String,
      Serdes.String)
  val countStore: KeyValueStore[String, String] = countStoreSupplier.build

  val builder: StreamsBuilder = new StreamsBuilder
  builder.addStateStore(countStoreSupplier)

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally,
  // write the result to a new topic called gdelt-histogram.
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  // Map the input data to output stream
  val transformed = records
    .mapValues(v => v.split("\t")) // Separate on tabs
    .filter((k, arr) => arr.length >= 24) // Filter on records with 24 or more fields
    .mapValues(v => v(23)) // Get allnames field at index 23
    .mapValues(v => v.split(";")) // Extract names,index pairs
    .mapValues(v => v.map(s => {
      val lastIndex = if (s.lastIndexOf(',') >= 0) s.lastIndexOf(',') else s.length
      s.substring(0, lastIndex)
    })) // Extract only names from names,index pairs
    .flatMapValues(arr => arr) // Flatmap values so we can apply the transform on it
    .filter((k, v) => !v.isEmpty) // Filter out empty strings
    .transform(new TransformerSupplier[String, String, KeyValue[String, Long]] {
      override def get(): Transformer[String, String, KeyValue[String, Long]] = new HistogramTransformer()
    }, "inmemory-dates") // Apply histogram transformer
  transformed.to("gdelt-histogram") // Send to next kafka topic)

  // Print the stream for debugging
  val fileout = Printed
    .toFile[String, Long]("output")
    .withLabel("gdeltStream")
  transformed.print(fileout)
  val sysout = Printed
    .toSysOut[String, Long]
    .withLabel("gdeltStream")
  transformed.print(sysout)

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.cleanUp()
    streams.close(10, TimeUnit.SECONDS)
  }

}

// This transformer should count the number of times a name occurs
// during the last hour. This means it needs to be able to
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, KeyValue[String, Long]] {
  // Fields
  var context: ProcessorContext = _
  var state_store: KeyValueStore[String, String] = _
  // Date time formatter for parsing
  val datetime_format: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  // How often to prune old entries in state store
  val poll_ms = 1000 * 60

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
    this.state_store = context.getStateStore("inmemory-dates").asInstanceOf[KeyValueStore[String, String]]
    // schedule a punctuate() method every "poll_ms" (wall clock time)
    this.context.schedule(this.poll_ms, PunctuationType.WALL_CLOCK_TIME, new Punctuator {
      override def punctuate(timestamp: Long): Unit = {
        pruneOldEntries()
      }
    })
  }

  // Removes all old entries in the state store (so for all keys)
  def pruneOldEntries() = {
    val pairs = this.state_store.all()
    pairs.foreach(p => {
      // Update state store for key
      updateStateStore(p.key)
      // If the value is null or empty delete the key from state store
      val v = this.state_store.get(p.value)
      if (v == null || v.isEmpty())
        this.state_store.delete(p.key)
    })
  }

  // Update the state store for a given name
  def updateStateStore(name: String): Unit = {
    // Get date strings
    val timestamps = this.state_store.get(name)
    //    println("Update 1: " + timestamps)
    // Return if empty
    if (timestamps == null || timestamps.isEmpty) return
    //    println("Update 2: " + timestamps)
    // Convert string of datetimes to array of datetimes
    val split = timestamps.split(',')
    // Convert date strings to localdatetime objects
    val parsed = split.map(s => LocalDateTime.parse(s, datetime_format))
    // Keep only the datetimes that are more recent than one hour ago
    val filtered = parsed.filter(ldt => !olderThanAnHour(ldt))
    // Convert datetimes to single string again
    val single_string = filtered.map(ldt => ldt.format(datetime_format)).mkString(",")
    // Update the state store
    this.state_store.put(name, single_string)
  }

  // Add date to state store for a given name
  def addToStateStore(name: String, date: String) = {
    val old = this.state_store.get(name)
    val all_string = if (old == null || old.isEmpty()) date else old + "," + date
    //    println("Adding: " + name + ", " + date + ", " + all_string)
    this.state_store.put(name, all_string)
  }

  // Get the count from the state store for a given name
  def getCountFromStateStore(name: String): Long = {
    val all_string = this.state_store.get(name)
    val count = if (all_string == null) 0 else all_string.split(',').length
    count
  }

  // Returns true if the timestamp is older than an hour ago, false otherwise
  def olderThanAnHour(timestamp: LocalDateTime): Boolean = {
    val older = LocalDateTime.now().minusHours(1).isAfter(timestamp)
    if (older) {
      println("Older than an hour: " + timestamp.format(datetime_format))
    }
    older
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): KeyValue[String, Long] = {
    //    println("Transform: " + name)
    // Prune old records
    updateStateStore(name)
    // Get date from key
    val dateTime = LocalDateTime.parse(key.split('-')(0), datetime_format)
    // If the dateTime is not older than an hour
    if (!olderThanAnHour(dateTime)) {
      // Put into statestore
      addToStateStore(name, dateTime.format(datetime_format))
    }
    // Get count of name
    val count = getCountFromStateStore(name)
    (name, count)
  }

  // Close any resources if any
  def close() {}
}
