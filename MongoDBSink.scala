//> using scala "2.12.18"
//> using dep "org.apache.kafka:kafka-clients:3.4.0"
//> using dep "org.mongodb.scala::mongo-scala-driver:4.11.1"
//> using dep "com.fasterxml.jackson.core:jackson-databind:2.15.2"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"

import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig, ConsumerRecords}
import org.mongodb.scala._
import org.mongodb.scala.model.{Filters, UpdateOptions, Updates}
import org.mongodb.scala.bson.{BsonDouble, BsonString}
import com.fasterxml.jackson.databind.ObjectMapper

import java.time.Duration
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
import scala.concurrent.Await

/**
 * MongoDBSink
 *
 * Consumes ML prediction messages from Kafka topic "forex-predictions"
 * and stores them in MongoDB collections:
 *   - forex_db.rf_predictions   (Random Forest 24h predictions)
 *   - forex_db.gbt_predictions  (GBT 24h predictions)
 *   - forex_db.lr_signals       (Logistic Regression direction signals)
 *
 * Prerequisites:
 *   1. MongoDB running on localhost:27017
 *   2. Kafka running on localhost:9092
 *   3. Run SparkMLModels.scala first to produce predictions
 *
 * Run:
 *   scala-cli run MongoDBSink.scala
 */
object MongoDBSink {

  val KAFKA_BROKER  = "localhost:9092"
  val KAFKA_TOPIC   = "forex-predictions"
  val GROUP_ID      = "mongodb-sink-consumer"
  val MONGO_URI     = "mongodb://localhost:27017"
  val DB_NAME       = "forex_db"

  // Helper: block on a MongoDB observable
  implicit class ObservableHelper[T](obs: Observable[T]) {
    def results(): Seq[T] = Await.result(
      obs.toFuture(),
      scala.concurrent.duration.Duration(30, "seconds")
    )
    def headResult(): T = Await.result(
      obs.head(),
      scala.concurrent.duration.Duration(30, "seconds")
    )
  }

  def main(args: Array[String]): Unit = {

    println("=" * 70)
    println("MONGODB SINK - KAFKA TO MONGODB PIPELINE")
    println("=" * 70)
    println(s"Kafka Broker : $KAFKA_BROKER")
    println(s"Kafka Topic  : $KAFKA_TOPIC")
    println(s"MongoDB URI  : $MONGO_URI")
    println(s"Database     : $DB_NAME")
    println("=" * 70)

    // -------------------------------------------------------------------
    // 1. Connect to MongoDB
    // -------------------------------------------------------------------
    val mongoClient = MongoClient(MONGO_URI)
    val database    = mongoClient.getDatabase(DB_NAME)

    val rfCollection  = database.getCollection("rf_predictions")
    val gbtCollection = database.getCollection("gbt_predictions")
    val lrCollection  = database.getCollection("lr_signals")

    println("\nConnected to MongoDB. Collections:")
    println("  - forex_db.rf_predictions")
    println("  - forex_db.gbt_predictions")
    println("  - forex_db.lr_signals")

    // -------------------------------------------------------------------
    // 2. Set up Kafka Consumer
    // -------------------------------------------------------------------
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(KAFKA_TOPIC))

    println(s"\nSubscribed to Kafka topic: $KAFKA_TOPIC")
    println("Consuming predictions and inserting into MongoDB...\n")

    val mapper = new ObjectMapper()
    var rfCount  = 0
    var gbtCount = 0
    var lrCount  = 0
    var emptyPolls = 0
    val MAX_EMPTY_POLLS = 10  // stop after 10 consecutive empty polls

    // -------------------------------------------------------------------
    // 3. Consume and insert loop
    // -------------------------------------------------------------------
    try {
      while (emptyPolls < MAX_EMPTY_POLLS) {
        val records: ConsumerRecords[String, String] =
          consumer.poll(Duration.ofMillis(2000))

        if (records.isEmpty) {
          emptyPolls += 1
          if (emptyPolls % 5 == 0) {
            println(s"  Waiting for messages... ($emptyPolls/$MAX_EMPTY_POLLS empty polls)")
          }
        } else {
          emptyPolls = 0
          val batch = records.asScala.toList

          // Group by model type for batch inserts
          val rfDocs  = scala.collection.mutable.ArrayBuffer.empty[Document]
          val gbtDocs = scala.collection.mutable.ArrayBuffer.empty[Document]
          val lrDocs  = scala.collection.mutable.ArrayBuffer.empty[Document]

          batch.foreach { record =>
            val json = record.value()
            val node = mapper.readTree(json)
            val model = node.get("model").asText()

            // Build MongoDB document from JSON fields
            val fields = node.fieldNames().asScala.toList
            val kvPairs = fields.map { field =>
              val value = node.get(field).asText()
              // Try to parse as Double for numeric fields
              val bsonValue = try {
                if (field == "Date" || field == "Pair" || field == "model" ||
                    field == "Actual" || field == "Signal") {
                  BsonString(value)
                } else {
                  BsonDouble(value.toDouble)
                }
              } catch {
                case _: NumberFormatException => BsonString(value)
              }
              field -> bsonValue
            }

            val mongoDoc = Document(kvPairs)

            model match {
              case "RandomForest"        => rfDocs  += mongoDoc
              case "GradientBoostedTrees" => gbtDocs += mongoDoc
              case "LogisticRegression"  => lrDocs  += mongoDoc
              case _ =>
            }
          }

          // Batch insert into respective collections
          if (rfDocs.nonEmpty) {
            rfCollection.insertMany(rfDocs).results()
            rfCount += rfDocs.size
          }
          if (gbtDocs.nonEmpty) {
            gbtCollection.insertMany(gbtDocs).results()
            gbtCount += gbtDocs.size
          }
          if (lrDocs.nonEmpty) {
            lrCollection.insertMany(lrDocs).results()
            lrCount += lrDocs.size
          }

          val total = rfCount + gbtCount + lrCount
          if (total % 5000 == 0 || batch.size > 100) {
            println(f"  Inserted: RF=$rfCount%,d  GBT=$gbtCount%,d  LR=$lrCount%,d  (total=$total%,d)")
          }
        }
      }
    } finally {
      consumer.close()
    }

    // -------------------------------------------------------------------
    // 4. Summary
    // -------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("MONGODB SINK - INGESTION COMPLETE")
    println("=" * 70)
    println(s"  RF predictions inserted  : $rfCount")
    println(s"  GBT predictions inserted : $gbtCount")
    println(s"  LR signals inserted      : $lrCount")
    println(s"  Total documents          : ${rfCount + gbtCount + lrCount}")

    // Verify counts in MongoDB
    println("\nMongoDB collection counts:")
    println(s"  rf_predictions  : ${rfCollection.countDocuments().headResult()}")
    println(s"  gbt_predictions : ${gbtCollection.countDocuments().headResult()}")
    println(s"  lr_signals      : ${lrCollection.countDocuments().headResult()}")

    // Show sample documents
    println("\nSample RF prediction:")
    rfCollection.find().limit(1).results().foreach(doc => println(s"  $doc"))

    println("\nSample GBT prediction:")
    gbtCollection.find().limit(1).results().foreach(doc => println(s"  $doc"))

    println("\nSample LR signal:")
    lrCollection.find().limit(1).results().foreach(doc => println(s"  $doc"))

    mongoClient.close()

    println("\n" + "=" * 70)
    println("PIPELINE COMPLETE: Kafka → MongoDB")
    println("=" * 70)
  }
}
