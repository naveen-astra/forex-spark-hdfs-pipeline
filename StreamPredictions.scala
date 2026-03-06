//> using scala "2.12.18"
//> using dep "org.apache.kafka:kafka-clients:3.4.0"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import java.util.Properties
import scala.io.Source

/**
 * StreamPredictions
 *
 * Reads ML model prediction CSVs from ml_output/ directory and streams
 * them to Kafka topic "forex-predictions" for downstream consumption
 * (e.g. MongoDBSink).
 *
 * Run after SparkMLModels.scala has completed:
 *   scala-cli run StreamPredictions.scala
 */
object StreamPredictions {

  val KAFKA_BROKER = "localhost:9092"
  val TOPIC_NAME   = "forex-predictions"
  val DELAY_MS     = 50L  // delay between messages for controlled streaming

  def main(args: Array[String]): Unit = {

    println("=" * 70)
    println("STREAM ML PREDICTIONS TO KAFKA")
    println("=" * 70)
    println(s"Broker : $KAFKA_BROKER")
    println(s"Topic  : $TOPIC_NAME")
    println("=" * 70)

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")

    val producer = new KafkaProducer[String, String](props)
    var totalSent = 0

    // Stream RF predictions (try new name first, fallback to old)
    totalSent += streamCsvToKafka(producer,
      if (new java.io.File("ml_output/rf_24h_predictions.csv").exists()) "ml_output/rf_24h_predictions.csv"
      else "ml_output/rf_predictions.csv", "RandomForest")

    // Stream GBT predictions
    totalSent += streamCsvToKafka(producer,
      if (new java.io.File("ml_output/gbt_24h_predictions.csv").exists()) "ml_output/gbt_24h_predictions.csv"
      else "ml_output/gbt_predictions.csv", "GradientBoostedTrees")

    // Stream LR signals
    totalSent += streamCsvToKafka(producer,
      if (new java.io.File("ml_output/lr_24h_signals.csv").exists()) "ml_output/lr_24h_signals.csv"
      else "ml_output/lr_signals.csv", "LogisticRegression")

    producer.flush()
    producer.close()

    println("\n" + "=" * 70)
    println(s"STREAMING COMPLETE: $totalSent total records sent to '$TOPIC_NAME'")
    println("=" * 70)
  }

  def streamCsvToKafka(producer: KafkaProducer[String, String],
                       filePath: String,
                       modelName: String): Int = {
    val file = new java.io.File(filePath)
    if (!file.exists()) {
      println(s"\n[SKIP] $filePath not found")
      return 0
    }

    println(s"\n[STREAM] $filePath ($modelName)")
    val source = Source.fromFile(file)
    try {
      val lines  = source.getLines().toList
      val header = lines.head.split(",").map(_.trim)
      val rows   = lines.tail

      println(s"         ${rows.length} records to stream")

      var sent = 0
      rows.foreach { line =>
        val values = line.split(",", -1)
        // Build JSON
        val jsonFields = header.zip(values).map { case (k, v) =>
          s""""${k.trim}":"${v.trim}""""
        }
        val json = s"""{"model":"$modelName",${jsonFields.mkString(",")}}"""

        // Use Pair as key for partitioning
        val key = if (header.contains("Pair") && values.length > header.indexOf("Pair")) {
          values(header.indexOf("Pair")).trim
        } else modelName

        val record = new ProducerRecord[String, String](TOPIC_NAME, key, json)
        producer.send(record)
        sent += 1

        if (sent % 10000 == 0) {
          println(f"         Sent $sent%,d / ${rows.length}%,d records")
        }
        if (DELAY_MS > 0 && sent % 100 == 0) Thread.sleep(DELAY_MS)
      }

      println(f"         Done: $sent%,d records streamed")
      sent
    } finally {
      source.close()
    }
  }
}
