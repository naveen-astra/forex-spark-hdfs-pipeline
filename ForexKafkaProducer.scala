//> using scala "2.12.18"
//> using dep "org.apache.kafka:kafka-clients:3.4.0"
//> using dep "com.fasterxml.jackson.core:jackson-databind:2.15.2"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import java.util.Properties
import scala.io.Source
import scala.util.Try

/**
 * ForexKafkaProducer
 *
 * Simulates a live forex data feed by reading rows from preprocessed
 * CSV files one-by-one and publishing them to a Kafka topic.
 *
 * Topic : forex-live
 * Broker: localhost:9092
 *
 * Start Kafka first:
 *   docker-compose up -d
 *
 * Then run:
 *   scala-cli run ForexKafkaProducer.scala
 */
object ForexKafkaProducer {

  val KAFKA_BROKER   = "localhost:9092"
  val TOPIC_NAME     = "forex-live"
  val DELAY_MS       = 200L   // milliseconds between messages (simulates real-time feed)

  // Pairs and timeframes to stream
  val PAIRS      = List("EURUSD", "GBPUSD", "USDJPY", "USDCAD", "AUDUSD")
  val TIMEFRAMES = List("1d")  // Use 1d for demo; switch to 1h or 1min for volume

  def main(args: Array[String]): Unit = {

    println("=" * 70)
    println("FOREX KAFKA PRODUCER - LIVE FEED SIMULATION")
    println("=" * 70)
    println(s"Broker : $KAFKA_BROKER")
    println(s"Topic  : $TOPIC_NAME")
    println(s"Pairs  : ${PAIRS.mkString(", ")}")
    println(s"Delay  : ${DELAY_MS}ms per message")
    println("=" * 70)

    // Kafka producer configuration
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5")

    val producer = new KafkaProducer[String, String](props)

    var totalSent = 0
    val startTime = System.currentTimeMillis()

    // Add JVM shutdown hook to cleanly close producer
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println(s"\nShutting down... Total messages sent: $totalSent")
      producer.flush()
      producer.close()
    }))

    try {
      // Stream each pair × timeframe combination
      for {
        pair <- PAIRS
        tf   <- TIMEFRAMES
      } {
        val filePath = s"spark_output/${pair}_${tf}_processed.csv"
        val file     = new java.io.File(filePath)

        if (!file.exists()) {
          println(s"[SKIP] $filePath not found")
        } else {
          println(s"\n[START] Streaming $pair $tf from $filePath")

          val source = Source.fromFile(file)
          try {
            val lines  = source.getLines().toList
            val header = lines.head
            val rows   = lines.tail

            println(s"        ${rows.length} records to stream")

            rows.foreach { line =>
              // Build JSON message
              val fields = header.split(",")
              val values = line.split(",", -1)

              val json = fields.zip(values).map { case (k, v) =>
                s""""${k.trim}":"${v.trim}""""
              }.mkString("{", ",", "}")

              // Key = pair (enables Kafka partitioning by pair)
              val record = new ProducerRecord[String, String](TOPIC_NAME, pair, json)

              Try(producer.send(record).get()) match {
                case scala.util.Success(meta) =>
                  totalSent += 1
                  if (totalSent % 100 == 0)
                    println(s"  [${pair}] Sent $totalSent messages | " +
                      s"partition=${meta.partition()} offset=${meta.offset()}")
                case scala.util.Failure(ex) =>
                  println(s"  [ERROR] Failed to send: ${ex.getMessage}")
              }

              Thread.sleep(DELAY_MS)
            }

            println(s"[DONE] $pair $tf - all records sent")
          } finally {
            source.close()
          }
        }
      }

    } finally {
      producer.flush()
      producer.close()
      val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
      println("\n" + "=" * 70)
      println(s"PRODUCER COMPLETE")
      println(s"Total messages sent : $totalSent")
      println(s"Elapsed time        : ${elapsed}s")
      println(s"Topic               : $TOPIC_NAME")
      println(s"Kafka UI            : http://localhost:8080")
      println("=" * 70)
    }
  }
}
