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
 * Sends the last row per currency pair (with computed lag features)
 * to Kafka topic "forex-live" to trigger the 24-hour recursive forecast
 * in SparkKafkaStreaming.
 *
 * Default : Send last row per pair → forex-live (trigger for recursive prediction)
 * --full  : Stream all rows for live feed simulation
 *
 * Topic : forex-live
 * Broker: localhost:9092
 *
 * Usage:
 *   scala-cli run ForexKafkaProducer.scala           (default: last-row trigger)
 *   scala-cli run ForexKafkaProducer.scala -- --full  (full stream simulation)
 */
object ForexKafkaProducer {

  val KAFKA_BROKER   = "localhost:9092"
  val TOPIC_NAME     = "forex-live"
  val DELAY_MS       = 200L   // milliseconds between messages (simulates real-time feed)

  // Pairs and timeframes to stream
  val PAIRS      = List("EURUSD", "GBPUSD", "USDJPY", "USDCAD", "AUDUSD")
  val TIMEFRAMES = List("1d")  // Use 1d for demo; switch to 1h or 1min for volume

  def main(args: Array[String]): Unit = {

    val fullMode = args.contains("--full")

    println("=" * 70)
    if (fullMode)
      println("FOREX KAFKA PRODUCER - FULL STREAM SIMULATION")
    else
      println("FOREX KAFKA PRODUCER - TRIGGER MODE (Last Row → Recursive Predictor)")
    println("=" * 70)
    println(s"Broker : $KAFKA_BROKER")
    println(s"Topic  : $TOPIC_NAME")
    println(s"Pairs  : ${PAIRS.mkString(", ")}")
    if (fullMode) println(s"Delay  : ${DELAY_MS}ms per message")
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
      if (fullMode) {
      // ---------------------------------------------------------------
      // FULL MODE (--full): Stream all rows (live feed simulation)
      // ---------------------------------------------------------------
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
              val fields = header.split(",")
              val values = line.split(",", -1)

              val json = fields.zip(values).map { case (k, v) =>
                s""""${k.trim}":"${v.trim}""""
              }.mkString("{", ",", "}")

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
      } else {
        // ---------------------------------------------------------------
        // DEFAULT: Send last row per pair (trigger for recursive prediction)
        // Sends to forex-live with computed lag features for SparkKafkaStreaming
        // ---------------------------------------------------------------
        val triggerPairs = List("EURUSD", "GBPUSD", "USDJPY", "USDCAD", "AUDUSD",
          "AUDJPY", "EURAUD", "EURCHF", "EURGBP", "EURJPY", "GBPJPY")

        for (pair <- triggerPairs) {
          val filePath = s"spark_output/${pair}_1h_processed.csv"
          val file     = new java.io.File(filePath)

          if (!file.exists()) {
            println(s"[SKIP] $filePath not found")
          } else {
            val source = Source.fromFile(file)
            try {
              val lines  = source.getLines().toList
              val header = lines.head.split(",").map(_.trim)
              val rows   = lines.tail

              // Get last 4 rows for lag features
              val lastRows = rows.takeRight(4).map(_.split(",", -1).map(_.trim))

              if (lastRows.length >= 4) {
                val latest = lastRows(3)  // row at t
                val lag1   = lastRows(2)  // row at t-1
                val lag2   = lastRows(1)  // row at t-2
                val lag3   = lastRows(0)  // row at t-3

                def colVal(row: Array[String], colName: String): String = {
                  val idx = header.indexOf(colName)
                  if (idx >= 0 && idx < row.length) row(idx) else "0.0"
                }

                val closeLag1 = colVal(lag1, "Close")
                val closeLag2 = colVal(lag2, "Close")
                val closeLag3 = colVal(lag3, "Close")
                val changeLag1 = colVal(lag1, "Change")
                val changeLag2 = colVal(lag2, "Change")
                val ma5Val = colVal(latest, "MA5")
                val ma20Val = colVal(latest, "MA20")
                val ma5Ratio = if (ma20Val.toDouble != 0) (ma5Val.toDouble / ma20Val.toDouble).toString else "1.0"

                val kvPairs = header.zip(latest).map { case (k, v) =>
                  s""""$k":"$v""""
                }.mkString(",")

                val lagFields = s""""Close_lag1":"$closeLag1","Close_lag2":"$closeLag2","Close_lag3":"$closeLag3",""" +
                  s""""Change_lag1":"$changeLag1","Change_lag2":"$changeLag2","MA5_MA20_Ratio":"$ma5Ratio""""

                val json = s"{$kvPairs,$lagFields}"

                val record = new ProducerRecord[String, String](TOPIC_NAME, pair, json)
                Try(producer.send(record).get()) match {
                  case scala.util.Success(meta) =>
                    totalSent += 1
                    println(s"  [TRIGGER] $pair → partition=${meta.partition()} offset=${meta.offset()}")
                    println(s"            Date=${colVal(latest, "Date")} Close=${colVal(latest, "Close")}")
                  case scala.util.Failure(ex) =>
                    println(s"  [ERROR] $pair: ${ex.getMessage}")
                }
              }
            } finally {
              source.close()
            }
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
