//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-sql:3.3.0"
//> using dep "org.apache.spark::spark-mllib:3.3.0"
//> using dep "org.apache.hadoop:hadoop-client:3.3.2"
//> using dep "org.apache.kafka:kafka-clients:3.4.0"
//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.util=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
//> using javaOpt "-Xmx4g"

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.PipelineModel
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import java.util.{Properties, Collections}
import java.time.Duration
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._
import org.apache.spark.sql.types.{StructType, StructField, DoubleType}
import org.apache.spark.sql.Row

/**
 * RecursivePredictor — 24-Hour Recursive Forex Forecasting Engine
 *
 * Architecture (from the project pipeline document):
 *   Phase 2: Kafka trigger → last row of latest hourly data with features
 *   Phase 3: Load saved RF model → predict h+1 → recalculate features → loop 24x
 *   Phase 4: Each prediction is published to Kafka for live dashboard consumption
 *
 * Flow:
 *   1. Reads TRIGGER message from Kafka topic "forex-trigger" (last row per pair)
 *   2. Loads saved RF h+1 PipelineModel from ml_output/rf_h1_model
 *   3. Runs 24-iteration recursive loop:
 *      - Predict next hour's Close using current features
 *      - Recalculate MA5, MA20, Volatility5 using a sliding price window
 *      - Publish prediction to Kafka topic "forex-predictions-live"
 *      - Feed prediction back as input for the next hour
 *   4. Outputs 24 predicted price points for the live dashboard
 *
 * Usage:
 *   1. Train model first:  scala-cli run SparkMLModels.scala
 *   2. Send trigger:       scala-cli run ForexKafkaProducer.scala --trigger
 *   3. Run this:           scala-cli run RecursivePredictor.scala
 */
object RecursivePredictor {

  val KAFKA_BROKER       = "localhost:9092"
  val TRIGGER_TOPIC      = "forex-trigger"
  val OUTPUT_TOPIC       = "forex-predictions-live"
  val MODEL_PATH         = "ml_output/rf_h1_model"
  val HORIZON            = 24
  val CONSUMER_GROUP     = "recursive-predictor"

  // Feature column order must match training (SparkMLModels.FEATURE_COLS)
  val FEATURE_COLS = Array(
    "Open", "High", "Low", "Close", "Volume",
    "Range", "Change", "ChangePct",
    "MA5", "MA20", "Volatility5",
    "Gain", "Loss",
    "Close_lag1", "Close_lag2", "Close_lag3",
    "Change_lag1", "Change_lag2",
    "MA5_MA20_Ratio"
  )

  def main(args: Array[String]): Unit = {
    println("=" * 70)
    println("RECURSIVE PREDICTOR — 24-HOUR FOREX FORECAST ENGINE")
    println("=" * 70)

    // -----------------------------------------------------------------------
    // 1. Initialize Spark (needed only for model loading / prediction)
    // -----------------------------------------------------------------------
    val spark = SparkSession.builder()
      .appName("Forex Recursive 24h Predictor")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // -----------------------------------------------------------------------
    // 2. Load saved RF h+1 PipelineModel
    // -----------------------------------------------------------------------
    println(s"\nLoading saved model from: $MODEL_PATH")
    val model = PipelineModel.load(MODEL_PATH)
    println("  Model loaded successfully.")

    // -----------------------------------------------------------------------
    // 3. Set up Kafka consumer (reads trigger) and producer (writes predictions)
    // -----------------------------------------------------------------------
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50")

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.ACKS_CONFIG, "1")
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "5")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    val producer = new KafkaProducer[String, String](producerProps)
    val mapper   = new ObjectMapper()

    consumer.subscribe(Collections.singletonList(TRIGGER_TOPIC))
    println(s"Subscribed to trigger topic: $TRIGGER_TOPIC")
    println("Waiting for trigger messages (last-row per pair)...\n")

    // -----------------------------------------------------------------------
    // 4. Consume triggers and run recursive prediction
    // -----------------------------------------------------------------------
    var emptyPolls = 0
    val MAX_EMPTY  = 15  // wait up to ~15 seconds for triggers
    var totalPredictions = 0

    try {
      while (emptyPolls < MAX_EMPTY) {
        val records = consumer.poll(Duration.ofMillis(1000))
        if (records.isEmpty) {
          emptyPolls += 1
          if (emptyPolls % 5 == 0)
            println(s"  Waiting for triggers... ($emptyPolls/$MAX_EMPTY)")
        } else {
          emptyPolls = 0
          records.asScala.foreach { record =>
            val json = record.value()
            val node = mapper.readTree(json)
            val pair = node.get("Pair").asText()
            val date = node.get("Date").asText()

            println(s"\n{'$pair'} Trigger received — Date: $date")
            println(s"  Starting 24-hour recursive prediction...")

            // Parse initial features from trigger
            var currentFeatures = Map[String, Double](
              "Open"        -> node.get("Open").asDouble(),
              "High"        -> node.get("High").asDouble(),
              "Low"         -> node.get("Low").asDouble(),
              "Close"       -> node.get("Close").asDouble(),
              "Volume"      -> node.get("Volume").asDouble(),
              "Range"       -> node.get("Range").asDouble(),
              "Change"      -> node.get("Change").asDouble(),
              "ChangePct"   -> node.get("ChangePct").asDouble(),
              "MA5"         -> node.get("MA5").asDouble(),
              "MA20"        -> node.get("MA20").asDouble(),
              "Volatility5" -> node.get("Volatility5").asDouble(),
              "Gain"        -> node.get("Gain").asDouble(),
              "Loss"        -> node.get("Loss").asDouble(),
              "Close_lag1"  -> node.get("Close_lag1").asDouble(),
              "Close_lag2"  -> node.get("Close_lag2").asDouble(),
              "Close_lag3"  -> node.get("Close_lag3").asDouble(),
              "Change_lag1" -> node.get("Change_lag1").asDouble(),
              "Change_lag2" -> node.get("Change_lag2").asDouble(),
              "MA5_MA20_Ratio" -> node.get("MA5_MA20_Ratio").asDouble()
            )

            // Sliding window of recent prices for MA recalculation
            // Initialize with values from the trigger context
            val recentPrices = scala.collection.mutable.ArrayBuffer[Double](
              currentFeatures("Close_lag3"),
              currentFeatures("Close_lag2"),
              currentFeatures("Close_lag1"),
              currentFeatures("Close")
            )

            // Also track last 20 prices for MA20 (best-effort: pad with current if needed)
            val ma20Prices = scala.collection.mutable.ArrayBuffer.fill(19)(currentFeatures("Close"))
            ma20Prices += currentFeatures("Close")

            // ---------------------------------------------------------------
            // 5. Recursive 24-step prediction loop
            // ---------------------------------------------------------------
            val predictions = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

            for (h <- 1 to HORIZON) {
              // Build a single-row DataFrame with current features
              val featureValues = FEATURE_COLS.map(c => currentFeatures(c))
              val schema = StructType(
                FEATURE_COLS.map(c => StructField(c, DoubleType, nullable = true))
              )
              val row = Row.fromSeq(featureValues.toSeq)
              val rowDF = spark.createDataFrame(
                java.util.Arrays.asList(row), schema
              )

              // Run prediction through the pipeline (VectorAssembler → Scaler → RF)
              val predDF = model.transform(rowDF)
              val predictedClose = predDF.select("prediction").head().getDouble(0)

              predictions += ((h, predictedClose))

              // Publish this prediction to Kafka immediately (for live dashboard)
              val predJson = s"""{
                |"pair":"$pair","hour":$h,"predicted_close":$predictedClose,
                |"trigger_date":"$date","model":"RandomForest_h1_recursive",
                |"current_close":${currentFeatures("Close")},
                |"ma5":${currentFeatures("MA5")},
                |"ma20":${currentFeatures("MA20")},
                |"volatility5":${currentFeatures("Volatility5")}
                |}""".stripMargin.replaceAll("\n", "")

              producer.send(new ProducerRecord[String, String](OUTPUT_TOPIC, pair, predJson))
              totalPredictions += 1

              printf("    h+%-2d  |  Predicted Close: %.5f  (from %.5f)\n",
                h, predictedClose, currentFeatures("Close"))

              // ----- FEATURE EVOLUTION: update features for next iteration -----

              val prevClose = currentFeatures("Close")
              val newChange = predictedClose - prevClose
              val newChangePct = if (prevClose != 0.0) (newChange / prevClose) * 100 else 0.0

              // Update sliding price window
              recentPrices += predictedClose
              ma20Prices += predictedClose

              // Recalculate MA5 (last 5 prices)
              val ma5Window = recentPrices.takeRight(5)
              val newMA5 = ma5Window.sum / ma5Window.length

              // Recalculate MA20 (last 20 prices)
              val ma20Window = ma20Prices.takeRight(20)
              val newMA20 = ma20Window.sum / ma20Window.length

              // Recalculate Volatility5 (stddev of last 5 prices)
              val vol5Mean = ma5Window.sum / ma5Window.length
              val newVol5 = math.sqrt(ma5Window.map(p => math.pow(p - vol5Mean, 2)).sum / ma5Window.length)

              // Update features for next iteration
              currentFeatures = currentFeatures ++ Map(
                "Open"       -> prevClose,  // open = previous close
                "High"       -> math.max(prevClose, predictedClose),
                "Low"        -> math.min(prevClose, predictedClose),
                "Close"      -> predictedClose,
                "Range"      -> math.abs(predictedClose - prevClose),
                "Change"     -> newChange,
                "ChangePct"  -> newChangePct,
                "MA5"        -> newMA5,
                "MA20"       -> newMA20,
                "Volatility5" -> newVol5,
                "Gain"       -> (if (newChange > 0) newChange else 0.0),
                "Loss"       -> (if (newChange < 0) math.abs(newChange) else 0.0),
                "Close_lag3" -> currentFeatures("Close_lag2"),
                "Close_lag2" -> currentFeatures("Close_lag1"),
                "Close_lag1" -> prevClose,
                "Change_lag2" -> currentFeatures("Change_lag1"),
                "Change_lag1" -> newChange,
                "MA5_MA20_Ratio" -> (if (newMA20 != 0) newMA5 / newMA20 else 1.0)
              )
            }

            producer.flush()

            // Print 24-hour prediction summary
            println(s"\n  24-Hour Prediction Curve for $pair:")
            println(f"  ${"Hour"}%-6s ${"Predicted Close"}%-18s ${"Change from h0"}%-15s")
            println("  " + "-" * 40)
            val baseClose = recentPrices(recentPrices.length - HORIZON - 1)
            predictions.foreach { case (h, price) =>
              val change = price - baseClose
              val pct = (change / baseClose) * 100
              println(f"  h+$h%-4d $price%-18.5f ${if (change >= 0) "+" else ""}$pct%.4f%%")
            }
          }
        }
      }

    } finally {
      consumer.close()
      producer.flush()
      producer.close()
    }

    println("\n" + "=" * 70)
    println(s"RECURSIVE PREDICTION COMPLETE — $totalPredictions predictions generated")
    println(s"Output topic: $OUTPUT_TOPIC")
    println("=" * 70)

    spark.stop()
  }
}
