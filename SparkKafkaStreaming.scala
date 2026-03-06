//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-sql:3.3.0"
//> using dep "org.apache.spark::spark-mllib:3.3.0"
//> using dep "org.apache.spark::spark-sql-kafka-0-10:3.3.0"
//> using dep "org.apache.hadoop:hadoop-client:3.3.2"
//> using dep "org.apache.kafka:kafka-clients:3.4.0"
//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.util=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.nio=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"
//> using javaOpt "-Djava.security.manager=allow"
//> using javaOpt "-Djava.library.path=C:\\winutils\\bin"
//> using javaOpt "-Xmx4g"

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.ml.PipelineModel
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.TopicPartition
import java.util.{Properties, Collections}
import java.time.Duration
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._

/**
 * SparkKafkaStreaming — Recursive 24-Hour Forex Prediction via Structured Streaming
 *
 * Architecture (Store & Retrieve pattern via Kafka):
 *   1. Reads TRIGGER data from Kafka topic "forex-live" (last row per pair)
 *   2. Loads saved RF h+1 PipelineModel from ml_output/rf_h1_model
 *   3. In foreachBatch, for each trigger:
 *      - Predict h+1 using the loaded model
 *      - STORE: Push prediction to Kafka "forex-predictions"
 *      - RETRIEVE: Read prediction back from Kafka to verify & update features
 *      - Repeat for 24 hours (recursive loop)
 *   4. Second streaming query monitors "forex-predictions" for dashboard
 *
 * Prerequisites:
 *   1. Train model:   scala-cli run SparkMLModels.scala
 *   2. Start Kafka:   KRaft mode (C:\kafka)
 *   3. Run this:      scala-cli run SparkKafkaStreaming.scala
 *   4. Send trigger:  scala-cli run ForexKafkaProducer.scala
 *
 * Spark UI: http://localhost:4040
 */
object SparkKafkaStreaming {

  val KAFKA_BROKER      = "localhost:9092"
  val LIVE_TOPIC        = "forex-live"          // Input:  trigger data from producer
  val PREDICTIONS_TOPIC = "forex-predictions"   // Store & Retrieve: recursive predictions
  val MODEL_PATH        = "ml_output/rf_h1_model"
  val HORIZON           = 24
  val CONSUMER_GROUP    = "streaming-predictor"

  // Feature columns — must match SparkMLModels.FEATURE_COLS
  val FEATURE_COLS = Array(
    "Open", "High", "Low", "Close", "Volume",
    "Range", "Change", "ChangePct",
    "MA5", "MA20", "Volatility5",
    "Gain", "Loss",
    "Close_lag1", "Close_lag2", "Close_lag3",
    "Change_lag1", "Change_lag2",
    "MA5_MA20_Ratio"
  )

  // Schema for trigger messages (last-row with lag features from ForexKafkaProducer)
  val TRIGGER_SCHEMA = new StructType()
    .add("Date",        StringType)
    .add("Pair",        StringType)
    .add("Timeframe",   StringType)
    .add("Open",        StringType)
    .add("High",        StringType)
    .add("Low",         StringType)
    .add("Close",       StringType)
    .add("Volume",      StringType)
    .add("Range",       StringType)
    .add("Change",      StringType)
    .add("ChangePct",   StringType)
    .add("MA5",         StringType)
    .add("MA20",        StringType)
    .add("Volatility5", StringType)
    .add("Gain",        StringType)
    .add("Loss",        StringType)
    .add("Close_lag1",  StringType)
    .add("Close_lag2",  StringType)
    .add("Close_lag3",  StringType)
    .add("Change_lag1", StringType)
    .add("Change_lag2", StringType)
    .add("MA5_MA20_Ratio", StringType)

  def main(args: Array[String]): Unit = {

    // Windows native libs
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("java.library.path", "C:\\winutils\\bin")

    // Create checkpoint directories
    new java.io.File("streaming_output/checkpoints/predict").mkdirs()
    new java.io.File("streaming_output/checkpoints/monitor").mkdirs()

    println("=" * 70)
    println("SPARK STREAMING — RECURSIVE 24-HOUR FOREX PREDICTOR")
    println("Store & Retrieve via Kafka")
    println("=" * 70)

    // -----------------------------------------------------------------
    // 1. Initialize Spark with ML support
    // -----------------------------------------------------------------
    val spark = SparkSession.builder()
      .appName("Forex Streaming Recursive Predictor")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.file.impl",
              "org.apache.hadoop.fs.local.RawLocalFs")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///D:/tmp/spark-events")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println(s"\nKafka Broker  : $KAFKA_BROKER")
    println(s"Input Topic   : $LIVE_TOPIC")
    println(s"Store/Retrieve: $PREDICTIONS_TOPIC")
    println(s"Model Path    : $MODEL_PATH")
    println(s"Spark UI      : http://localhost:4040\n")

    // -----------------------------------------------------------------
    // 2. Load saved RF h+1 PipelineModel (trained by SparkMLModels)
    // -----------------------------------------------------------------
    println("Loading RF h+1 model from: " + MODEL_PATH)
    val model = PipelineModel.load(MODEL_PATH)
    println("  Model loaded. Stages: " +
      model.stages.map(_.getClass.getSimpleName).mkString(" → "))

    // -----------------------------------------------------------------
    // 3. Kafka producer (STORE) and consumer (RETRIEVE)
    // -----------------------------------------------------------------
    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all")
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0")

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP + "-retrieve")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    val kafkaProducer = new KafkaProducer[String, String](producerProps)
    val kafkaConsumer = new KafkaConsumer[String, String](consumerProps)
    val mapper = new ObjectMapper()

    // -----------------------------------------------------------------
    // 4. Read trigger stream from Kafka topic "forex-live"
    // -----------------------------------------------------------------
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BROKER)
      .option("subscribe", LIVE_TOPIC)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    val triggerStream = kafkaStream
      .selectExpr("CAST(value AS STRING) AS json_value",
                   "timestamp AS kafka_timestamp")
      .withColumn("data", from_json(col("json_value"), TRIGGER_SCHEMA))
      .filter(col("data").isNotNull)
      .select(
        col("kafka_timestamp"),
        col("data.Date").alias("Date"),
        col("data.Pair").alias("Pair"),
        col("data.Timeframe").alias("Timeframe"),
        col("data.Open").cast(DoubleType).alias("Open"),
        col("data.High").cast(DoubleType).alias("High"),
        col("data.Low").cast(DoubleType).alias("Low"),
        col("data.Close").cast(DoubleType).alias("Close"),
        col("data.Volume").cast(DoubleType).alias("Volume"),
        col("data.Range").cast(DoubleType).alias("Range"),
        col("data.Change").cast(DoubleType).alias("Change"),
        col("data.ChangePct").cast(DoubleType).alias("ChangePct"),
        col("data.MA5").cast(DoubleType).alias("MA5"),
        col("data.MA20").cast(DoubleType).alias("MA20"),
        col("data.Volatility5").cast(DoubleType).alias("Volatility5"),
        col("data.Gain").cast(DoubleType).alias("Gain"),
        col("data.Loss").cast(DoubleType).alias("Loss"),
        col("data.Close_lag1").cast(DoubleType).alias("Close_lag1"),
        col("data.Close_lag2").cast(DoubleType).alias("Close_lag2"),
        col("data.Close_lag3").cast(DoubleType).alias("Close_lag3"),
        col("data.Change_lag1").cast(DoubleType).alias("Change_lag1"),
        col("data.Change_lag2").cast(DoubleType).alias("Change_lag2"),
        col("data.MA5_MA20_Ratio").cast(DoubleType).alias("MA5_MA20_Ratio")
      )
      .filter(col("Close").isNotNull && col("Close_lag1").isNotNull)

    // -----------------------------------------------------------------
    // 5. foreachBatch: Recursive 24-step prediction with Store & Retrieve
    //
    //    For each trigger message:
    //      Loop h = 1 to 24:
    //        1. Build feature row → model.transform() → predicted close
    //        2. STORE:    Push prediction to Kafka "forex-predictions"
    //        3. RETRIEVE: Read prediction back from Kafka (exact offset)
    //        4. Use retrieved value to recalculate features for h+1
    // -----------------------------------------------------------------
    var totalPredictions = 0

    val predictionQuery = triggerStream
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "streaming_output/checkpoints/predict")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          val triggers = batchDF.collect()
          println(s"\n${"=" * 60}")
          println(s"Batch $batchId: ${triggers.length} trigger(s) received")
          println("=" * 60)

          triggers.foreach { triggerRow =>
            val pair = triggerRow.getAs[String]("Pair")
            val date = triggerRow.getAs[String]("Date")

            println(s"\n  [$pair] Trigger — Date: $date, Close: ${triggerRow.getAs[Double]("Close")}")
            println("  24-step recursive prediction (Store & Retrieve via Kafka):\n")

            // Parse initial features from the trigger
            var currentFeatures = Map[String, Double](
              "Open"           -> triggerRow.getAs[Double]("Open"),
              "High"           -> triggerRow.getAs[Double]("High"),
              "Low"            -> triggerRow.getAs[Double]("Low"),
              "Close"          -> triggerRow.getAs[Double]("Close"),
              "Volume"         -> triggerRow.getAs[Double]("Volume"),
              "Range"          -> triggerRow.getAs[Double]("Range"),
              "Change"         -> triggerRow.getAs[Double]("Change"),
              "ChangePct"      -> triggerRow.getAs[Double]("ChangePct"),
              "MA5"            -> triggerRow.getAs[Double]("MA5"),
              "MA20"           -> triggerRow.getAs[Double]("MA20"),
              "Volatility5"    -> triggerRow.getAs[Double]("Volatility5"),
              "Gain"           -> triggerRow.getAs[Double]("Gain"),
              "Loss"           -> triggerRow.getAs[Double]("Loss"),
              "Close_lag1"     -> triggerRow.getAs[Double]("Close_lag1"),
              "Close_lag2"     -> triggerRow.getAs[Double]("Close_lag2"),
              "Close_lag3"     -> triggerRow.getAs[Double]("Close_lag3"),
              "Change_lag1"    -> triggerRow.getAs[Double]("Change_lag1"),
              "Change_lag2"    -> triggerRow.getAs[Double]("Change_lag2"),
              "MA5_MA20_Ratio" -> triggerRow.getAs[Double]("MA5_MA20_Ratio")
            )

            // Sliding price windows for MA recalculation
            val recentPrices = scala.collection.mutable.ArrayBuffer[Double](
              currentFeatures("Close_lag3"),
              currentFeatures("Close_lag2"),
              currentFeatures("Close_lag1"),
              currentFeatures("Close")
            )
            val ma20Prices = scala.collection.mutable.ArrayBuffer.fill(19)(
              currentFeatures("Close")
            )
            ma20Prices += currentFeatures("Close")

            val baseClose = currentFeatures("Close")

            // ------- Recursive 24-step prediction loop -------
            for (h <- 1 to HORIZON) {

              // Build single-row DataFrame with current features
              val featureValues = FEATURE_COLS.map(c => currentFeatures(c))
              val schema = StructType(
                FEATURE_COLS.map(c => StructField(c, DoubleType, nullable = true))
              )
              val rowDF = spark.createDataFrame(
                java.util.Arrays.asList(Row.fromSeq(featureValues.toSeq)), schema
              )

              // Predict next-hour Close
              val predDF = model.transform(rowDF)
              val predictedClose = predDF.select("prediction").head().getDouble(0)

              // -------- STORE: Push prediction to Kafka --------
              val predJson = s"""{"pair":"$pair","hour":$h,"predicted_close":$predictedClose,""" +
                s""""trigger_date":"$date","model":"RF_h1_recursive",""" +
                s""""base_close":$baseClose,"current_close":${currentFeatures("Close")},""" +
                s""""ma5":${currentFeatures("MA5")},"ma20":${currentFeatures("MA20")},""" +
                s""""volatility5":${currentFeatures("Volatility5")}}"""

              val sendFuture = kafkaProducer.send(
                new ProducerRecord[String, String](PREDICTIONS_TOPIC, pair, predJson)
              )
              kafkaProducer.flush()
              val metadata = sendFuture.get()  // block until stored

              // -------- RETRIEVE: Read prediction back from Kafka --------
              val tp = new TopicPartition(PREDICTIONS_TOPIC, metadata.partition())
              kafkaConsumer.assign(Collections.singletonList(tp))
              kafkaConsumer.seek(tp, metadata.offset())
              val records = kafkaConsumer.poll(Duration.ofMillis(2000))

              var retrievedClose = predictedClose  // fallback
              records.asScala.foreach { rec =>
                if (rec.offset() == metadata.offset()) {
                  val node = mapper.readTree(rec.value())
                  retrievedClose = node.get("predicted_close").asDouble()
                }
              }

              printf("    h+%-2d  |  Stored: %.5f  |  Retrieved: %.5f  |  from: %.5f\n",
                h, predictedClose, retrievedClose, currentFeatures("Close"))

              totalPredictions += 1

              // -------- Feature evolution using RETRIEVED value --------
              val prevClose = currentFeatures("Close")
              val newChange = retrievedClose - prevClose
              val newChangePct = if (prevClose != 0.0)
                (newChange / prevClose) * 100 else 0.0

              recentPrices += retrievedClose
              ma20Prices += retrievedClose

              val ma5Window  = recentPrices.takeRight(5)
              val newMA5     = ma5Window.sum / ma5Window.length
              val ma20Window = ma20Prices.takeRight(20)
              val newMA20    = ma20Window.sum / ma20Window.length

              val vol5Mean = ma5Window.sum / ma5Window.length
              val newVol5 = math.sqrt(
                ma5Window.map(p => math.pow(p - vol5Mean, 2)).sum / ma5Window.length
              )

              currentFeatures = currentFeatures ++ Map(
                "Open"           -> prevClose,
                "High"           -> math.max(prevClose, retrievedClose),
                "Low"            -> math.min(prevClose, retrievedClose),
                "Close"          -> retrievedClose,
                "Range"          -> math.abs(retrievedClose - prevClose),
                "Change"         -> newChange,
                "ChangePct"      -> newChangePct,
                "MA5"            -> newMA5,
                "MA20"           -> newMA20,
                "Volatility5"    -> newVol5,
                "Gain"           -> (if (newChange > 0) newChange else 0.0),
                "Loss"           -> (if (newChange < 0) math.abs(newChange) else 0.0),
                "Close_lag3"     -> currentFeatures("Close_lag2"),
                "Close_lag2"     -> currentFeatures("Close_lag1"),
                "Close_lag1"     -> prevClose,
                "Change_lag2"    -> currentFeatures("Change_lag1"),
                "Change_lag1"    -> newChange,
                "MA5_MA20_Ratio" -> (if (newMA20 != 0) newMA5 / newMA20 else 1.0)
              )
            }

            // 24-hour prediction summary
            println(s"\n  [$pair] 24-hour recursive prediction complete.")
            println(f"  Base close: $baseClose%.5f  →  Final: ${currentFeatures("Close")}%.5f")
            val totalChange = currentFeatures("Close") - baseClose
            val totalPct = (totalChange / baseClose) * 100
            println(f"  Total change: ${if (totalChange >= 0) "+" else ""}$totalPct%.4f%%\n")
          }
        }
      }
      .start()

    // -----------------------------------------------------------------
    // 6. Monitor query: Read predictions from "forex-predictions"
    //    (This is the RETRIEVE side visible to the dashboard / other consumers)
    // -----------------------------------------------------------------
    val monitorStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BROKER)
      .option("subscribe", PREDICTIONS_TOPIC)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING) AS pair",
                   "CAST(value AS STRING) AS prediction_json")

    val monitorQuery = monitorStream
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "30")
      .option("checkpointLocation", "streaming_output/checkpoints/monitor")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .queryName("prediction_monitor")
      .start()

    println("Streaming queries started:")
    println("  [1] recursive_predictor  — foreachBatch with Store & Retrieve via Kafka")
    println("  [2] prediction_monitor   — console display of forex-predictions topic")
    println(s"\nWaiting for triggers from topic: $LIVE_TOPIC")
    println("Send trigger: scala-cli run ForexKafkaProducer.scala")
    println("Press Ctrl+C to stop.\n")

    spark.streams.awaitAnyTermination()

    kafkaProducer.close()
    kafkaConsumer.close()
    spark.stop()
  }
}
