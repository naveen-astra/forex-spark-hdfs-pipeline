//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-sql:3.5.1"
//> using dep "org.apache.spark::spark-sql-kafka-0-10:3.5.1"
//> using dep "org.apache.hadoop:hadoop-client:3.3.6"
//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.util=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.nio=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"
//> using javaOpt "-Djava.security.manager=allow"
//> using javaOpt "-Djava.library.path=C:\\winutils\\bin"

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._

/**
 * SparkKafkaStreaming
 *
 * Spark Structured Streaming consumer that reads live forex data
 * from Kafka topic "forex-live", applies real-time transformations
 * (moving average signals, volatility alerts, trading signals),
 * and outputs results to the console and streaming_output/ directory.
 *
 * Prerequisites:
 *   1. Start Kafka:   docker-compose up -d
 *   2. Start producer in another terminal: scala-cli run ForexKafkaProducer.scala
 *   3. Then run this: scala-cli run SparkKafkaStreaming.scala
 *
 * Spark UI: http://localhost:4040
 * Kafka UI: http://localhost:8080
 */
object SparkKafkaStreaming {

  val KAFKA_BROKER = "localhost:9092"
  val TOPIC_NAME   = "forex-live"

  // Schema of each JSON message arriving from the Kafka producer
  val MESSAGE_SCHEMA = new StructType()
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

  def main(args: Array[String]): Unit = {

    // Windows: point Hadoop native libs (hadoop.dll) for NativeIO
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    System.setProperty("java.library.path", "C:\\winutils\\bin")

    // Create output directories
    new java.io.File("streaming_output/ticks").mkdirs()
    new java.io.File("streaming_output/checkpoints/tick").mkdirs()
    new java.io.File("streaming_output/checkpoints/window").mkdirs()

    val spark = SparkSession.builder()
      .appName("Forex Spark Structured Streaming - Kafka KRaft")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.shuffle.partitions", "4")
      // Windows native IO - load hadoop.dll from HADOOP_HOME/bin
      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.RawLocalFs")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("=" * 70)
    println("SPARK STRUCTURED STREAMING - KAFKA FOREX FEED")
    println("=" * 70)
    println(s"Kafka Broker : $KAFKA_BROKER")
    println(s"Topic        : $TOPIC_NAME")
    println(s"Spark UI     : http://localhost:4040")
    println("=" * 70 + "\n")

    // -----------------------------------------------------------------------
    // 1. Read stream from Kafka
    // -----------------------------------------------------------------------
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BROKER)
      .option("subscribe", TOPIC_NAME)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    // -----------------------------------------------------------------------
    // 2. Parse JSON messages into typed columns
    // -----------------------------------------------------------------------
    val parsedStream = kafkaStream
      .selectExpr("CAST(key AS STRING) AS pair_key",
                  "CAST(value AS STRING) AS json_value",
                  "timestamp AS kafka_timestamp")
      .withColumn("data", from_json(col("json_value"), MESSAGE_SCHEMA))
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
        col("data.MA5").cast(DoubleType).alias("MA5"),
        col("data.MA20").cast(DoubleType).alias("MA20"),
        col("data.Volatility5").cast(DoubleType).alias("Volatility5"),
        col("data.ChangePct").cast(DoubleType).alias("ChangePct"),
        col("data.Change").cast(DoubleType).alias("Change")
      )
      .filter(col("Close").isNotNull)

    // -----------------------------------------------------------------------
    // 3. Real-time feature computation and signal generation
    // -----------------------------------------------------------------------
    val enrichedStream = parsedStream
      // MA crossover signal: BUY when short-term MA > long-term MA
      .withColumn("MA_Signal",
        when(col("MA5") > col("MA20"), "BUY")
        .when(col("MA5") < col("MA20"), "SELL")
        .otherwise("HOLD"))

      // Volatility alert: flag if volatility is high
      .withColumn("Volatility_Alert",
        when(col("Volatility5") > 0.01, "HIGH_VOLATILITY")
        .otherwise("NORMAL"))

      // Momentum: classify price change direction
      .withColumn("Momentum",
        when(col("ChangePct") > 0.5,  "STRONG_UP")
        .when(col("ChangePct") > 0.0,  "WEAK_UP")
        .when(col("ChangePct") < -0.5, "STRONG_DOWN")
        .when(col("ChangePct") < 0.0,  "WEAK_DOWN")
        .otherwise("FLAT"))

      // Spread (High - Low as percentage of Close)
      .withColumn("Spread_Pct",
        round((col("High") - col("Low")) / col("Close") * 100, 4))

      // RSI approximation signal (using Gain/Loss ratio trend)
      .withColumn("Price_Level",
        when(col("Close") > col("MA20"), "ABOVE_MA20")
        .otherwise("BELOW_MA20"))

    // -----------------------------------------------------------------------
    // 4. Windowed aggregation - rolling 5-minute statistics per pair
    // -----------------------------------------------------------------------
    val windowedStats = parsedStream
      .withWatermark("kafka_timestamp", "1 minute")
      .groupBy(
        window(col("kafka_timestamp"), "5 minutes", "1 minute"),
        col("Pair")
      )
      .agg(
        count("*").alias("Record_Count"),
        round(avg("Close"),    5).alias("Avg_Close"),
        round(min("Close"),    5).alias("Min_Close"),
        round(max("Close"),    5).alias("Max_Close"),
        round(avg("ChangePct"), 4).alias("Avg_Change_Pct"),
        round(avg("Volatility5"), 6).alias("Avg_Volatility"),
        round(stddev("Close"), 6).alias("Price_StdDev")
      )

    // -----------------------------------------------------------------------
    // 5. Output 1 - Console: enriched tick-level stream
    // -----------------------------------------------------------------------
    val consoleQuery: StreamingQuery = enrichedStream
      .select(
        "Date", "Pair", "Close",
        "MA5", "MA20",
        "MA_Signal", "Momentum",
        "Volatility_Alert", "Spread_Pct", "Price_Level"
      )
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "20")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .queryName("forex_tick_stream")
      .start()

    // -----------------------------------------------------------------------
    // 6. Output 2 - Console: windowed 5-minute aggregated stats
    // -----------------------------------------------------------------------
    val windowQuery: StreamingQuery = windowedStats
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .option("numRows", "30")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .queryName("forex_window_stats")
      .start()

    // -----------------------------------------------------------------------
    // 7. Output 3 - In-memory sink: live query-able table (no NativeIO)
    // -----------------------------------------------------------------------
    val memoryQuery: StreamingQuery = enrichedStream
      .select("Date", "Pair", "Timeframe", "Open", "High", "Low",
              "Close", "Volume", "MA5", "MA20", "Volatility5",
              "ChangePct", "MA_Signal", "Momentum", "Volatility_Alert",
              "Spread_Pct", "Price_Level")
      .writeStream
      .outputMode("append")
      .format("memory")
      .queryName("forex_live_mem")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    println("Streaming queries started:")
    println("  [1] forex_tick_stream   - console output every 10s")
    println("  [2] forex_window_stats  - windowed aggregation every 30s")
    println("  [3] forex_live_mem      - in-memory table (queryable at Spark UI)")
    println("\nWaiting for data from Kafka topic: " + TOPIC_NAME)
    println("Start the producer: scala-cli run ForexKafkaProducer.scala")
    println("\nPress Ctrl+C to stop.\n")

    // Wait for all queries to terminate
    spark.streams.awaitAnyTermination()
  }
}
