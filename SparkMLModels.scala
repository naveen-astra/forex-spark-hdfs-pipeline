//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-sql:3.3.0"
//> using dep "org.apache.spark::spark-mllib:3.3.0"
//> using dep "org.apache.hadoop:hadoop-client:3.3.2"
//> using dep "org.apache.kafka:kafka-clients:3.4.0"
//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.util=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
//> using javaOpt "-Xmx8g"

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.regression.{RandomForestRegressor, GBTRegressor, RandomForestRegressionModel, GBTRegressionModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.{RegressionEvaluator, MulticlassClassificationEvaluator, BinaryClassificationEvaluator}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

object SparkMLModels {

  val FEATURE_COLS = Array(
    "Open", "High", "Low", "Close", "Volume",
    "Range", "Change", "ChangePct",
    "MA5", "MA20", "Volatility5",
    "Gain", "Loss",
    "Close_lag1", "Close_lag2", "Close_lag3",
    "Change_lag1", "Change_lag2",
    "MA5_MA20_Ratio"
  )

  val HORIZON = 24  // predict next 24 hours

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Forex SparkML - 24h Hourly Price Prediction")
      .master("local[*]")
      .config("spark.driver.memory", "8g")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("=" * 70)
    println("SPARK ML MODELS - NEXT 24-HOUR FOREX PRICE PREDICTION")
    println("=" * 70)

    // -----------------------------------------------------------------------
    // 1. Load and combine all 15 hourly pair files
    // -----------------------------------------------------------------------
    val pairs = List(
      "AUDJPY", "AUDUSD", "EURAUD", "EURCHF", "EURGBP",
      "EURJPY", "EURUSD", "GBPAUD", "GBPCAD", "GBPJPY",
      "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY"
    )

    println(s"\nLoading preprocessed hourly data for ${pairs.length} pairs...")

    val hourlyDFs = pairs.flatMap { pair =>
      val path = s"spark_output/${pair}_1h_processed.csv"
      val file = new java.io.File(path)
      if (file.exists()) {
        val df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(s"file:///${file.getAbsolutePath.replace("\\", "/")}")
        Some(df)
      } else {
        println(s"  Warning: $path not found, skipping.")
        None
      }
    }

    if (hourlyDFs.isEmpty) {
      println("No processed hourly files found. Run SparkHDFSPreprocess.scala first.")
      spark.stop()
      return
    }

    val rawDF = hourlyDFs.reduce(_ union _)
    println(s"Loaded ${rawDF.count()} total hourly records across ${pairs.length} pairs.")

    // Cast all numeric columns to Double
    val numericCols = Seq("Open", "High", "Low", "Close", "Volume",
      "Range", "Change", "ChangePct", "MA5", "MA20", "Volatility5", "Gain", "Loss")
    val castDF = numericCols.foldLeft(rawDF) { (df, colName) =>
      df.withColumn(colName, col(colName).cast("double"))
    }

    // -----------------------------------------------------------------------
    // 2. Feature Engineering - lag features, ratios & 24h future targets
    // -----------------------------------------------------------------------
    println("\nEngineering features (lag values, ratios, 24h targets)...")

    val w = Window.partitionBy("Pair").orderBy("Date")

    var featureDF = castDF
      .withColumn("Close_lag1",   lag("Close",  1).over(w))
      .withColumn("Close_lag2",   lag("Close",  2).over(w))
      .withColumn("Close_lag3",   lag("Close",  3).over(w))
      .withColumn("Change_lag1",  lag("Change", 1).over(w))
      .withColumn("Change_lag2",  lag("Change", 2).over(w))
      .withColumn("MA5_MA20_Ratio", col("MA5") / col("MA20"))

    // Create 24 target columns: Close_h1 ... Close_h24
    for (h <- 1 to HORIZON) {
      featureDF = featureDF.withColumn(s"Close_h$h", lead("Close", h).over(w))
    }

    // Classification label: price direction at +24h
    featureDF = featureDF
      .withColumn("PriceDirection",
        when(lead("Close", HORIZON).over(w) > col("Close"), 1.0).otherwise(0.0))
      .na.drop()

    val totalRecords = featureDF.count()
    println(s"Feature-engineered hourly records: $totalRecords")

    // -----------------------------------------------------------------------
    // 3. Train / Test Split  (80% train, 20% test - ordered by date)
    // -----------------------------------------------------------------------
    // Use date-based split for proper time-series ordering
    val allDates = featureDF.select("Date").distinct().orderBy("Date")
      .collect().map(_.get(0).toString)
    val splitDate = allDates((allDates.length * 0.8).toInt)
    println(s"Split date (80th percentile): $splitDate")

    val trainDF = featureDF.filter(col("Date") <= lit(splitDate)).cache()
    val testDF  = featureDF.filter(col("Date") > lit(splitDate)).cache()

    println(s"Train set: ${trainDF.count()} records")
    println(s"Test  set: ${testDF.count()} records")

    // -----------------------------------------------------------------------
    // 4. Vector Assembler (shared for all models)
    // -----------------------------------------------------------------------
    val assembler = new VectorAssembler()
      .setInputCols(FEATURE_COLS)
      .setOutputCol("raw_features")
      .setHandleInvalid("skip")

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    // -----------------------------------------------------------------------
    // 5. RANDOM FOREST — 24-hour ahead prediction (one model per hour)
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("RANDOM FOREST REGRESSOR — NEXT 24 HOURS (h+1 to h+24)")
    println("=" * 70)

    val regEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")

    case class HourlyResult(hour: Int, rmse: Double, mae: Double, r2: Double)
    val rfResults = scala.collection.mutable.ArrayBuffer.empty[HourlyResult]

    // Store all per-hour predictions for CSV output
    var rfAllPredictions: DataFrame = null

    for (h <- 1 to HORIZON) {
      val labelCol = s"Close_h$h"

      val rf = new RandomForestRegressor()
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
        .setNumTrees(50)
        .setMaxDepth(8)
        .setMinInstancesPerNode(10)
        .setSeed(42)

      val pipeline = new Pipeline().setStages(Array(assembler, scaler, rf))
      val model    = pipeline.fit(trainDF)
      val preds    = model.transform(testDF)

      val rmse = regEvaluator.setLabelCol(labelCol).setMetricName("rmse").evaluate(preds)
      val mae  = regEvaluator.setLabelCol(labelCol).setMetricName("mae").evaluate(preds)
      val r2   = regEvaluator.setLabelCol(labelCol).setMetricName("r2").evaluate(preds)

      rfResults += HourlyResult(h, rmse, mae, r2)
      println(f"  h+$h%2d  |  RMSE: $rmse%10.6f  |  MAE: $mae%10.6f  |  R²: $r2%.6f")

      // Collect last-hour predictions for CSV output (save only h1,h6,h12,h24 for efficiency)
      if (h == 1 || h == 6 || h == 12 || h == 24) {
        val hourPreds = preds
          .select(col("Date"), col("Pair"), col("Close"), col(labelCol).as(s"Actual_h$h"), col("prediction").as(s"RF_h$h"))
        if (rfAllPredictions == null) {
          rfAllPredictions = hourPreds
        } else {
          rfAllPredictions = rfAllPredictions
            .join(hourPreds.select("Date", "Pair", s"Actual_h$h", s"RF_h$h"), Seq("Date", "Pair"), "inner")
        }
      }
    }

    // Feature importances from h+1 model (representative)
    val rfH1Model = {
      val rf1 = new RandomForestRegressor().setLabelCol("Close_h1").setFeaturesCol("features")
        .setNumTrees(50).setMaxDepth(8).setMinInstancesPerNode(10).setSeed(42)
      new Pipeline().setStages(Array(assembler, scaler, rf1)).fit(trainDF)
    }
    val rfModelStage = rfH1Model.stages.last.asInstanceOf[RandomForestRegressionModel]
    println("\n  Top 5 Features (from h+1 model):")
    rfModelStage.featureImportances.toArray.zip(FEATURE_COLS).sortBy(-_._1).take(5)
      .zipWithIndex.foreach { case ((imp, name), i) =>
        println(f"    ${i+1}. $name%-20s : ${imp * 100}%.2f%%")
      }

    val rfAvgR2 = rfResults.map(_.r2).sum / rfResults.size
    println(f"\n  Average R² across 24 hours: $rfAvgR2%.6f")

    // -----------------------------------------------------------------------
    // 6. GBT — 24-hour ahead prediction
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("GRADIENT BOOSTED TREES — NEXT 24 HOURS (h+1 to h+24)")
    println("=" * 70)

    val gbtResults = scala.collection.mutable.ArrayBuffer.empty[HourlyResult]
    var gbtAllPredictions: DataFrame = null

    for (h <- 1 to HORIZON) {
      val labelCol = s"Close_h$h"

      val gbt = new GBTRegressor()
        .setLabelCol(labelCol)
        .setFeaturesCol("features")
        .setMaxIter(50)
        .setMaxDepth(5)
        .setStepSize(0.05)
        .setSeed(42)

      val pipeline = new Pipeline().setStages(Array(assembler, scaler, gbt))
      val model    = pipeline.fit(trainDF)
      val preds    = model.transform(testDF)

      val rmse = regEvaluator.setLabelCol(labelCol).setMetricName("rmse").evaluate(preds)
      val mae  = regEvaluator.setLabelCol(labelCol).setMetricName("mae").evaluate(preds)
      val r2   = regEvaluator.setLabelCol(labelCol).setMetricName("r2").evaluate(preds)

      gbtResults += HourlyResult(h, rmse, mae, r2)
      println(f"  h+$h%2d  |  RMSE: $rmse%10.6f  |  MAE: $mae%10.6f  |  R²: $r2%.6f")

      if (h == 1 || h == 6 || h == 12 || h == 24) {
        val hourPreds = preds
          .select(col("Date"), col("Pair"), col("Close"), col(labelCol).as(s"Actual_h$h"), col("prediction").as(s"GBT_h$h"))
        if (gbtAllPredictions == null) {
          gbtAllPredictions = hourPreds
        } else {
          gbtAllPredictions = gbtAllPredictions
            .join(hourPreds.select("Date", "Pair", s"Actual_h$h", s"GBT_h$h"), Seq("Date", "Pair"), "inner")
        }
      }
    }

    val gbtAvgR2 = gbtResults.map(_.r2).sum / gbtResults.size
    println(f"\n  Average R² across 24 hours: $gbtAvgR2%.6f")

    // -----------------------------------------------------------------------
    // 7. Logistic Regression — 24h direction classification
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("LOGISTIC REGRESSION — 24h Direction (Up/Down at h+24)")
    println("=" * 70)

    val lrClassifier = new LogisticRegression()
      .setLabelCol("PriceDirection")
      .setFeaturesCol("features")
      .setMaxIter(200)
      .setRegParam(0.01)
      .setElasticNetParam(0.0)
      .setFamily("binomial")

    val lrPipeline = new Pipeline()
      .setStages(Array(assembler, scaler, lrClassifier))

    println("Training Logistic Regression Classifier...")
    val lrModel       = lrPipeline.fit(trainDF)
    val lrPredictions = lrModel.transform(testDF)

    val binaryEval = new BinaryClassificationEvaluator()
      .setLabelCol("PriceDirection")
      .setRawPredictionCol("rawPrediction")

    val multiEval = new MulticlassClassificationEvaluator()
      .setLabelCol("PriceDirection")
      .setPredictionCol("prediction")

    val lrAUC      = binaryEval.setMetricName("areaUnderROC").evaluate(lrPredictions)
    val lrAccuracy = multiEval.setMetricName("accuracy").evaluate(lrPredictions)
    val lrF1       = multiEval.setMetricName("f1").evaluate(lrPredictions)
    val lrPrecision = multiEval.setMetricName("weightedPrecision").evaluate(lrPredictions)
    val lrRecall   = multiEval.setMetricName("weightedRecall").evaluate(lrPredictions)

    println(s"  AUC-ROC   : ${"%.4f".format(lrAUC)}")
    println(s"  Accuracy  : ${"%.4f".format(lrAccuracy * 100)}%")
    println(s"  F1 Score  : ${"%.4f".format(lrF1)}")
    println(s"  Precision : ${"%.4f".format(lrPrecision)}")
    println(s"  Recall    : ${"%.4f".format(lrRecall)}")

    println("\n  Confusion Matrix:")
    lrPredictions
      .groupBy("PriceDirection", "prediction")
      .count()
      .orderBy("PriceDirection", "prediction")
      .show()

    println("\n  Sample Signals:")
    lrPredictions
      .select("Date", "Pair", "Close", "PriceDirection", "prediction")
      .withColumn("Signal", when(col("prediction") === 1.0, "BUY").otherwise("SELL"))
      .withColumn("Actual", when(col("PriceDirection") === 1.0, "UP").otherwise("DOWN"))
      .select("Date", "Pair", "Close", "Actual", "Signal")
      .orderBy(desc("Date"))
      .limit(10)
      .show(truncate = false)

    // -----------------------------------------------------------------------
    // 8. Model Comparison Summary
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("MODEL COMPARISON SUMMARY — 24-HOUR PREDICTION")
    println("=" * 70)

    println("\nRandom Forest — per-hour R²:")
    println(f"  ${"Hour"}%-8s ${"RMSE"}%-12s ${"MAE"}%-12s ${"R²"}%-10s")
    println("  " + "-" * 45)
    rfResults.foreach { r =>
      println(f"  h+${r.hour}%-5d ${r.rmse}%-12.6f ${r.mae}%-12.6f ${r.r2}%-10.6f")
    }
    println(f"  Average R²: $rfAvgR2%.6f")

    println("\nGradient Boosted Trees — per-hour R²:")
    println(f"  ${"Hour"}%-8s ${"RMSE"}%-12s ${"MAE"}%-12s ${"R²"}%-10s")
    println("  " + "-" * 45)
    gbtResults.foreach { r =>
      println(f"  h+${r.hour}%-5d ${r.rmse}%-12.6f ${r.mae}%-12.6f ${r.r2}%-10.6f")
    }
    println(f"  Average R²: $gbtAvgR2%.6f")

    println("\nClassification (24h direction):")
    println(f"  AUC-ROC: $lrAUC%.4f  |  Accuracy: ${lrAccuracy * 100}%.2f%%  |  F1: $lrF1%.4f")

    val bestRegressor = if (rfAvgR2 >= gbtAvgR2) "Random Forest" else "Gradient Boosted Trees"
    println(s"\n  Best Regressor (by avg R²): $bestRegressor")

    // -----------------------------------------------------------------------
    // 9. Save predictions to CSV
    // -----------------------------------------------------------------------
    println("\nSaving 24h predictions to ml_output/...")
    new java.io.File("ml_output").mkdirs()

    // RF 24h predictions — save via collect + PrintWriter (key horizons: h1,h6,h12,h24)
    val rfSaveHours = Seq(1, 6, 12, 24)
    val rfCols = Seq("Date", "Pair", "Close") ++ rfSaveHours.flatMap(h => Seq(s"Actual_h$h", s"RF_h$h"))
    val rfOut = rfAllPredictions.select(rfCols.head, rfCols.tail: _*).collect()
    val rfWriter = new java.io.PrintWriter("ml_output/rf_24h_predictions.csv")
    rfWriter.println(rfCols.mkString(","))
    rfOut.foreach(r => rfWriter.println(r.mkString(",")))
    rfWriter.close()

    // GBT 24h predictions
    val gbtCols = Seq("Date", "Pair", "Close") ++ rfSaveHours.flatMap(h => Seq(s"Actual_h$h", s"GBT_h$h"))
    val gbtOut = gbtAllPredictions.select(gbtCols.head, gbtCols.tail: _*).collect()
    val gbtWriter = new java.io.PrintWriter("ml_output/gbt_24h_predictions.csv")
    gbtWriter.println(gbtCols.mkString(","))
    gbtOut.foreach(r => gbtWriter.println(r.mkString(",")))
    gbtWriter.close()

    // LR signals
    val lrOut = lrPredictions
      .select("Date", "Pair", "Close", "PriceDirection", "prediction")
      .withColumn("Signal", when(col("prediction") === 1.0, "BUY").otherwise("SELL"))
      .withColumn("Actual", when(col("PriceDirection") === 1.0, "UP").otherwise("DOWN"))
      .drop("PriceDirection", "prediction")
      .collect()
    val lrWriter = new java.io.PrintWriter("ml_output/lr_24h_signals.csv")
    lrWriter.println("Date,Pair,Close,Actual,Signal")
    lrOut.foreach(r => lrWriter.println(r.mkString(",")))
    lrWriter.close()

    println("Saved:")
    println("  ml_output/rf_24h_predictions.csv  (24 hourly RF predictions)")
    println("  ml_output/gbt_24h_predictions.csv (24 hourly GBT predictions)")
    println("  ml_output/lr_24h_signals.csv      (24h direction signals)")

    // -----------------------------------------------------------------------
    // 10. Stream predictions to Kafka topic "forex-predictions"
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("STREAMING PREDICTIONS TO KAFKA (topic: forex-predictions)")
    println("=" * 70)

    val kafkaProps = new java.util.Properties()
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1")
    kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, "5")

    val kafkaProducer = new KafkaProducer[String, String](kafkaProps)
    val predTopic = "forex-predictions"
    var streamedCount = 0

    // Stream RF predictions
    println("  Streaming RF 24h predictions...")
    rfOut.foreach { row =>
      val jsonParts = rfCols.zip(row.toSeq).map { case (k, v) =>
        s""""$k":"$v""""
      }
      val json = s"""{"model":"RandomForest",${jsonParts.mkString(",")}}"""
      val record = new ProducerRecord[String, String](predTopic, row.getString(1), json)
      kafkaProducer.send(record)
      streamedCount += 1
    }
    println(s"    Sent $streamedCount RF prediction records")

    // Stream GBT predictions
    val gbtStreamStart = streamedCount
    println("  Streaming GBT 24h predictions...")
    gbtOut.foreach { row =>
      val jsonParts = gbtCols.zip(row.toSeq).map { case (k, v) =>
        s""""$k":"$v""""
      }
      val json = s"""{"model":"GradientBoostedTrees",${jsonParts.mkString(",")}}"""
      val record = new ProducerRecord[String, String](predTopic, row.getString(1), json)
      kafkaProducer.send(record)
      streamedCount += 1
    }
    println(s"    Sent ${streamedCount - gbtStreamStart} GBT prediction records")

    // Stream LR signals
    val lrStreamStart = streamedCount
    println("  Streaming LR direction signals...")
    lrOut.foreach { row =>
      val json = s"""{"model":"LogisticRegression","Date":"${row.get(0)}","Pair":"${row.get(1)}","Close":"${row.get(2)}","Actual":"${row.get(3)}","Signal":"${row.get(4)}"}"""
      val record = new ProducerRecord[String, String](predTopic, row.getString(1), json)
      kafkaProducer.send(record)
      streamedCount += 1
    }
    println(s"    Sent ${streamedCount - lrStreamStart} LR signal records")

    kafkaProducer.flush()
    kafkaProducer.close()
    println(s"\n  Total records streamed to Kafka: $streamedCount")

    println("\n" + "=" * 70)
    println("SPARK ML — 24-HOUR PREDICTION COMPLETE")
    println("=" * 70)

    spark.stop()
  }
}
