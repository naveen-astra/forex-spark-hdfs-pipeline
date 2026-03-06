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
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///D:/tmp/spark-events")
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

    // Create h+1 target (recursive approach: one model predicts next hour,
    // same model is reused with recalculated features to forecast t+2, t+3, ... t+24)
    featureDF = featureDF.withColumn("Close_h1", lead("Close", 1).over(w))

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
    // 5. RANDOM FOREST — h+1 model (Recursive Forecasting Approach)
    //    One model predicts t+1, recalculates features, predicts t+2, etc.
    //    The 24-step recursive loop runs in SparkKafkaStreaming.scala
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("RANDOM FOREST REGRESSOR — h+1 (Recursive Approach)")
    println("=" * 70)
    println("Training a single h+1 model. SparkKafkaStreaming uses it recursively")
    println("to predict t+1, recalculate features, then predict t+2, ... t+24.\n")

    val regEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")

    val rf = new RandomForestRegressor()
      .setLabelCol("Close_h1")
      .setFeaturesCol("features")
      .setNumTrees(50)
      .setMaxDepth(8)
      .setMinInstancesPerNode(10)
      .setSeed(42)

    val rfPipeline = new Pipeline().setStages(Array(assembler, scaler, rf))
    println("  Training RF h+1 model...")
    val rfH1Model = rfPipeline.fit(trainDF)
    val rfPreds   = rfH1Model.transform(testDF)

    val rfRmse = regEvaluator.setLabelCol("Close_h1").setMetricName("rmse").evaluate(rfPreds)
    val rfMae  = regEvaluator.setLabelCol("Close_h1").setMetricName("mae").evaluate(rfPreds)
    val rfR2   = regEvaluator.setLabelCol("Close_h1").setMetricName("r2").evaluate(rfPreds)

    println(f"  h+1  |  RMSE: $rfRmse%10.6f  |  MAE: $rfMae%10.6f  |  R²: $rfR2%.6f")

    // Save the h+1 RF PipelineModel for recursive streaming pipeline
    val modelSavePath = "ml_output/rf_h1_model"
    rfH1Model.write.overwrite().save(modelSavePath)
    println(s"\n  Saved RF h+1 PipelineModel to: $modelSavePath")
    println("  (Loaded by SparkKafkaStreaming.scala for recursive 24-step prediction)")

    val rfModelStage = rfH1Model.stages.last.asInstanceOf[RandomForestRegressionModel]
    println("\n  Top 5 Features:")
    rfModelStage.featureImportances.toArray.zip(FEATURE_COLS).sortBy(-_._1).take(5)
      .zipWithIndex.foreach { case ((imp, name), i) =>
        println(f"    ${i+1}. $name%-20s : ${imp * 100}%.2f%%")
      }

    // -----------------------------------------------------------------------
    // 6. GBT — h+1 model (Recursive Forecasting Approach)
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("GRADIENT BOOSTED TREES — h+1 (Recursive Approach)")
    println("=" * 70)

    val gbt = new GBTRegressor()
      .setLabelCol("Close_h1")
      .setFeaturesCol("features")
      .setMaxIter(50)
      .setMaxDepth(5)
      .setStepSize(0.05)
      .setSeed(42)

    val gbtPipeline = new Pipeline().setStages(Array(assembler, scaler, gbt))
    println("  Training GBT h+1 model...")
    val gbtH1Model = gbtPipeline.fit(trainDF)
    val gbtPreds   = gbtH1Model.transform(testDF)

    val gbtRmse = regEvaluator.setLabelCol("Close_h1").setMetricName("rmse").evaluate(gbtPreds)
    val gbtMae  = regEvaluator.setLabelCol("Close_h1").setMetricName("mae").evaluate(gbtPreds)
    val gbtR2   = regEvaluator.setLabelCol("Close_h1").setMetricName("r2").evaluate(gbtPreds)

    println(f"  h+1  |  RMSE: $gbtRmse%10.6f  |  MAE: $gbtMae%10.6f  |  R²: $gbtR2%.6f")

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
    println("MODEL COMPARISON SUMMARY — h+1 RECURSIVE PREDICTION")
    println("=" * 70)

    println(f"\nRandom Forest h+1:        RMSE: $rfRmse%.6f  |  MAE: $rfMae%.6f  |  R²: $rfR2%.6f")
    println(f"Gradient Boosted Trees h+1: RMSE: $gbtRmse%.6f  |  MAE: $gbtMae%.6f  |  R²: $gbtR2%.6f")

    println("\nClassification (24h direction):")
    println(f"  AUC-ROC: $lrAUC%.4f  |  Accuracy: ${lrAccuracy * 100}%.2f%%  |  F1: $lrF1%.4f")

    val bestRegressor = if (rfR2 >= gbtR2) "Random Forest" else "Gradient Boosted Trees"
    println(s"\n  Best Regressor (by R²): $bestRegressor")
    println("  Note: h+1 model is used recursively by SparkKafkaStreaming for 24-step prediction.")

    // -----------------------------------------------------------------------
    // 9. Save h+1 predictions to CSV
    // -----------------------------------------------------------------------
    println("\nSaving h+1 predictions to ml_output/...")
    new java.io.File("ml_output").mkdirs()

    // RF h+1 predictions
    val rfSaveDF = rfPreds
      .select(col("Date"), col("Pair"), col("Close"), col("Close_h1").as("Actual_h1"), col("prediction").as("RF_Predicted"))
      .orderBy("Date", "Pair")
    val rfOut = rfSaveDF.collect()
    val rfWriter = new java.io.PrintWriter("ml_output/rf_predictions.csv")
    rfWriter.println("Date,Pair,Close,Actual_h1,RF_Predicted")
    rfOut.foreach(r => rfWriter.println(r.mkString(",")))
    rfWriter.close()

    // GBT h+1 predictions
    val gbtSaveDF = gbtPreds
      .select(col("Date"), col("Pair"), col("Close"), col("Close_h1").as("Actual_h1"), col("prediction").as("GBT_Predicted"))
      .orderBy("Date", "Pair")
    val gbtOut = gbtSaveDF.collect()
    val gbtWriter = new java.io.PrintWriter("ml_output/gbt_predictions.csv")
    gbtWriter.println("Date,Pair,Close,Actual_h1,GBT_Predicted")
    gbtOut.foreach(r => gbtWriter.println(r.mkString(",")))
    gbtWriter.close()

    // LR signals
    val lrOut = lrPredictions
      .select("Date", "Pair", "Close", "PriceDirection", "prediction")
      .withColumn("Signal", when(col("prediction") === 1.0, "BUY").otherwise("SELL"))
      .withColumn("Actual", when(col("PriceDirection") === 1.0, "UP").otherwise("DOWN"))
      .drop("PriceDirection", "prediction")
      .collect()
    val lrWriter = new java.io.PrintWriter("ml_output/lr_signals.csv")
    lrWriter.println("Date,Pair,Close,Actual,Signal")
    lrOut.foreach(r => lrWriter.println(r.mkString(",")))
    lrWriter.close()

    println("Saved:")
    println("  ml_output/rf_predictions.csv   (h+1 RF predictions)")
    println("  ml_output/gbt_predictions.csv  (h+1 GBT predictions)")
    println("  ml_output/lr_signals.csv       (24h direction signals)")

    // -----------------------------------------------------------------------
    // 10. Stream h+1 predictions to Kafka topic "forex-predictions"
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("STREAMING h+1 PREDICTIONS TO KAFKA (topic: forex-predictions)")
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

    // Stream RF h+1 predictions
    println("  Streaming RF h+1 predictions...")
    rfOut.foreach { row =>
      val json = s"""  {"model":"RandomForest","Date":"${row.get(0)}","Pair":"${row.get(1)}","Close":"${row.get(2)}","Actual_h1":"${row.get(3)}","RF_Predicted":"${row.get(4)}"}"""
      val record = new ProducerRecord[String, String](predTopic, row.getString(1), json)
      kafkaProducer.send(record)
      streamedCount += 1
    }
    println(s"    Sent $streamedCount RF records")

    // Stream GBT h+1 predictions
    val gbtStreamStart = streamedCount
    println("  Streaming GBT h+1 predictions...")
    gbtOut.foreach { row =>
      val json = s"""  {"model":"GradientBoostedTrees","Date":"${row.get(0)}","Pair":"${row.get(1)}","Close":"${row.get(2)}","Actual_h1":"${row.get(3)}","GBT_Predicted":"${row.get(4)}"}"""
      val record = new ProducerRecord[String, String](predTopic, row.getString(1), json)
      kafkaProducer.send(record)
      streamedCount += 1
    }
    println(s"    Sent ${streamedCount - gbtStreamStart} GBT records")

    // Stream LR signals
    val lrStreamStart = streamedCount
    println("  Streaming LR direction signals...")
    lrOut.foreach { row =>
      val json = s"""  {"model":"LogisticRegression","Date":"${row.get(0)}","Pair":"${row.get(1)}","Close":"${row.get(2)}","Actual":"${row.get(3)}","Signal":"${row.get(4)}"}"""
      val record = new ProducerRecord[String, String](predTopic, row.getString(1), json)
      kafkaProducer.send(record)
      streamedCount += 1
    }
    println(s"    Sent ${streamedCount - lrStreamStart} LR records")

    kafkaProducer.flush()
    kafkaProducer.close()
    println(s"\n  Total records streamed to Kafka: $streamedCount")

    println("\n" + "=" * 70)
    println("SPARK ML — h+1 RECURSIVE MODEL TRAINING COMPLETE")
    println("=" * 70)
    println("Next step: Run SparkKafkaStreaming.scala to perform 24-step recursive prediction.")

    spark.stop()
  }
}
