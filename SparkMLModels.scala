//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-sql:3.3.0"
//> using dep "org.apache.spark::spark-mllib:3.3.0"
//> using dep "org.apache.hadoop:hadoop-client:3.3.2"
//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.util=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

import org.apache.spark.sql.{SparkSession, DataFrame}
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

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Forex SparkML - RF, GBT, Logistic Regression")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("=" * 70)
    println("SPARK ML MODELS - FOREX PRICE PREDICTION & TRADING SIGNALS")
    println("=" * 70)

    // -----------------------------------------------------------------------
    // 1. Load and combine all 15 daily pair files
    // -----------------------------------------------------------------------
    val pairs = List(
      "AUDJPY", "AUDUSD", "EURAUD", "EURCHF", "EURGBP",
      "EURJPY", "EURUSD", "GBPAUD", "GBPCAD", "GBPJPY",
      "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY"
    )

    println(s"\nLoading preprocessed daily data for ${pairs.length} pairs...")

    val dailyDFs = pairs.flatMap { pair =>
      val path = s"spark_output/${pair}_1d_processed.csv"
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

    if (dailyDFs.isEmpty) {
      println("No processed files found. Run SparkHDFSPreprocess.scala first.")
      spark.stop()
      return
    }

    val rawDF = dailyDFs.reduce(_ union _)
    println(s"Loaded ${rawDF.count()} total records across ${pairs.length} pairs.")

    // Cast all numeric columns to Double (CSV inference may read some as String)
    val numericCols = Seq("Open", "High", "Low", "Close", "Volume",
      "Range", "Change", "ChangePct", "MA5", "MA20", "Volatility5", "Gain", "Loss")
    val castDF = numericCols.foldLeft(rawDF) { (df, colName) =>
      df.withColumn(colName, col(colName).cast("double"))
    }

    // -----------------------------------------------------------------------
    // 2. Feature Engineering - lag features & derived ratios
    // -----------------------------------------------------------------------
    println("\nEngineering features (lag values, ratios)...")

    val w = Window.partitionBy("Pair").orderBy("Date")

    val featureDF = castDF
      .withColumn("Close_lag1",   lag("Close",  1).over(w))
      .withColumn("Close_lag2",   lag("Close",  2).over(w))
      .withColumn("Close_lag3",   lag("Close",  3).over(w))
      .withColumn("Change_lag1",  lag("Change", 1).over(w))
      .withColumn("Change_lag2",  lag("Change", 2).over(w))
      .withColumn("MA5_MA20_Ratio", col("MA5") / col("MA20"))
      // Regression label: next day's Close price
      .withColumn("NextDayClose", lead("Close", 1).over(w))
      // Classification label: 1.0 if price goes up next day, 0.0 if down
      .withColumn("PriceDirection",
        when(lead("Close", 1).over(w) > col("Close"), 1.0).otherwise(0.0))
      // Drop rows with any null features or labels
      .na.drop()

    val totalRecords = featureDF.count()
    println(s"Feature-engineered records: $totalRecords")

    // -----------------------------------------------------------------------
    // 3. Train / Test Split  (80% train, 20% test - ordered by date)
    // -----------------------------------------------------------------------
    val splitIndex = (totalRecords * 0.8).toLong
    val indexedDF  = featureDF.withColumn("row_idx", monotonically_increasing_id())
    val trainDF    = indexedDF.filter(col("row_idx") <= splitIndex).drop("row_idx")
    val testDF     = indexedDF.filter(col("row_idx") > splitIndex).drop("row_idx")

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
    // 5. MODEL 1: Random Forest Regressor - Next Day Close Price
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("MODEL 1: Random Forest Regressor (Next Day Close Price)")
    println("=" * 70)

    val rfRegressor = new RandomForestRegressor()
      .setLabelCol("NextDayClose")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(10)
      .setMinInstancesPerNode(5)
      .setSeed(42)

    val rfPipeline = new Pipeline()
      .setStages(Array(assembler, scaler, rfRegressor))

    println("Training Random Forest Regressor...")
    val rfModel    = rfPipeline.fit(trainDF)
    val rfPredictions = rfModel.transform(testDF)

    val regEvaluator = new RegressionEvaluator()
      .setLabelCol("NextDayClose")
      .setPredictionCol("prediction")

    val rfRMSE = regEvaluator.setMetricName("rmse").evaluate(rfPredictions)
    val rfMAE  = regEvaluator.setMetricName("mae").evaluate(rfPredictions)
    val rfR2   = regEvaluator.setMetricName("r2").evaluate(rfPredictions)

    println(s"  RMSE : ${"%.6f".format(rfRMSE)}")
    println(s"  MAE  : ${"%.6f".format(rfMAE)}")
    println(s"  R2   : ${"%.6f".format(rfR2)}")

    // Feature importances
    val rfModelStage = rfModel.stages.last.asInstanceOf[RandomForestRegressionModel]
    println("\n  Top 5 Important Features:")
    rfModelStage.featureImportances.toArray
      .zip(FEATURE_COLS)
      .sortBy(-_._1)
      .take(5)
      .zipWithIndex
      .foreach { case ((imp, name), i) =>
        println(f"    ${i+1}. $name%-20s : ${imp * 100}%.2f%%")
      }

    // Save sample predictions
    println("\n  Sample Predictions (RF):")
    rfPredictions
      .select("Date", "Pair", "Close", "NextDayClose", "prediction")
      .orderBy(desc("Date"))
      .limit(5)
      .show(truncate = false)

    // -----------------------------------------------------------------------
    // 6. MODEL 2: Gradient Boosted Trees Regressor - Next Day Close Price
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("MODEL 2: Gradient Boosted Trees Regressor (Next Day Close Price)")
    println("=" * 70)

    val gbtRegressor = new GBTRegressor()
      .setLabelCol("NextDayClose")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setMaxDepth(5)
      .setStepSize(0.05)
      .setSeed(42)

    val gbtPipeline = new Pipeline()
      .setStages(Array(assembler, scaler, gbtRegressor))

    println("Training Gradient Boosted Trees Regressor...")
    val gbtModel      = gbtPipeline.fit(trainDF)
    val gbtPredictions = gbtModel.transform(testDF)

    val gbtRMSE = regEvaluator.setMetricName("rmse").evaluate(gbtPredictions)
    val gbtMAE  = regEvaluator.setMetricName("mae").evaluate(gbtPredictions)
    val gbtR2   = regEvaluator.setMetricName("r2").evaluate(gbtPredictions)

    println(s"  RMSE : ${"%.6f".format(gbtRMSE)}")
    println(s"  MAE  : ${"%.6f".format(gbtMAE)}")
    println(s"  R2   : ${"%.6f".format(gbtR2)}")

    // Feature importances
    val gbtModelStage = gbtModel.stages.last.asInstanceOf[GBTRegressionModel]
    println("\n  Top 5 Important Features:")
    gbtModelStage.featureImportances.toArray
      .zip(FEATURE_COLS)
      .sortBy(-_._1)
      .take(5)
      .zipWithIndex
      .foreach { case ((imp, name), i) =>
        println(f"    ${i+1}. $name%-20s : ${imp * 100}%.2f%%")
      }

    println("\n  Sample Predictions (GBT):")
    gbtPredictions
      .select("Date", "Pair", "Close", "NextDayClose", "prediction")
      .orderBy(desc("Date"))
      .limit(5)
      .show(truncate = false)

    // -----------------------------------------------------------------------
    // 7. MODEL 3: Logistic Regression Classifier - Buy/Sell Signal
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("MODEL 3: Logistic Regression Classifier (Buy=1 / Sell=0 Signal)")
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

    // Confusion matrix
    println("\n  Confusion Matrix:")
    lrPredictions
      .groupBy("PriceDirection", "prediction")
      .count()
      .orderBy("PriceDirection", "prediction")
      .show()

    println("\n  Sample Predictions (Trading Signals):")
    lrPredictions
      .select("Date", "Pair", "Close",
        "PriceDirection",
        "prediction",
        "probability")
      .withColumn("Signal",
        when(col("prediction") === 1.0, "BUY").otherwise("SELL"))
      .withColumn("Actual",
        when(col("PriceDirection") === 1.0, "UP").otherwise("DOWN"))
      .select("Date", "Pair", "Close", "Actual", "Signal")
      .orderBy(desc("Date"))
      .limit(10)
      .show(truncate = false)

    // -----------------------------------------------------------------------
    // 8. Model Comparison Summary
    // -----------------------------------------------------------------------
    println("\n" + "=" * 70)
    println("MODEL COMPARISON SUMMARY")
    println("=" * 70)

    println("\nRegression Models (Next Day Price Prediction):")
    println(f"  ${"Model"}%-35s ${"RMSE"}%-12s ${"MAE"}%-12s ${"R2"}%-10s")
    println("  " + "-" * 70)
    println(f"  ${"Random Forest Regressor"}%-35s ${rfRMSE}%-12.6f ${rfMAE}%-12.6f ${rfR2}%-10.6f")
    println(f"  ${"Gradient Boosted Trees"}%-35s ${gbtRMSE}%-12.6f ${gbtMAE}%-12.6f ${gbtR2}%-10.6f")

    println("\nClassification Model (Buy/Sell Signal):")
    println(f"  ${"Model"}%-35s ${"AUC-ROC"}%-12s ${"Accuracy"}%-12s ${"F1"}%-10s")
    println("  " + "-" * 70)
    println(f"  ${"Logistic Regression Classifier"}%-35s ${lrAUC}%-12.4f ${lrAccuracy * 100}%-12.2f%% ${lrF1}%-10.4f")

    val bestRegressor = if (rfR2 >= gbtR2) "Random Forest Regressor" else "Gradient Boosted Trees"
    println(s"\n  Best Regression Model   : $bestRegressor")
    println(s"  Classification AUC-ROC  : ${"%.4f".format(lrAUC)} (above 0.5 = better than random)")

    // -----------------------------------------------------------------------
    // 9. Save predictions to CSV
    // -----------------------------------------------------------------------
    println("\nSaving predictions to ml_output/...")
    new java.io.File("ml_output").mkdirs()

    // RF predictions
    val rfOut = rfPredictions
      .select("Date", "Pair", "Close", "NextDayClose", "prediction")
      .withColumnRenamed("prediction", "RF_Predicted_Close")
      .collect()
    val rfWriter = new java.io.PrintWriter("ml_output/rf_predictions.csv")
    rfWriter.println("Date,Pair,Close,NextDayClose,RF_Predicted_Close")
    rfOut.foreach(r => rfWriter.println(r.mkString(",")))
    rfWriter.close()

    // GBT predictions
    val gbtOut = gbtPredictions
      .select("Date", "Pair", "Close", "NextDayClose", "prediction")
      .withColumnRenamed("prediction", "GBT_Predicted_Close")
      .collect()
    val gbtWriter = new java.io.PrintWriter("ml_output/gbt_predictions.csv")
    gbtWriter.println("Date,Pair,Close,NextDayClose,GBT_Predicted_Close")
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
    println("  ml_output/rf_predictions.csv")
    println("  ml_output/gbt_predictions.csv")
    println("  ml_output/lr_signals.csv")

    println("\n" + "=" * 70)
    println("SPARK ML TRAINING COMPLETE")
    println("=" * 70)

    spark.stop()
  }
}
