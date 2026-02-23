//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-sql:3.3.0"
//> using dep "org.apache.hadoop:hadoop-client:3.3.2"
//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.util=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SparkHDFSPreprocess {
  
  def main(args: Array[String]): Unit = {
    
    // Initialize Spark with local file system
    val spark = SparkSession.builder()
      .appName("Forex HDFS Preprocessing with Spark")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._
    
    println("=" * 70)
    println("SPARK LOCAL FOREX PREPROCESSING - ALL FILES")
    println("=" * 70)
    
    // All 15 currency pairs and 3 timeframes (45 files total)
    val pairs = List(
      "AUDJPY", "AUDUSD", "EURAUD", "EURCHF", "EURGBP", 
      "EURJPY", "EURUSD", "GBPAUD", "GBPCAD", "GBPJPY", 
      "GBPUSD", "NZDUSD", "USDCAD", "USDCHF", "USDJPY"
    )
    val timeframes = List("1d", "1h", "1min")
    
    val files = for {
      pair <- pairs
      tf <- timeframes
    } yield (pair, tf)
    
    println(s"Processing ${files.length} files (${pairs.length} pairs × ${timeframes.length} timeframes)")
    new java.io.File("spark_output").mkdirs()
    
    val startTime = System.currentTimeMillis()
    
    files.zipWithIndex.foreach { case ((pair, tf), idx) =>
      println(s"\n[${idx+1}/${files.length}] Processing $pair $tf...")
      
      val inputPath = s"raw2/${pair}_${tf}_2017-01-01_2024-12-31.csv"
      val outputPath = s"spark_output/${pair}_${tf}_processed.csv"
      
      try {
        // Read directly from raw2/ with Spark
        print("  Reading...")
        val absPath = new java.io.File(inputPath).getAbsolutePath.replace("\\", "/")
        val df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(s"file:///$absPath")
        val recordCount = df.count()
        println(s" ✓ (${recordCount} records)")
        
        // Preprocess with Spark transformations
        print("  Transforming...")
        val processed = preprocessWithSpark(df, pair, tf).cache()
        println(" ✓")
        
        // Write output - collect and write manually for better control
        print("  Writing CSV...")
        val results = processed.collect()
        
        val csvFile = new java.io.PrintWriter(outputPath)
        csvFile.println(processed.columns.mkString(","))
        results.foreach { row =>
          csvFile.println(row.mkString(","))
        }
        csvFile.close()
        println(" ✓")
        
        processed.unpersist() // Release cache
          
      } catch {
        case e: Exception =>
          println(s" ✗ Error: ${e.getMessage}")
      }
    }
    
    val endTime = System.currentTimeMillis()
    val duration = (endTime - startTime) / 1000.0
    
    println("\n" + "=" * 70)
    println("✓ SPARK PROCESSING COMPLETE")
    println(s"Processed: ${files.length} files in ${duration}s (avg: ${duration/files.length}s per file)")
    println("Output: spark_output/ directory")
    println("Format: CSV with technical indicators (MA5, MA20, Volatility, etc.)")
    println("=" * 70)
    
    spark.stop()
  }
  
  def preprocessWithSpark(df: DataFrame, pair: String, tf: String): DataFrame = {
    
    // Define window specifications for moving averages
    val window5 = Window.orderBy("Date").rowsBetween(-4, 0)
    val window20 = Window.orderBy("Date").rowsBetween(-19, 0)
    
    df
      // Add metadata columns
      .withColumn("Pair", lit(pair))
      .withColumn("Timeframe", lit(tf))
      
      // Calculate price features using Spark SQL functions
      .withColumn("Range", col("High") - col("Low"))
      .withColumn("Change", col("Close") - col("Open"))
      .withColumn("ChangePct", 
        ((col("Close") - col("Open")) / col("Open") * 100))
      
      // Calculate Moving Averages using Spark Window functions
      .withColumn("MA5", avg("Close").over(window5))
      .withColumn("MA20", avg("Close").over(window20))
      
      // Calculate Volatility using Spark aggregations
      .withColumn("Volatility5", stddev("Close").over(window5))
      
      // Calculate RSI components
      .withColumn("Gain", 
        when(col("Change") > 0, col("Change")).otherwise(0))
      .withColumn("Loss", 
        when(col("Change") < 0, -col("Change")).otherwise(0))
      
      // Filter out rows with null MA20 (first 19 rows)
      .filter(col("MA20").isNotNull)
      
      // Reorder columns
      .select(
        "Date", "Pair", "Timeframe",
        "Open", "High", "Low", "Close", "Volume",
        "Range", "Change", "ChangePct",
        "MA5", "MA20", "Volatility5",
        "Gain", "Loss"
      )
  }
  
  def showSparkStats(df: DataFrame, pair: String, tf: String): Unit = {
    println(f"\n  Spark Statistics for $pair $tf:")
    
    // Use Spark aggregations for statistics
    val stats = df.agg(
      count("*").alias("count"),
      min("Date").alias("start"),
      max("Date").alias("end"),
      avg("Close").alias("avg_close"),
      min("Close").alias("min_close"),
      max("Close").alias("max_close"),
      avg("ChangePct").alias("avg_change"),
      avg("Volume").alias("avg_volume"),
      avg("Volatility5").alias("avg_volatility")
    ).collect()(0)
    
    println(f"    Records: ${stats.getAs[Long]("count")}")
    println(f"    Period: ${stats.getAs[String]("start")} → ${stats.getAs[String]("end")}")
    println(f"    Avg Close: ${stats.getAs[Double]("avg_close")}%.5f")
    println(f"    Min/Max: ${stats.getAs[Double]("min_close")}%.5f / ${stats.getAs[Double]("max_close")}%.5f")
    println(f"    Avg Change: ${stats.getAs[Double]("avg_change")}%+.3f%%")
    println(f"    Avg Volatility: ${stats.getAs[Double]("avg_volatility")}%.5f")
  }
}
