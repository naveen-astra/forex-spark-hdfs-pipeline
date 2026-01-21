//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-sql:3.3.0"
//> using dep "org.apache.hadoop:hadoop-client:3.3.2"

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
    println("SPARK + HDFS FOREX PREPROCESSING")
    println("=" * 70)
    
    val files = List(
      ("EURUSD", "1d"),
      ("GBPUSD", "1d"),
      ("USDCAD", "1d"),
      ("USDJPY", "1d")
    )
    
    val containerId = "4b1572a73e7b33e5355cff51616b584a2d3e7450eb9f927f512c84279f02324e"
    
    files.zipWithIndex.foreach { case ((pair, tf), idx) =>
      println(s"\n[${idx+1}/4] Processing $pair $tf with Spark...")
      
      val hdfsPath = s"/forex/raw/${tf}/${pair}_${tf}_2017-01-01_2024-12-31.csv"
      val localFile = s"temp_spark_${pair}_${tf}.csv"
      val processedLocal = s"spark_output/${pair}_${tf}_processed"
      val hdfsOutput = s"/forex_new/spark_processed/${tf}/${pair}_${tf}"
      
      try {
        // Download from HDFS using Docker
        print("  Fetching from HDFS...")
        import scala.sys.process._
        s"docker exec $containerId hdfs dfs -cat $hdfsPath".#>(new java.io.File(localFile)).!
        println(" ✓")
        
        // Read local file with Spark (explicit file:// protocol)
        print("  Reading with Spark...")
        val absPath = new java.io.File(localFile).getAbsolutePath.replace("\\", "/")
        val df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(s"file:///$absPath")
        println(s" ✓ (${df.count()} records)")
        
        // Preprocess with Spark transformations
        print("  Applying Spark transformations...")
        val processed = preprocessWithSpark(df, pair, tf).cache() // Cache to persist in memory
        println(" ✓")
        
        // Show statistics using Spark aggregations (before collecting)
        showSparkStats(processed, pair, tf)
        
        // Collect results and write manually (avoid Hadoop binaries issue)
        print("  Saving as CSV...")
        new java.io.File("spark_output").mkdirs()
        val results = processed.collect()
        
        val csvFile = new java.io.PrintWriter(s"$processedLocal.csv")
        csvFile.println(processed.columns.mkString(","))
        results.foreach { row =>
          csvFile.println(row.mkString(","))
        }
        csvFile.close()
        println(" ✓")
        
        // Upload CSV to HDFS
        print("  Uploading to HDFS...")
        s"docker exec $containerId hdfs dfs -mkdir -p $hdfsOutput".!
        s"docker exec $containerId hdfs dfs -put -f $processedLocal.csv $hdfsOutput/data.csv".!
        println(" ✓")
        
        processed.unpersist() // Release cache
        
        // Show sample data
        println("\n  Sample (last 3 rows):")
        val sampleDf = spark.read.option("header", "true").option("inferSchema", "true")
          .csv(s"$processedLocal.csv")
        sampleDf.orderBy(desc("Date"))
          .select("Date", "Close", "ChangePct", "MA5", "MA20", "Volatility5")
          .show(3, truncate = false)
          
      } catch {
        case e: Exception =>
          println(s" ✗ Error: ${e.getMessage}")
      }
    }
    
    println("\n" + "=" * 70)
    println("✓ SPARK PROCESSING COMPLETE")
    println("Output: hdfs://localhost:9870/forex_new/spark_processed/")
    println("Format: Parquet (efficient columnar storage)")
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
