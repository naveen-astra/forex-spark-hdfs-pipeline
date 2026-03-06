//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-sql:3.3.0"
//> using dep "org.apache.hadoop:hadoop-client:3.3.2"
//> using javaOpt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.lang=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/java.util=ALL-UNNAMED"
//> using javaOpt "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkUIDemo {
  
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("Forex Data Explorer - UI Demo")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:///D:/tmp/spark-events")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._
    
    println("\n" + "=" * 70)
    println("SPARK UI DEMO - FOREX DATA EXPLORER")
    println("=" * 70)
    println("\n✓ Spark UI available at: http://localhost:4040")
    println("✓ Press Enter to stop and exit...\n")
    
    // Load multiple datasets
    println("Loading processed datasets...")
    
    val eurusd = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("spark_output/EURUSD_1d_processed.csv")
      .cache()
    
    val gbpusd = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("spark_output/GBPUSD_1d_processed.csv")
      .cache()
      
    val usdjpy = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("spark_output/USDJPY_1d_processed.csv")
      .cache()
    
    println("✓ Loaded 3 datasets\n")
    
    // Show some analytics
    println("=" * 70)
    println("EURUSD Statistics (2017-2024)")
    println("=" * 70)
    eurusd.select(
      count("*").alias("Records"),
      min("Close").alias("Min"),
      max("Close").alias("Max"),
      avg("Close").alias("Average"),
      avg("ChangePct").alias("Avg Change %"),
      avg("Volatility5").alias("Avg Volatility")
    ).show(false)
    
    println("\n" + "=" * 70)
    println("Top 10 Most Volatile Days (EURUSD)")
    println("=" * 70)
    eurusd.select("Date", "Close", "ChangePct", "Volatility5")
      .orderBy(desc("Volatility5"))
      .limit(10)
      .show(false)
    
    println("\n" + "=" * 70)
    println("Monthly Average Close Prices (2024)")
    println("=" * 70)
    eurusd.filter(year($"Date") === 2024)
      .withColumn("Month", month($"Date"))
      .groupBy("Month")
      .agg(
        avg("Close").alias("Avg_Close"),
        avg("ChangePct").alias("Avg_Change"),
        count("*").alias("Trading_Days")
      )
      .orderBy("Month")
      .show(false)
    
    println("\n" + "=" * 70)
    println("Cross-Pair Comparison (Latest Values)")
    println("=" * 70)
    
    val allPairs = eurusd.union(gbpusd).union(usdjpy)
    allPairs.groupBy("Pair")
      .agg(
        max("Date").alias("Latest_Date"),
        last("Close").alias("Latest_Close"),
        avg("Close").alias("Avg_Close"),
        avg("Volatility5").alias("Avg_Volatility")
      )
      .show(false)
    
    println("\n" + "=" * 70)
    println("🌐 Explore the Spark UI at: http://localhost:4040")
    println("=" * 70)
    println("You can view:")
    println("  • Jobs tab - All completed Spark jobs")
    println("  • Stages tab - Detailed stage execution")
    println("  • Storage tab - Cached dataframes (3 datasets)")
    println("  • Environment tab - Spark configuration")
    println("  • Executors tab - Resource usage")
    println("  • SQL tab - Query plans and execution details")
    println("=" * 70)
    println("\nPress Enter to stop Spark and close UI...")
    
    // Keep Spark running until user presses Enter
    scala.io.StdIn.readLine()
    
    println("\nStopping Spark...")
    spark.stop()
    println("✓ Spark stopped. UI closed.")
  }
}
