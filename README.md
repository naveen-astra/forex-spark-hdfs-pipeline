# Forex Spark HDFS Pipeline

A distributed big data preprocessing pipeline for forex market data using Apache Spark and Hadoop HDFS. Processes multi-timeframe currency pair data with advanced feature engineering for technical analysis.

## Overview

This project implements a scalable data pipeline that fetches forex data from HDFS, applies distributed preprocessing using Apache Spark, and generates engineered features including moving averages, volatility metrics, and technical indicators.

## Architecture

```
Raw Data (HDFS) → Spark Processing → Feature Engineering → Preprocessed Data (HDFS)
```

**Data Flow:**
- Source: HDFS at `hdfs://localhost:9870/forex/raw/`
- Processing: Apache Spark 3.3.0 with DataFrame API
- Storage: HDFS at `hdfs://localhost:9870/forex_new/spark_processed/`

## Features

### Data Processing
- Distributed processing using Apache Spark
- Fetch data directly from Hadoop HDFS
- Support for multiple timeframes (1min, 1h, 1d)
- Batch processing capabilities

### Feature Engineering
- **Moving Averages**: 5-day and 20-day moving averages using Spark Window functions
- **Volatility Analysis**: 5-day rolling standard deviation
- **Price Metrics**: Range, Change, Percentage Change
- **Technical Indicators**: Gain/Loss components for RSI calculation

### Data Quality
- Null handling for initial moving average periods
- Automatic filtering of incomplete data
- Validation and statistics computation

## Dataset

**Currency Pairs:** 15 major forex pairs
- EUR/USD, GBP/USD, USD/CAD, USD/JPY
- AUD/JPY, AUD/USD, EUR/AUD, EUR/CHF
- EUR/GBP, EUR/JPY, GBP/AUD, GBP/CAD
- GBP/JPY, NZD/USD, USD/CHF

**Timeframes:** Daily (1d), Hourly (1h), Minute (1min)

**Period:** January 1, 2017 - December 31, 2024 (8 years)

**Total Records:** 2,922 records per daily timeframe per pair

## Technical Stack

- **Language:** Scala 2.12.18
- **Processing:** Apache Spark 3.3.0
- **Storage:** Hadoop HDFS 3.3.2
- **Build Tool:** Scala CLI
- **Container:** Docker (for HDFS access)

## Project Structure

```
bda-forex/
├── SparkHDFSPreprocess.scala    # Main Spark preprocessing script
├── raw2/                         # Local raw data files
├── processed/                    # Preprocessed output (local)
├── spark_output/                 # Spark processed files
├── run.ps1                       # Windows execution script
├── run.sh                        # Linux/Mac execution script
└── README.md                     # Project documentation
```

## Installation

### Prerequisites

```bash
# Required software
- Java 17 or higher
- Scala 2.12.x
- Scala CLI
- Docker (with Hadoop HDFS container)
- Apache Spark 3.3.0+
```

### Dependencies

The project uses the following Spark dependencies:

```scala
//> using scala "2.12.18"
//> using dep "org.apache.spark::spark-sql:3.3.0"
//> using dep "org.apache.hadoop:hadoop-client:3.3.2"
```

## Usage

### Running the Pipeline

**Windows:**
```powershell
.\run.ps1
```

**Linux/Mac:**
```bash
./run.sh
```

**Manual Execution:**
```bash
scala-cli run --java-opt "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" SparkHDFSPreprocess.scala
```

### Configuration

Edit `SparkHDFSPreprocess.scala` to configure:

```scala
val containerId = "4b1572a73e7b"  // Docker container ID
val pairs = List("EURUSD", "GBPUSD", "USDCAD", "USDJPY")  // Currency pairs
val timeframe = "1d"  // Timeframe: 1d, 1h, 1min
```

## Output

### Data Schema

| Column | Type | Description |
|--------|------|-------------|
| Date | Timestamp | Trading date/time |
| Pair | String | Currency pair identifier |
| Timeframe | String | Data granularity (1d/1h/1min) |
| Open | Double | Opening price |
| High | Double | Highest price |
| Low | Double | Lowest price |
| Close | Double | Closing price |
| Volume | Long | Trading volume |
| Range | Double | High - Low |
| Change | Double | Close - Open |
| ChangePct | Double | Percentage change |
| MA5 | Double | 5-period moving average |
| MA20 | Double | 20-period moving average |
| Volatility5 | Double | 5-period standard deviation |
| Gain | Double | Positive price changes |
| Loss | Double | Negative price changes |

### Statistics (Daily Data)

| Pair | Avg Close | Min Close | Max Close | Avg Volatility |
|------|-----------|-----------|-----------|----------------|
| EUR/USD | 1.18449 | 1.05524 | 1.34279 | 0.00641 |
| GBP/USD | 0.83385 | 0.62957 | 1.39264 | 0.00596 |
| USD/CAD | 1.09533 | 0.84194 | 1.33296 | 0.00599 |
| USD/JPY | 133.417 | 107.096 | 157.661 | 0.844 |

## Implementation Details

### Spark Optimizations

**Window Functions:**
```scala
val window5 = Window.orderBy("Date").rowsBetween(-4, 0)
val window20 = Window.orderBy("Date").rowsBetween(-19, 0)
```

**Caching Strategy:**
```scala
processed.cache()  // Cache before aggregations
processed.unpersist()  // Release after operations
```

**Coalesce for Output:**
```scala
processed.coalesce(1).write.mode("overwrite")
```

### HDFS Integration

**Fetching Data:**
```bash
docker exec <container> hdfs dfs -cat /forex/raw/1d/<pair>.csv
```

**Uploading Results:**
```bash
docker exec <container> hdfs dfs -put -f <local_file> <hdfs_path>
```

## Performance

- **Processing Time:** ~6 seconds per currency pair (daily data)
- **Data Volume:** ~600 KB per processed file
- **Memory Usage:** 2.2 GB Spark executor memory
- **Parallelization:** Window operations with degradation warnings (single partition for small datasets)

## Troubleshooting

### Common Issues

**1. RPC Response Size Error**
```
Solution: Use Docker exec commands instead of direct HDFS client
```

**2. Hadoop Home Not Set (Windows)**
```
Solution: Use collect() and manual CSV writing instead of DataFrame.write
```

**3. File Not Found After Processing**
```
Solution: Cache DataFrame before statistics computation
```

### Debug Mode

Add to Spark configuration:
```scala
.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
```

## Development

### Adding New Currency Pairs

1. Add pair to the list in `SparkHDFSPreprocess.scala`
2. Ensure raw data exists in HDFS at `/forex/raw/<timeframe>/<pair>.csv`
3. Run the pipeline

### Extending Features

Modify the `preprocessWithSpark` function to add new calculated columns:

```scala
.withColumn("NewFeature", <calculation>)
```

## License

This project is part of academic coursework for Big Data Analytics.

## Contributors

- Naveen - Big Data Analytics, Semester 6

## Acknowledgments

- Apache Spark community for distributed processing framework
- Hadoop ecosystem for scalable storage solutions
- Forex data providers for historical market data

## References

- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- Hadoop HDFS Guide: https://hadoop.apache.org/docs/stable/
- Scala CLI: https://scala-cli.virtuslab.org/
