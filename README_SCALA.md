# Forex Data Preprocessing with Scala

This project fetches forex data from HDFS and preprocesses it using Scala.

## Project Structure

```
bda-forex/
├── build.sbt                          # SBT build configuration
├── src/main/scala/
│   ├── ForexDataPreprocessor.scala    # Spark-based preprocessor
│   ├── HDFSDataLoader.scala          # Upload files to HDFS
│   └── SimplePreprocessor.scala      # Simple HDFS preprocessor
└── raw2/                             # Local raw CSV files
```

## Prerequisites

- Hadoop running at `hdfs://localhost:9870`
- Container ID: `4b1572a73e7b33e5355cff51616b584a2d3e7450eb9f927f512c84279f02324e`
- Scala 2.12.18
- SBT (Scala Build Tool)
- Apache Spark 3.5.0 (for ForexDataPreprocessor)

## Setup

1. **Upload data to HDFS:**

```bash
# Using Hadoop CLI
hadoop fs -mkdir -p /user/forex/raw2
hadoop fs -put raw2/*.csv /user/forex/raw2/

# Or using the HDFSDataLoader Scala script
sbt "runMain HDFSDataLoader"
```

2. **Verify files in HDFS:**

```bash
hdfs dfs -ls /user/forex/raw2/
```

## Running the Preprocessor

### Option 1: Spark-based Preprocessor (Recommended)

This version uses Apache Spark for distributed processing:

```bash
sbt "runMain ForexDataPreprocessor"
```

**Features:**
- Reads all 45 CSV files from HDFS
- Calculates technical indicators (MA, volatility, RSI components)
- Adds time-based features (day, month, year)
- Saves processed data as Parquet files
- Displays statistics for each currency pair

### Option 2: Simple Preprocessor

Lightweight version without Spark dependencies:

```bash
sbt "runMain SimplePreprocessor"
```

**Features:**
- Direct HDFS read/write using Hadoop API
- Basic feature engineering (price range, change %)
- Saves as CSV
- Good for testing and small datasets

## Preprocessing Features

The preprocessor adds the following features to each record:

1. **Basic Features:**
   - Price Range (High - Low)
   - Price Change (Close - Open)
   - Price Change Percentage

2. **Technical Indicators:**
   - Moving Average (5 periods)
   - Moving Average (20 periods)
   - Volatility (5-period standard deviation)
   - Gain/Loss (for RSI calculation)

3. **Time Features:**
   - Day of week
   - Month
   - Year

## Output

Processed data is saved to HDFS at:
```
/user/forex/processed/<PAIR>_<TIMEFRAME>_processed/
```

## Data Schema

Input CSV format:
```
Date,Open,High,Low,Close,Volume
2017-01-01,1.185,1.1901,1.17141,1.18404,6615
```

Processed output includes additional columns:
```
timestamp, currency_pair, timeframe, Open, High, Low, Close, Volume,
price_range, price_change, price_change_pct, ma_5, ma_20, volatility_5,
gain, loss, day_of_week, month, year
```

## Docker Commands

If running Hadoop in Docker:

```bash
# Access HDFS from container
docker exec -it 4b1572a73e7b /bin/bash
hdfs dfs -ls /user/forex/raw2/

# Check HDFS web UI
# http://localhost:9870
```

## Troubleshooting

1. **Connection refused error:**
   - Verify Hadoop is running: `jps` or check docker container status
   - Check namenode web UI at http://localhost:9870

2. **File not found:**
   - Ensure files are uploaded to HDFS
   - Check path: `hdfs dfs -ls /user/forex/raw2/`

3. **Out of memory:**
   - Reduce the number of files processed at once
   - Increase JVM heap size in build.sbt

## Next Steps

After preprocessing:
1. Load processed Parquet files for analysis
2. Train machine learning models
3. Perform time series forecasting
4. Create visualizations
