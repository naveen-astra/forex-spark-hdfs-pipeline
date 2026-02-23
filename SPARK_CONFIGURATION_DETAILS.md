# Spark Configuration & Project Details

## Dataset Information

### Original Dataset (Raw Data)
- **Location**: `raw2/` directory
- **Total Files**: 45 CSV files
- **Total Size**: 4,400.76 MB (approximately 4.3 GB)
- **Structure**: 
  - 15 currency pairs across 3 timeframes
  - Currency Pairs: EURUSD, GBPUSD, AUDJPY, AUDUSD, EURAUD, EURCHF, EURGBP, EURJPY, GBPAUD, GBPCAD, GBPJPY, NZDUSD, USDCAD, USDCHF, USDJPY
  - Timeframes: 1d (daily), 1h (hourly), 1min (minute)
- **Time Period**: January 1, 2017 - December 31, 2024 (8 years)
- **Records per File**: Approximately 2,922 records for daily timeframe; significantly more for hourly and minute data

### Preprocessed Dataset
- **Location**: `spark_output/` directory  
- **Total Files**: 45 CSV files
- **Total Size**: 11,109.22 MB (approximately 10.8 GB)
- **Size Increase**: 2.52x (due to added feature columns)
- **Added Features**: 
  - Pair and Timeframe metadata columns
  - Range (High - Low)
  - Change (Close - Open)
  - ChangePct (Percentage change)
  - MA5 (5-period Moving Average)
  - MA20 (20-period Moving Average)
  - Volatility5 (5-period rolling standard deviation)
  - Gain and Loss components (for RSI calculation)

---

## Spark Configuration Details

### 1. Spark Cluster Type
- **Mode**: `local[*]` - Standalone Local Mode
- **Description**: Spark runs on a single machine utilizing all available CPU cores
- **Architecture**: Non-distributed cluster; all processing occurs on one machine

### 2. Number of Spark Workers
- **Workers**: 1 worker (local mode)
- **Explanation**: In local mode, Spark operates in a single JVM process on the driver machine
- **Note**: For distributed cluster deployment, configuration would require `.master("spark://host:port")` to connect to a standalone/YARN/Mesos cluster

### 3. Number of Cores
- **Configuration**: `local[*]`
- **Meaning**: Utilizes all available CPU cores on the machine
- **Typical Range**: 4-16 cores depending on hardware specifications
- **Verification Method**: Run `Runtime.getRuntime.availableProcessors()` in Scala

### 4. Number of Executors
- **Executors**: 1 executor (local mode)
- **Explanation**: Local mode executes all operations in a single executor process
- **Note**: Distributed deployments would have multiple executors across worker nodes

### 5. Memory Configuration
```scala
.config("spark.driver.memory", "4g")
```
- **Driver Memory**: 4 GB allocated to the Spark driver
- **Executor Memory**: Same as driver in local mode (4 GB)

### 6. Data Processing API
- **API Used**: DataFrame API
- **Implementation Example**:
  ```scala
  import org.apache.spark.sql.{SparkSession, DataFrame}
  
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"file:///$absPath")
  
  // DataFrame transformations
  df.withColumn("MA5", avg("Close").over(window5))
    .withColumn("Range", col("High") - col("Low"))
  ```
- **Advantages of DataFrame API over RDD**:
  - Catalyst optimizer for query optimization
  - Higher-level API with SQL-like operations  
  - Tungsten execution engine for enhanced performance
  - Schema awareness and type safety

---

## Spark Architecture Summary

```
┌─────────────────────────────────────────────────┐
│         Spark Application (Local Mode)          │
├─────────────────────────────────────────────────┤
│  Driver Program                                  │
│  ├─ SparkContext                                │
│  ├─ DAG Scheduler                               │
│  └─ Task Scheduler                              │
│                                                  │
│  Single Executor (uses all CPU cores)           │
│  ├─ Task 1 (Core 1)                             │
│  ├─ Task 2 (Core 2)                             │
│  ├─ Task 3 (Core 3)                             │
│  └─ Task N (Core N)                             │
│                                                  │
│  Block Manager (4 GB memory)                    │
│  └─ Cached DataFrames                           │
└─────────────────────────────────────────────────┘
```

---

## Processing Statistics

### Performance Metrics
- **Total Files Processed**: 45 files
- **Input Size**: 4.4 GB
- **Output Size**: 10.8 GB
- **Data Expansion**: 2.52x (due to feature engineering columns)
- **Estimated Processing Time**: 15-20 minutes for all 45 files
- **Average Processing Time per File**: Approximately 20-30 seconds

### Spark Optimizations Enabled
```scala
.config("spark.sql.adaptive.enabled", "true")  // Adaptive Query Execution
```
- **Adaptive Query Execution (AQE)**: Dynamically optimizes query plans based on runtime statistics

---

## Technical Stack

| Component | Version/Details |
|-----------|----------------|
| **Language** | Scala 2.12.18 |
| **Spark Version** | Apache Spark 3.3.0 |
| **Hadoop Client** | 3.3.2 |
| **Build Tool** | Scala CLI |
| **JVM Options** | Java 17+ with module exports |
| **Data Format** | CSV (input/output) |
| **Schema Inference** | Automatic via Spark |

---

## Key Spark Operations Used

### 1. **Window Functions** (for Moving Averages)
```scala
val window5 = Window.orderBy("Date").rowsBetween(-4, 0)
val window20 = Window.orderBy("Date").rowsBetween(-19, 0)

df.withColumn("MA5", avg("Close").over(window5))
  .withColumn("MA20", avg("Close").over(window20))
```

### 2. **Column Transformations**
```scala
.withColumn("Range", col("High") - col("Low"))
.withColumn("ChangePct", ((col("Close") - col("Open")) / col("Open") * 100))
```

### 3. **Conditional Logic**
```scala
.withColumn("Gain", when(col("Change") > 0, col("Change")).otherwise(0))
.withColumn("Loss", when(col("Change") < 0, -col("Change")).otherwise(0))
```

### 4. **Filtering**
```scala
.filter(col("MA20").isNotNull)  // Remove first 19 rows with null MA20
```

### 5. **Data Caching**
```scala
val processed = preprocessWithSpark(df, pair, tf).cache()
// ... use DataFrame ...
processed.unpersist()  // Release memory
```

---

## Spark Deployment Mode Comparison

### Current Setup: Local Mode `local[*]`
**Best suited for**:
- Development and testing environments
- Small to medium datasets (under 50 GB)
- Single machine with sufficient RAM
- Rapid prototyping and experimentation

**Limitations**:
- Not suitable for very large datasets (exceeding 100 GB)
- Lacks fault tolerance required for production workloads
- Cannot scale horizontally across multiple machines

### Cluster Mode (Not implemented in this project)
```scala
.master("spark://master:7077")  // Standalone cluster
.master("yarn")                  // YARN cluster
```
**Best suited for**:
- Large-scale data processing (terabyte/petabyte scale)
- Distributed processing across multiple worker nodes
- Production environments
- High availability and fault tolerance requirements

---

## Documentation Screenshots

### Screenshot 1: Original Dataset
Required elements:
- Directory listing of `raw2/` folder
- File sizes and names
- Total size: 4,400.76 MB

### Screenshot 2: Preprocessed Dataset  
Required elements:
- Directory listing of `spark_output/` folder
- File sizes and names
- Total size: 11,109.22 MB

### Screenshot 3: Spark UI (if running)
- URL: http://localhost:4040
- Display: Jobs, Stages, Storage, Environment tabs
- Purpose: Demonstrates Spark execution plan and DAG visualization

### Screenshot 4: Sample Data Comparison
- Before: Raw CSV with columns (Date, Open, High, Low, Close, Volume)
- After: Processed CSV with added features (MA5, MA20, Volatility5, etc.)

---

## Execution Instructions

### Processing All Files
```powershell
# Windows
.\run.ps1

# Or directly with Scala CLI
scala-cli run SparkHDFSPreprocess.scala
```

### Viewing Spark UI During Processing
```scala
scala-cli run SparkUIDemo.scala
```
Access the Spark UI at: http://localhost:4040

---

## Configuration Summary

| Metric | Value |
|--------|-------|
| **Spark Mode** | Local `local[*]` |
| **Cluster Type** | Standalone (Single Machine) |
| **Number of Workers** | 1 |
| **Number of Executors** | 1 |
| **CPU Cores Used** | All available cores |
| **Driver Memory** | 4 GB |
| **API Type** | DataFrame API |
| **Original Data Size** | 4,400.76 MB |
| **Preprocessed Data Size** | 11,109.22 MB |
| **Total Files** | 45 (15 pairs across 3 timeframes) |
| **Spark Version** | 3.3.0 |
| **Optimization** | Adaptive Query Execution enabled |
