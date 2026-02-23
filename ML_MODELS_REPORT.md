# Machine Learning Models Report
## Forex Price Prediction and Trading Signal Generation using Apache Spark MLlib

**Project**: Distributed Forex Data Preprocessing Pipeline Using Apache Spark and HDFS  
**Framework**: Apache Spark MLlib 3.3.0  
**Language**: Scala 2.12.18  
**Date**: February 23, 2026

---

## 1. Overview

This report documents the implementation and evaluation of three machine learning models trained using Apache Spark MLlib on preprocessed forex market data. The models address two distinct prediction tasks:

1. **Regression** — Predict the next day's closing price for a given currency pair
2. **Classification** — Predict the direction of price movement (Buy / Sell signal)

All models were trained on daily timeframe data covering 15 major currency pairs over an 8-year period (2017–2024).

---

## 2. Dataset

| Property | Details |
|----------|---------|
| **Source** | Preprocessed output from SparkHDFSPreprocess.scala (`spark_output/`) |
| **Currency Pairs** | EURUSD, GBPUSD, USDJPY, USDCAD, AUDUSD, AUDJPY, EURAUD, EURCHF, EURGBP, EURJPY, GBPAUD, GBPCAD, GBPJPY, NZDUSD, USDCHF |
| **Timeframe** | Daily (1d) |
| **Period** | January 1, 2017 - December 31, 2024 |
| **Total Records Loaded** | 43,830 |
| **Records After Feature Engineering** | 43,770 |
| **Train Set** | 11,672 records (80%) |
| **Test Set** | 32,098 records (20%) |

---

## 3. Feature Engineering

Before model training, the following additional features were derived from the preprocessed data:

### 3.1 Lag Features
Lag features capture temporal dependencies in the time series by providing the model access to historical values.

| Feature | Description |
|---------|-------------|
| `Close_lag1` | Closing price 1 day prior |
| `Close_lag2` | Closing price 2 days prior |
| `Close_lag3` | Closing price 3 days prior |
| `Change_lag1` | Price change 1 day prior |
| `Change_lag2` | Price change 2 days prior |

### 3.2 Derived Ratio Features

| Feature | Description |
|---------|-------------|
| `MA5_MA20_Ratio` | Ratio of 5-day to 20-day moving average; indicates short-term vs long-term trend momentum |

### 3.3 Complete Feature Vector (19 Features)

```
Open, High, Low, Close, Volume,
Range, Change, ChangePct,
MA5, MA20, Volatility5,
Gain, Loss,
Close_lag1, Close_lag2, Close_lag3,
Change_lag1, Change_lag2,
MA5_MA20_Ratio
```

### 3.4 Target Labels

| Label | Type | Description |
|-------|------|-------------|
| `NextDayClose` | Continuous (Double) | Closing price of the following trading day — used for regression |
| `PriceDirection` | Binary (0.0 / 1.0) | 1.0 if next day's close is higher than today's; 0.0 otherwise — used for classification |

### 3.5 Preprocessing Pipeline

All models use the following shared pipeline stages before the learning algorithm:

1. **VectorAssembler** — Combines all 19 feature columns into a single feature vector
2. **StandardScaler** — Normalizes features to zero mean and unit standard deviation (prevents scale bias)

```scala
val assembler = new VectorAssembler()
  .setInputCols(FEATURE_COLS)
  .setOutputCol("raw_features")
  .setHandleInvalid("skip")

val scaler = new StandardScaler()
  .setInputCol("raw_features")
  .setOutputCol("features")
  .setWithStd(true)
  .setWithMean(true)
```

---

## 4. Models

### 4.1 Model 1 — Random Forest Regressor

#### Description
Random Forest is an ensemble learning method that constructs multiple decision trees during training and averages their predictions to improve accuracy and control overfitting. It is well-suited for tabular financial data with non-linear relationships.

#### Configuration

```scala
val rfRegressor = new RandomForestRegressor()
  .setLabelCol("NextDayClose")
  .setFeaturesCol("features")
  .setNumTrees(100)
  .setMaxDepth(10)
  .setMinInstancesPerNode(5)
  .setSeed(42)
```

| Hyperparameter | Value | Rationale |
|----------------|-------|-----------|
| `numTrees` | 100 | Sufficient ensemble size for stable predictions |
| `maxDepth` | 10 | Controls tree complexity and prevents overfitting |
| `minInstancesPerNode` | 5 | Ensures leaf nodes have sufficient data points |
| `seed` | 42 | Fixed seed for reproducibility |

#### Evaluation Metrics

| Metric | Value |
|--------|-------|
| **RMSE** (Root Mean Squared Error) | 6.398515 |
| **MAE** (Mean Absolute Error) | 2.012709 |
| **R2** (Coefficient of Determination) | **0.987185** |

An R2 of 0.987 indicates that the model explains **98.7%** of the variance in the next day's closing price, demonstrating strong predictive capability.

#### Feature Importances (Top 5)

| Rank | Feature | Importance |
|------|---------|------------|
| 1 | High | 18.56% |
| 2 | Open | 12.25% |
| 3 | Close_lag3 | 12.10% |
| 4 | Close_lag1 | 11.76% |
| 5 | MA5 | 8.14% |

#### Sample Predictions

| Date | Pair | Actual Close | Actual Next Day | RF Predicted |
|------|------|-------------|-----------------|--------------|
| 2024-12-30 | EURCHF | 1.09768 | 1.09608 | 1.10517 |
| 2024-12-30 | GBPJPY | 90.39016 | 90.79612 | 86.97605 |
| 2024-12-30 | EURJPY | 153.94621 | 155.59517 | 156.47793 |
| 2024-12-30 | GBPUSD | 0.65305 | 0.64916 | 0.65500 |
| 2024-12-30 | EURUSD | 1.12699 | 1.12780 | 1.10355 |

---

### 4.2 Model 2 — Gradient Boosted Trees Regressor

#### Description
Gradient Boosted Trees (GBT) is a sequential ensemble method where each tree corrects the errors of the previous one by minimizing a loss function. GBT generally achieves higher accuracy than Random Forest but is more sensitive to hyperparameter tuning and can be prone to overfitting without regularization.

#### Configuration

```scala
val gbtRegressor = new GBTRegressor()
  .setLabelCol("NextDayClose")
  .setFeaturesCol("features")
  .setMaxIter(100)
  .setMaxDepth(5)
  .setStepSize(0.05)
  .setSeed(42)
```

| Hyperparameter | Value | Rationale |
|----------------|-------|-----------|
| `maxIter` | 100 | Number of boosting rounds |
| `maxDepth` | 5 | Shallower trees reduce overfitting in GBT |
| `stepSize` | 0.05 | Conservative learning rate for better generalization |
| `seed` | 42 | Fixed seed for reproducibility |

#### Evaluation Metrics

| Metric | Value |
|--------|-------|
| **RMSE** (Root Mean Squared Error) | 7.493041 |
| **MAE** (Mean Absolute Error) | 1.876358 |
| **R2** (Coefficient of Determination) | **0.982425** |

An R2 of 0.982 indicates that the model explains **98.2%** of the variance. While GBT achieves a lower MAE than Random Forest (1.876 vs 2.013), its higher RMSE and R2 indicates higher variance in large prediction errors.

#### Feature Importances (Top 5)

| Rank | Feature | Importance |
|------|---------|------------|
| 1 | High | 96.20% |
| 2 | MA5 | 1.28% |
| 3 | Volatility5 | 0.57% |
| 4 | Low | 0.44% |
| 5 | Close | 0.42% |

> **Observation**: GBT became heavily reliant on the `High` feature (96.2%), suggesting potential feature dominance. This may indicate that the model collapsed to a near-trivial predictor. Removing `High` and retraining would provide a more meaningful model.

#### Sample Predictions

| Date | Pair | Actual Close | Actual Next Day | GBT Predicted |
|------|------|-------------|-----------------|---------------|
| 2024-12-30 | EURCHF | 1.09768 | 1.09608 | 1.06649 |
| 2024-12-30 | GBPJPY | 90.39016 | 90.79612 | 86.63196 |
| 2024-12-30 | EURJPY | 153.94621 | 155.59517 | 156.87244 |
| 2024-12-30 | GBPUSD | 0.65305 | 0.64916 | 0.65047 |
| 2024-12-30 | EURUSD | 1.12699 | 1.12780 | 1.07076 |

---

### 4.3 Model 3 — Logistic Regression Classifier

#### Description
Logistic Regression is a linear classification model that estimates the probability of a binary outcome. Here it is used to classify whether the next day's closing price will be higher (Buy = 1) or lower (Sell = 0) than the current day's price.

#### Configuration

```scala
val lrClassifier = new LogisticRegression()
  .setLabelCol("PriceDirection")
  .setFeaturesCol("features")
  .setMaxIter(200)
  .setRegParam(0.01)
  .setElasticNetParam(0.0)
  .setFamily("binomial")
```

| Hyperparameter | Value | Rationale |
|----------------|-------|-----------|
| `maxIter` | 200 | Sufficient iterations for convergence |
| `regParam` | 0.01 | L2 regularization to prevent overfitting |
| `elasticNetParam` | 0.0 | Pure L2 regularization (Ridge) |
| `family` | binomial | Binary classification (Buy / Sell) |

#### Evaluation Metrics

| Metric | Value |
|--------|-------|
| **AUC-ROC** | 0.5042 |
| **Accuracy** | 49.86% |
| **F1 Score** | 0.4426 |
| **Precision** | 0.4965 |
| **Recall** | 0.4986 |

#### Confusion Matrix

| | Predicted: SELL (0) | Predicted: BUY (1) |
|--|---------------------|---------------------|
| **Actual: DOWN (0)** | 13,106 (True Negative) | 2,977 (False Positive) |
| **Actual: UP (1)** | 13,117 (False Negative) | 2,898 (True Positive) |

#### Analysis

An AUC-ROC of 0.5042 and accuracy of 49.86% indicate that the Logistic Regression model performs at approximately **random chance level** for price direction prediction. This is expected for a linear model applied to a highly non-linear and noisy financial time series.

**Reasons for poor performance**:
- Forex price direction is inherently noisy and non-linear
- Logistic Regression assumes a linear decision boundary, which is insufficient for this domain
- The feature set, while relevant for price level prediction, may not capture directional momentum effectively without additional indicators (e.g., RSI, MACD, Bollinger Bands)

**Recommendation**: Replace Logistic Regression with a Random Forest Classifier or Gradient Boosted Trees Classifier for significantly improved directional prediction.

#### Sample Predictions (Trading Signals)

| Date | Pair | Close | Actual | Signal |
|------|------|-------|--------|--------|
| 2024-12-30 | EURJPY | 153.94621 | UP | SELL |
| 2024-12-30 | GBPCAD | 2.00498 | DOWN | SELL |
| 2024-12-30 | USDCHF | 0.79670 | DOWN | SELL |
| 2024-12-30 | EURUSD | 1.12699 | UP | SELL |
| 2024-12-30 | NZDUSD | 0.56372 | DOWN | BUY |
| 2024-12-30 | GBPUSD | 0.65305 | DOWN | SELL |

---

## 5. Model Comparison Summary

### 5.1 Regression Models

| Model | RMSE | MAE | R2 | Verdict |
|-------|------|-----|----|---------|
| Random Forest Regressor | 6.398515 | 2.012709 | **0.987185** | Best overall |
| Gradient Boosted Trees | 7.493041 | 1.876358 | 0.982425 | Lower MAE, higher RMSE |

### 5.2 Classification Model

| Model | AUC-ROC | Accuracy | F1 Score | Verdict |
|-------|---------|----------|----------|---------|
| Logistic Regression | 0.5042 | 49.86% | 0.4426 | Near-random; needs upgrade |

---

## 6. Output Files

All model predictions are saved to the `ml_output/` directory:

| File | Model | Contents |
|------|-------|----------|
| `ml_output/rf_predictions.csv` | Random Forest | Date, Pair, Close, NextDayClose, RF_Predicted_Close |
| `ml_output/gbt_predictions.csv` | GBT | Date, Pair, Close, NextDayClose, GBT_Predicted_Close |
| `ml_output/lr_signals.csv` | Logistic Regression | Date, Pair, Close, Actual Direction, Signal (BUY/SELL) |

---

## 7. Pipeline Architecture

```
spark_output/*.csv (Preprocessed Data)
        |
        v
 Load & Cast to Double
        |
        v
 Feature Engineering
 (Lag Features, MA Ratio)
        |
        v
   na.drop() → Clean Dataset (43,770 records)
        |
        v
  Train/Test Split (80% / 20%)
        |
   _____|______________________
  |                            |
  v                            v
Regression Task          Classification Task
(NextDayClose label)     (PriceDirection label)
  |         |                  |
  v         v                  v
  RF       GBT            Logistic Reg
  |         |                  |
  v         v                  v
RMSE=6.40  RMSE=7.49      AUC=0.50
R2=0.987   R2=0.982       Acc=49.86%
        |
        v
  ml_output/ (CSV predictions saved)
```

---

## 8. Spark Configuration Used

| Parameter | Value |
|-----------|-------|
| Spark Master | `local[*]` |
| Driver Memory | 4 GB |
| Adaptive Query Execution | Enabled |
| Spark Version | 3.3.0 |
| Scala Version | 2.12.18 |

---

## 9. Recommendations for Improvement

| Issue | Recommendation |
|-------|---------------|
| Logistic Regression performs at random | Replace with Random Forest Classifier or GBT Classifier |
| GBT dominated by `High` feature | Retrain after removing `High` and `Open` to avoid data leakage |
| No cross-validation used | Implement k-fold cross-validation for more reliable evaluation |
| No hyperparameter tuning | Use `CrossValidator` + `ParamGridBuilder` for optimal hyperparameters |
| Missing technical indicators | Add RSI, MACD, Bollinger Bands as additional features |
| Per-pair models | Train individual models per currency pair for specialized predictions |
