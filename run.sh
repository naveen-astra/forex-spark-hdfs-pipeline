#!/bin/bash

# Forex Data Preprocessing Runner Script

echo "=== Forex Data Preprocessing ==="
echo ""

# Step 1: Upload data to HDFS
echo "Step 1: Uploading data to HDFS..."
sbt "runMain HDFSDataLoader"

if [ $? -ne 0 ]; then
    echo "Error: Failed to upload data to HDFS"
    exit 1
fi

echo ""
echo "Step 2: Preprocessing data with Spark..."
sbt "runMain ForexDataPreprocessor"

if [ $? -ne 0 ]; then
    echo "Error: Failed to preprocess data"
    exit 1
fi

echo ""
echo "=== Processing Complete ==="
echo "Check HDFS for processed data:"
echo "  hdfs dfs -ls /user/forex/processed/"
