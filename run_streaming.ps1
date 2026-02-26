# ============================================================
# run_streaming.ps1 - Forex Kafka + Spark Streaming Pipeline
# KRaft Mode (no Docker, no Zookeeper)
# ============================================================
#
# Prerequisites: Run once to install Kafka:
#   .\setup_kafka_kraft.ps1 install
#
# Usage:
#   .\run_streaming.ps1 start     - Start Kafka (KRaft mode)
#   .\run_streaming.ps1 producer  - Start Kafka Producer
#   .\run_streaming.ps1 stream    - Start Spark Streaming Consumer
#   .\run_streaming.ps1 stop      - Stop Kafka broker
#   .\run_streaming.ps1 status    - Show Kafka status
#   .\run_streaming.ps1 logs      - Show Kafka broker logs
#   .\run_streaming.ps1 topic     - List all Kafka topics
#
# Full pipeline (3 terminals required):
#   Terminal 1: .\run_streaming.ps1 start
#   Terminal 2: .\run_streaming.ps1 stream
#   Terminal 3: .\run_streaming.ps1 producer
# ============================================================

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

$KAFKA_TOPIC   = "forex-live"
$KAFKA_BROKER  = "localhost:9092"
$SPARK_UI      = "http://localhost:4040"
$KAFKA_DIR     = "C:\kafka"

function Write-Header {
    param([string]$Title)
    Write-Host ""
    Write-Host ("=" * 60) -ForegroundColor Cyan
    Write-Host "  $Title" -ForegroundColor Cyan
    Write-Host ("=" * 60) -ForegroundColor Cyan
    Write-Host ""
}

switch ($Command.ToLower()) {

    # ----------------------------------------------------------
    "start" {
        Write-Header "Starting Kafka (KRaft Mode)"
        if (-not (Test-Path "$KAFKA_DIR\bin\windows\kafka-server-start.bat")) {
            Write-Host "Kafka not found at $KAFKA_DIR." -ForegroundColor Red
            Write-Host "Run first: .\setup_kafka_kraft.ps1 install" -ForegroundColor Yellow
            exit 1
        }
        .\setup_kafka_kraft.ps1 start
    }

    # ----------------------------------------------------------
    "producer" {
        Write-Header "Starting Forex Kafka Producer"
        Write-Host "Publishing forex data to topic: $KAFKA_TOPIC" -ForegroundColor Yellow
        Write-Host ""
        scala-cli run ForexKafkaProducer.scala
    }

    # ----------------------------------------------------------
    "stream" {
        Write-Header "Starting Spark Structured Streaming Consumer"
        Write-Host "Reading from Kafka topic : $KAFKA_TOPIC" -ForegroundColor Yellow
        Write-Host "Spark UI                 : $SPARK_UI" -ForegroundColor Cyan
        Write-Host "Output directory         : streaming_output/" -ForegroundColor Cyan
        Write-Host ""
        scala-cli run SparkKafkaStreaming.scala
    }

    # ----------------------------------------------------------
    "stop" {
        Write-Header "Stopping Kafka Broker"
        .\setup_kafka_kraft.ps1 stop
    }

    # ----------------------------------------------------------
    "status" {
        Write-Header "Kafka Status"
        .\setup_kafka_kraft.ps1 status
        Write-Host ""
        Write-Host "Spark UI : $SPARK_UI (only active when streaming)" -ForegroundColor Cyan
    }

    # ----------------------------------------------------------
    "logs" {
        Write-Header "Kafka Broker Logs"
        $logDir = "$KAFKA_DIR\logs"
        if (Test-Path $logDir) {
            Get-ChildItem $logDir -Filter "*.log" |
                Sort-Object LastWriteTime -Descending |
                Select-Object -First 1 |
                ForEach-Object { Get-Content $_.FullName -Tail 50 }
        } else {
            Write-Host "Log directory not found: $logDir" -ForegroundColor Yellow
        }
    }

    # ----------------------------------------------------------
    "topic" {
        Write-Header "List Kafka Topics"
        & "$KAFKA_DIR\bin\windows\kafka-topics.bat" `
            --bootstrap-server $KAFKA_BROKER `
            --list
    }

    # ----------------------------------------------------------
    default {
        Write-Header "Forex Kafka + Spark Streaming - Help"
        Write-Host "Prerequisites:" -ForegroundColor Yellow
        Write-Host "  .\setup_kafka_kraft.ps1 install   (run once)"
        Write-Host ""
        Write-Host "Commands:" -ForegroundColor Yellow
        Write-Host "  start    - Start Kafka broker (KRaft mode, no Docker)"
        Write-Host "  producer - Start Kafka Producer (publishes forex CSV data)"
        Write-Host "  stream   - Start Spark Structured Streaming consumer"
        Write-Host "  stop     - Stop Kafka broker"
        Write-Host "  status   - Show Kafka broker status"
        Write-Host "  logs     - Show last 50 lines of Kafka logs"
        Write-Host "  topic    - List all Kafka topics"
        Write-Host ""
        Write-Host "Full pipeline (3 terminals):" -ForegroundColor Cyan
        Write-Host "  Terminal 1 : .\run_streaming.ps1 start"
        Write-Host "  Terminal 2 : .\run_streaming.ps1 stream"
        Write-Host "  Terminal 3 : .\run_streaming.ps1 producer"
        Write-Host ""
        Write-Host "Spark UI : $SPARK_UI"
    }
}
