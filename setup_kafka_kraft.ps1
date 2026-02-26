# ============================================================
# setup_kafka_kraft.ps1
# Installs and configures Apache Kafka in KRaft mode (no Zookeeper, no Docker)
# Java 11+ required (detected Java 23)
# ============================================================

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

$KAFKA_VERSION   = "3.6.1"
$SCALA_VERSION   = "2.13"
$KAFKA_DIR       = "C:\kafka"
$KAFKA_DOWNLOAD  = "https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz"
$KAFKA_TGZ       = "$env:TEMP\kafka.tgz"
$TOPIC_NAME      = "forex-live"
$KAFKA_BROKER    = "localhost:9092"

function Write-Header {
    param([string]$Title)
    Write-Host ""
    Write-Host ("=" * 65) -ForegroundColor Cyan
    Write-Host "  $Title" -ForegroundColor Cyan
    Write-Host ("=" * 65) -ForegroundColor Cyan
    Write-Host ""
}

function Wait-KafkaReady {
    Write-Host "Waiting for Kafka to be ready..." -ForegroundColor Yellow
    $maxTries = 20
    for ($i = 1; $i -le $maxTries; $i++) {
        Start-Sleep -Seconds 2
        $result = & "$KAFKA_DIR\bin\windows\kafka-topics.bat" `
            --bootstrap-server $KAFKA_BROKER --list 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Kafka is ready!" -ForegroundColor Green
            return $true
        }
        Write-Host "  Attempt $i/$maxTries..." -ForegroundColor Gray
    }
    Write-Host "Kafka did not become ready in time." -ForegroundColor Red
    return $false
}

switch ($Command.ToLower()) {

    # ----------------------------------------------------------
    "install" {
        Write-Header "Installing Kafka $KAFKA_VERSION (KRaft Mode)"

        # Check Java
        $javaVer = java -version 2>&1 | Select-String "version"
        Write-Host "Java detected: $javaVer" -ForegroundColor Green

        # Download Kafka
        if (Test-Path "$KAFKA_DIR\bin\windows\kafka-server-start.bat") {
            Write-Host "Kafka already installed at $KAFKA_DIR" -ForegroundColor Green
        } else {
            Write-Host "Downloading Kafka $KAFKA_VERSION from Apache..."
            Write-Host "URL: $KAFKA_DOWNLOAD"
            Write-Host "(This may take 1-2 minutes on slow connections...)" -ForegroundColor Yellow
            
            # Use BITS (Background Intelligent Transfer) - Windows built-in, supports resume
            $MIRROR = "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz"
            Write-Host "Downloading via BITS transfer (supports resume, no timeout)..." -ForegroundColor Yellow
            try {
                Start-BitsTransfer -Source $MIRROR -Destination $KAFKA_TGZ -DisplayName "Kafka $KAFKA_VERSION" -Description "Downloading Kafka from archive.apache.org"
                Write-Host "Download complete." -ForegroundColor Green
            } catch {
                Write-Host "BITS failed: $_. Falling back to curl.exe..." -ForegroundColor Yellow
                curl.exe -L -C - --max-time 3600 --retry 3 --retry-delay 10 -o $KAFKA_TGZ $MIRROR
                if ($LASTEXITCODE -ne 0) { Write-Host "Download failed." -ForegroundColor Red; exit 1 }
                Write-Host "Download complete." -ForegroundColor Green
            }

            # Extract
            Write-Host "Extracting to $KAFKA_DIR ..."
            New-Item -ItemType Directory -Path $KAFKA_DIR -Force | Out-Null
            tar -xzf $KAFKA_TGZ -C $KAFKA_DIR --strip-components=1
            Write-Host "Extraction complete." -ForegroundColor Green
        }

        # Configure KRaft - update server.properties
        Write-Host "Configuring KRaft mode..."
        $kraftConfig = "$KAFKA_DIR\config\kraft\server.properties"

        # Generate cluster UUID
        $uuid = & "$KAFKA_DIR\bin\windows\kafka-storage.bat" random-uuid 2>&1 |
                Where-Object { $_ -match "^[A-Za-z0-9_-]{22}$" } |
                Select-Object -First 1

        if (-not $uuid) {
            # Fallback: generate a random UUID manually
            $uuid = [System.Convert]::ToBase64String([System.Guid]::NewGuid().ToByteArray()).TrimEnd('=').Replace('+','-').Replace('/','_')
        }

        Write-Host "Cluster UUID: $uuid" -ForegroundColor Cyan

        # Save UUID for later use
        $uuid | Out-File "$KAFKA_DIR\cluster.uuid" -Encoding utf8

        # Format KRaft storage
        Write-Host "Formatting KRaft storage..."
        & "$KAFKA_DIR\bin\windows\kafka-storage.bat" format `
            -t $uuid `
            -c $kraftConfig 2>&1 | Write-Host

        Write-Host ""
        Write-Host "Kafka installed and configured in KRaft mode." -ForegroundColor Green
        Write-Host ""
        Write-Host "Next step:" -ForegroundColor Yellow
        Write-Host "  .\setup_kafka_kraft.ps1 start"
    }

    # ----------------------------------------------------------
    "start" {
        Write-Header "Starting Kafka (KRaft Mode - No Zookeeper)"

        if (-not (Test-Path "$KAFKA_DIR\bin\windows\kafka-server-start.bat")) {
            Write-Host "Kafka not found. Run install first:" -ForegroundColor Red
            Write-Host "  .\setup_kafka_kraft.ps1 install"
            exit 1
        }

        # Check if already running
        $existing = Get-Process -Name "java" -ErrorAction SilentlyContinue |
                    Where-Object { $_.CommandLine -like "*kafka*" }
        if ($existing) {
            Write-Host "Kafka is already running (PID $($existing.Id))." -ForegroundColor Yellow
        } else {
            Write-Host "Starting Kafka broker in background..."
            $kraftConfig = "$KAFKA_DIR\config\kraft\server.properties"

            # Start Kafka as a background job
            Start-Process -FilePath "cmd.exe" `
                -ArgumentList "/c `"$KAFKA_DIR\bin\windows\kafka-server-start.bat`" `"$kraftConfig`"" `
                -WindowStyle Minimized `
                -PassThru | Out-Null

            Write-Host "Kafka starting..." -ForegroundColor Yellow
            Start-Sleep -Seconds 8
        }

        # Create topic if it doesn't exist
        Write-Host "Creating topic '$TOPIC_NAME'..."
        & "$KAFKA_DIR\bin\windows\kafka-topics.bat" `
            --create `
            --topic $TOPIC_NAME `
            --bootstrap-server $KAFKA_BROKER `
            --partitions 3 `
            --replication-factor 1 `
            --if-not-exists 2>&1 | Write-Host

        Write-Host ""
        Write-Host "Kafka is running!" -ForegroundColor Green
        Write-Host "  Broker : $KAFKA_BROKER"
        Write-Host "  Topic  : $TOPIC_NAME"
        Write-Host ""
        Write-Host "Next steps (separate terminals):" -ForegroundColor Yellow
        Write-Host "  Terminal 2: .\run_streaming.ps1 stream"
        Write-Host "  Terminal 3: .\run_streaming.ps1 producer"
    }

    # ----------------------------------------------------------
    "stop" {
        Write-Header "Stopping Kafka"
        & "$KAFKA_DIR\bin\windows\kafka-server-stop.bat" 2>&1 | Write-Host
        Write-Host "Kafka stopped." -ForegroundColor Green
    }

    # ----------------------------------------------------------
    "status" {
        Write-Header "Kafka Status"
        $topics = & "$KAFKA_DIR\bin\windows\kafka-topics.bat" `
            --bootstrap-server $KAFKA_BROKER --list 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Kafka is RUNNING" -ForegroundColor Green
            Write-Host "Topics: $topics"
        } else {
            Write-Host "Kafka is NOT running" -ForegroundColor Red
        }
    }

    # ----------------------------------------------------------
    "topic" {
        Write-Header "Kafka Topic Info: $TOPIC_NAME"
        & "$KAFKA_DIR\bin\windows\kafka-topics.bat" `
            --describe `
            --topic $TOPIC_NAME `
            --bootstrap-server $KAFKA_BROKER 2>&1 | Write-Host
    }

    # ----------------------------------------------------------
    "clean" {
        Write-Header "Cleaning Kafka Logs and Offsets"
        Stop-Process -Name "java" -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
        Remove-Item -Recurse -Force "$env:TEMP\kafka-logs" -ErrorAction SilentlyContinue
        Remove-Item -Recurse -Force "C:\tmp\kraft-combined-logs" -ErrorAction SilentlyContinue
        Write-Host "Clean complete. Re-run 'install' to re-format storage." -ForegroundColor Green
    }

    # ----------------------------------------------------------
    default {
        Write-Header "Kafka KRaft Setup - Help"
        Write-Host "Commands:" -ForegroundColor Yellow
        Write-Host "  install  - Download and configure Kafka in KRaft mode"
        Write-Host "  start    - Start Kafka broker + create forex-live topic"
        Write-Host "  stop     - Stop Kafka broker"
        Write-Host "  status   - Check if Kafka is running"
        Write-Host "  topic    - Show topic details"
        Write-Host "  clean    - Remove logs (use before reinstall)"
        Write-Host ""
        Write-Host "Full setup sequence:" -ForegroundColor Cyan
        Write-Host "  Step 1: .\setup_kafka_kraft.ps1 install"
        Write-Host "  Step 2: .\setup_kafka_kraft.ps1 start"
        Write-Host "  Step 3: .\run_streaming.ps1 stream    (Terminal 2)"
        Write-Host "  Step 4: .\run_streaming.ps1 producer  (Terminal 3)"
        Write-Host ""
        Write-Host "No Docker, no Zookeeper required." -ForegroundColor Green
    }
}
