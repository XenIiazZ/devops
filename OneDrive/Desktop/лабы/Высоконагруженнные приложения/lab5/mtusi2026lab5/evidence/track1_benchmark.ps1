<#
Lab 5 / Track 1 benchmark for the existing RabbitMQ-based project.

No edits are made to app.py, worker.py, docker-compose.yml, or other application files.
The script only:
  * creates test PNGs and evidence under .\evidence\track1_<timestamp>\;
  * scales the existing Compose worker service to 1, 2, and 3 replicas;
  * uploads test images and records API latency, worker-start lag, queue depth,
    worker RSS, and throughput;
  * stops workers for a bounded-admission probe, then restarts them to measure recovery;
  * restores the original worker replica count when it exits.

Prefetch is NOT changed. The script records the value found in the running worker.py.
If prefetch is hardcoded (as in the current version), only concurrency/replica count
is varied. A 429/503 during the admission probe is evidence of an application-side
admission limit; if all requests are accepted, the report explicitly says that
bounded admission/backpressure was not demonstrated.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Token,

    [Parameter(Mandatory = $true)]
    [string]$ChatId,

    [string]$BaseUrl = "http://localhost:8080",

    [ValidateRange(1, 20)]
    [int]$SamplesPerSize = 5,

    [int[]]$WorkerProfiles = @(1, 2, 3),

    [ValidateRange(1, 100)]
    [int]$RecoveryProbeCount = 20,

    [ValidateRange(30, 1800)]
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($Token)) { throw "Параметр -Token пустой. Обнови сессию и передай непустой access token." }
if ([string]::IsNullOrWhiteSpace($ChatId)) { throw "Параметр -ChatId пустой. Передай GUID чата как -ChatId ([string]`$chat.id)." }

function Find-ProjectRoot {
    $candidates = @()
    if ($PSScriptRoot) {
        $candidates += $PSScriptRoot
        $parent = Split-Path -Parent $PSScriptRoot
        if ($parent) { $candidates += $parent }
    }
    $candidates += (Get-Location).Path

    foreach ($candidate in ($candidates | Select-Object -Unique)) {
        foreach ($composeName in @("docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml")) {
            if (Test-Path (Join-Path $candidate $composeName)) {
                return (Resolve-Path $candidate).Path
            }
        }
    }
    throw "Не найден docker-compose.yml/compose.yaml. Положи скрипт в корень проекта или в его папку evidence."
}

function Write-Utf8File {
    param([string]$Path, [string]$Content)
    $encoding = [System.Text.UTF8Encoding]::new($true)
    [System.IO.File]::WriteAllText($Path, $Content, $encoding)
}

function Invoke-ComposeText {
    param([string[]]$ComposeArgs)
    $output = & docker compose @ComposeArgs 2>&1
    $exitCode = $LASTEXITCODE
    $text = ($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    if ($exitCode -ne 0) {
        throw "docker compose $($ComposeArgs -join ' ') завершился с кодом $exitCode`n$text"
    }
    return $text
}

function Get-WorkerContainerIds {
    $ids = @(& docker compose ps -q worker 2>$null | Where-Object { $_ -and $_.ToString().Trim() -ne "" })
    return @($ids | ForEach-Object { $_.ToString().Trim() } | Select-Object -Unique)
}

function Set-WorkerReplicas {
    param([int]$Count, [string]$LogPath)
    $output = Invoke-ComposeText -ComposeArgs @(
        "up", "-d", "--no-deps", "--force-recreate", "--scale", "worker=$Count", "worker"
    )
    Write-Utf8File -Path $LogPath -Content $output
    Start-Sleep -Seconds 5
    $ids = Get-WorkerContainerIds
    if ($ids.Count -ne $Count) {
        throw "Запрошено worker=$Count, но Compose показывает контейнеров: $($ids.Count). Проверь docker compose ps."
    }
}

function Convert-ToDto {
    param($Value)
    if ($null -eq $Value -or [string]$Value -eq "") { return $null }
    try {
        if ($Value -is [datetime]) {
            return [DateTimeOffset]$Value
        }
        return [DateTimeOffset]::Parse(
            [string]$Value,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal
        )
    }
    catch {
        try {
            return [DateTimeOffset]::Parse(
                [string]$Value,
                [Globalization.CultureInfo]::GetCultureInfo("ru-RU"),
                [Globalization.DateTimeStyles]::AssumeLocal
            )
        }
        catch { return $null }
    }
}

function Get-HttpUpload {
    param(
        [string]$ImagePath,
        [string]$Filename,
        [string]$ProfileName,
        [int]$Workers,
        [int]$Width,
        [string]$ResponseDirectory
    )

    $started = [DateTimeOffset]::UtcNow
    $responsePath = Join-Path $ResponseDirectory ("response_{0}_{1}.json" -f $ProfileName, $Filename)
    $uri = "$BaseUrl/api/v1/chats/$ChatId/attachments?filename=$Filename"
    $curlArgs = @(
        "-sS", "-o", $responsePath, "-w", "%{http_code}",
        "-X", "POST", $uri,
        "-H", "Authorization: Bearer $Token",
        "-H", "Content-Type: image/png",
        "--data-binary", "@$ImagePath"
    )

    $httpCodeText = & curl.exe @curlArgs
    $curlExit = $LASTEXITCODE
    $finished = [DateTimeOffset]::UtcNow
    $bodyText = ""
    if (Test-Path $responsePath) { $bodyText = Get-Content -Path $responsePath -Raw }

    $parsed = $null
    if ($bodyText) {
        try { $parsed = $bodyText | ConvertFrom-Json } catch { }
    }

    $httpCode = 0
    [void][int]::TryParse(([string]$httpCodeText).Trim(), [ref]$httpCode)
    $created = $null
    if ($parsed -and $parsed.created_at) { $created = Convert-ToDto $parsed.created_at }
    if ($null -eq $created) { $created = $started }

    return [pscustomobject]@{
        profile                  = $ProfileName
        workers                  = $Workers
        image_size_px            = $Width
        filename                 = $Filename
        attachment_id            = $(if ($parsed) { [string]$parsed.id } else { "" })
        http_status              = $httpCode
        curl_exit                = $curlExit
        api_status               = $(if ($parsed) { [string]$parsed.status } else { "upload_error" })
        upload_started_utc       = $started.ToString("o")
        upload_finished_utc      = $finished.ToString("o")
        created_at_utc            = $created.ToUniversalTime().ToString("o")
        upload_request_seconds   = [Math]::Round(($finished - $started).TotalSeconds, 3)
        response_file            = $responsePath
        response_body            = $bodyText
        ready_at_utc             = ""
        processing_started_utc   = ""
        upload_to_ready_seconds  = $null
        queue_lag_seconds        = $null
        final_status             = $(if ($parsed) { [string]$parsed.status } else { "upload_error" })
        error                    = ""
    }
}

function Get-QueueSnapshot {
    $rabbitIds = @(& docker compose ps -q rabbitmq 2>$null | Where-Object { $_ -and $_.ToString().Trim() -ne "" })
    if ($rabbitIds.Count -eq 0) {
        return [pscustomobject]@{ queue_ready = 0; queue_unacked = 0; raw = "rabbitmq_container_not_found" }
    }
    $rawLines = & docker exec $rabbitIds[0] rabbitmqctl list_queues -p / name messages_ready messages_unacknowledged 2>$null
    $raw = ($rawLines | ForEach-Object { $_.ToString() }) -join "`n"
    $ready = 0
    $unacked = 0
    foreach ($line in $rawLines) {
        $lineText = $line.ToString()
        if ($lineText -match '^\s*\S+\s+(\d+)\s+(\d+)\s*$') {
            $ready += [int]$Matches[1]
            $unacked += [int]$Matches[2]
        }
    }
    return [pscustomobject]@{ queue_ready = $ready; queue_unacked = $unacked; raw = $raw }
}

function Get-WorkerRssSnapshot {
    $ids = Get-WorkerContainerIds
    $sumKb = 0
    $maxKb = 0
    $details = @()
    foreach ($id in $ids) {
        $statusLines = & docker exec $id cat /proc/1/status 2>$null
        $vmLine = $statusLines | Where-Object { $_ -match '^VmRSS:' } | Select-Object -First 1
        if ($vmLine -match '^VmRSS:\s+(\d+)\s+kB') {
            $kb = [int64]$Matches[1]
            $sumKb += $kb
            if ($kb -gt $maxKb) { $maxKb = $kb }
            $details += ("{0}:{1}kB" -f $id.Substring(0, [Math]::Min(12, $id.Length)), $kb)
        }
    }
    return [pscustomobject]@{
        worker_count = $ids.Count
        total_worker_rss_mb = [Math]::Round(($sumKb / 1024.0), 2)
        max_single_worker_rss_mb = [Math]::Round(($maxKb / 1024.0), 2)
        worker_rss_details = ($details -join ";")
    }
}

function Add-MetricSample {
    param([string]$ProfileName, [System.Collections.Generic.List[object]]$MetricList)
    $queue = Get-QueueSnapshot
    $rss = Get-WorkerRssSnapshot
    $MetricList.Add([pscustomobject]@{
        timestamp_utc             = [DateTimeOffset]::UtcNow.ToString("o")
        profile                   = $ProfileName
        workers                   = $rss.worker_count
        queue_messages_ready      = $queue.queue_ready
        queue_messages_unacked    = $queue.queue_unacked
        total_worker_rss_mb       = $rss.total_worker_rss_mb
        max_single_worker_rss_mb  = $rss.max_single_worker_rss_mb
        worker_rss_details        = $rss.worker_rss_details
        queue_raw                 = $queue.raw
    })
}

function Get-P95 {
    param([object[]]$Values)
    $numbers = @($Values | Where-Object { $null -ne $_ } | ForEach-Object { [double]$_ } | Sort-Object)
    if ($numbers.Count -eq 0) { return $null }
    $index = [Math]::Max(0, [int][Math]::Ceiling(0.95 * $numbers.Count) - 1)
    return [Math]::Round($numbers[$index], 3)
}

function Get-WorkerLogText {
    param([string]$OutputPath)
    $log = Invoke-ComposeText -ComposeArgs @("logs", "--timestamps", "--no-color", "--since", "2h", "worker")
    Write-Utf8File -Path $OutputPath -Content $log
    return $log
}

function Add-QueueLagFromLogs {
    param([object[]]$Jobs, [string]$LogText)
    foreach ($job in $Jobs) {
        if (-not $job.attachment_id) { continue }
        $escapedId = [regex]::Escape([string]$job.attachment_id)
        $pattern = "(?<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z).*Processing job for attachment $escapedId"
        $match = [regex]::Match($LogText, $pattern)
        if ($match.Success) {
            $rawTimestamp = $match.Groups["ts"].Value
            $rawTimestamp = [regex]::Replace($rawTimestamp, '(\.\d{7})\d+(Z)$', '$1$2')
            $processing = Convert-ToDto $rawTimestamp
            $created = Convert-ToDto $job.created_at_utc
            if ($processing -and $created) {
                $job.processing_started_utc = $processing.ToUniversalTime().ToString("o")
                $lag = ($processing - $created).TotalSeconds
                if ($lag -ge 0) { $job.queue_lag_seconds = [Math]::Round($lag, 3) }
            }
        }
    }
}

function Wait-ForJobs {
    param(
        [System.Collections.Generic.List[object]]$Jobs,
        [string]$ProfileName,
        [int]$Timeout,
        [System.Collections.Generic.List[object]]$MetricList
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($Timeout)
    do {
        $pending = 0
        foreach ($job in $Jobs) {
            if (-not $job.attachment_id) { continue }
            if ($job.final_status -in @("ready", "failed")) { continue }
            $pending++
            try {
                $state = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/v1/attachments/$($job.attachment_id)" `
                    -Headers @{ Authorization = "Bearer $Token" } -TimeoutSec 20
                $job.final_status = [string]$state.status
                if ($state.status -in @("ready", "failed")) {
                    $readyAt = Convert-ToDto $state.updated_at
                    if ($null -eq $readyAt) { $readyAt = [DateTimeOffset]::UtcNow }
                    $createdAt = Convert-ToDto $job.created_at_utc
                    $job.ready_at_utc = $readyAt.ToUniversalTime().ToString("o")
                    if ($createdAt -and $state.status -eq "ready") {
                        $job.upload_to_ready_seconds = [Math]::Round(($readyAt - $createdAt).TotalSeconds, 3)
                    }
                    if ($state.status -eq "failed") { $job.error = [string]$state.error_code }
                    Write-Host ("  {0} -> {1}" -f $job.filename, $state.status)
                }
            }
            catch {
                $job.error = $_.Exception.Message
            }
        }

        Add-MetricSample -ProfileName $ProfileName -MetricList $MetricList
        $pending = @($Jobs | Where-Object { $_.attachment_id -and $_.final_status -notin @("ready", "failed") }).Count
        if ($pending -eq 0) { break }
        if ([DateTimeOffset]::UtcNow -ge $deadline) { break }
        Start-Sleep -Seconds 1
    } while ($true)

    foreach ($job in $Jobs) {
        if ($job.attachment_id -and $job.final_status -notin @("ready", "failed")) {
            $job.final_status = "timeout"
            if (-not $job.error) { $job.error = "Timed out waiting for ready/failed" }
        }
    }
}

function Export-CsvEvidence {
    param([object[]]$Rows, [string]$Path)
    if ($Rows.Count -gt 0) {
        $Rows | Export-Csv -Path $Path -NoTypeInformation -Encoding UTF8
    }
    else {
        "" | Set-Content -Path $Path -Encoding UTF8
    }
}

# --- Project/evidence setup ---
$ProjectRoot = Find-ProjectRoot
$EvidenceRoot = Join-Path $ProjectRoot "evidence"
if (-not (Test-Path $EvidenceRoot)) { New-Item -ItemType Directory -Path $EvidenceRoot | Out-Null }
$RunStamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$RunDir = Join-Path $EvidenceRoot "track1_$RunStamp"
$AssetsDir = Join-Path $RunDir "images"
$ResponsesDir = Join-Path $RunDir "responses"
New-Item -ItemType Directory -Path $RunDir, $AssetsDir, $ResponsesDir -Force | Out-Null

$BaseUrl = $BaseUrl.TrimEnd('/')
$scriptStart = [DateTimeOffset]::UtcNow
$originalWorkerIds = @()
$originalWorkerCount = 1
$allJobs = [System.Collections.Generic.List[object]]::new()
$allMetrics = [System.Collections.Generic.List[object]]::new()
$summaryRows = [System.Collections.Generic.List[object]]::new()
$probeJobs = [System.Collections.Generic.List[object]]::new()
$profileManifest = [System.Collections.Generic.List[object]]::new()
$prefetchValue = "unknown"
$overallResult = "started"

Push-Location $ProjectRoot
try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "Команда docker не найдена." }
    if (-not (Get-Command curl.exe -ErrorAction SilentlyContinue)) { throw "curl.exe не найден. Нужен стандартный curl из Windows." }
    if (-not (Get-Command python -ErrorAction SilentlyContinue)) { throw "python не найден в PATH." }

    $originalWorkerIds = Get-WorkerContainerIds
    if ($originalWorkerIds.Count -gt 0) { $originalWorkerCount = $originalWorkerIds.Count }

    $baselinePs = Invoke-ComposeText -ComposeArgs @("ps")
    Write-Utf8File -Path (Join-Path $RunDir "compose_ps_before.txt") -Content $baselinePs

    # Capture the existing prefetch configuration without changing it.
    $workerSource = ""
    if ($originalWorkerIds.Count -gt 0) {
        $sourceLines = & docker exec $originalWorkerIds[0] sh -c "cat /app/worker.py" 2>$null
        $workerSource = ($sourceLines | ForEach-Object { $_.ToString() }) -join "`n"
    }
    if ($workerSource) {
        $prefetchMatches = [regex]::Matches($workerSource, 'basic_qos\s*\([^\)]*prefetch_count\s*=\s*(\d+)')
        if ($prefetchMatches.Count -gt 0) { $prefetchValue = $prefetchMatches[$prefetchMatches.Count - 1].Groups[1].Value }
        Write-Utf8File -Path (Join-Path $RunDir "worker_prefetch_source.txt") -Content $workerSource
    }

    $meta = [ordered]@{
        started_utc = $scriptStart.ToString("o")
        project_root = $ProjectRoot
        base_url = $BaseUrl
        chat_id = $ChatId
        original_worker_count = $originalWorkerCount
        worker_profiles = $WorkerProfiles
        samples_per_size = $SamplesPerSize
        image_sizes_px = @(512, 1400, 2200)
        detected_prefetch_count = $prefetchValue
        prefetch_changed = $false
        application_files_modified = $false
        notes = @(
            "Prefetch is not changed by this script.",
            "Concurrency is varied by scaling the existing Compose worker service.",
            "Queue lag is calculated from API created_at to the first worker Processing log line.",
            "RSS is read from VmRSS in /proc/1/status inside each worker container.",
            "A 429/503 during the stopped-worker probe is evidence of admission rejection; all-202 means no bound was demonstrated at the tested burst size."
        )
    }
    Write-Utf8File -Path (Join-Path $RunDir "run_metadata.json") -Content ($meta | ConvertTo-Json -Depth 8)

    # Generate PNGs using Python standard library only (Pillow is not required).
    $generatorPath = Join-Path $RunDir "generate_test_pngs.py"
    $generatorCode = @'
import os, struct, zlib, sys

def chunk(kind, data):
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xffffffff)

def write_png(path, width, height):
    compressor = zlib.compressobj(6)
    parts = []
    for y in range(height):
        row = bytearray([0])  # PNG filter: None
        for x in range(width):
            row.extend(( (x * 11 + y * 3) & 255,
                         (x * 5 + y * 13) & 255,
                         (x * 17 + y * 7) & 255 ))
        parts.append(compressor.compress(bytes(row)))
    parts.append(compressor.flush())
    raw = b"".join(parts)
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    with open(path, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")
        f.write(chunk(b"IHDR", ihdr))
        f.write(chunk(b"IDAT", raw))
        f.write(chunk(b"IEND", b""))

out = sys.argv[1]
os.makedirs(out, exist_ok=True)
for size in (512, 1400, 2200):
    path = os.path.join(out, f"image_{size}x{size}.png")
    write_png(path, size, size)
    print(f"{path}\t{os.path.getsize(path)}")
'@
    Write-Utf8File -Path $generatorPath -Content $generatorCode
    $generatorOutput = & python $generatorPath $AssetsDir 2>&1
    if ($LASTEXITCODE -ne 0) { throw "Не удалось создать тестовые PNG:`n$($generatorOutput -join "`n")" }
    Write-Utf8File -Path (Join-Path $RunDir "generated_images.txt") -Content (($generatorOutput | ForEach-Object { $_.ToString() }) -join "`n")

    $imageMap = @{
        512 = (Join-Path $AssetsDir "image_512x512.png")
        1400 = (Join-Path $AssetsDir "image_1400x1400.png")
        2200 = (Join-Path $AssetsDir "image_2200x2200.png")
    }

    foreach ($workers in $WorkerProfiles) {
        if ($workers -lt 1 -or $workers -gt 8) { throw "Допустимые профили worker: от 1 до 8." }
        $profileName = "workers_$workers`_prefetch_$prefetchValue"
        Write-Host "`n=== Профиль $profileName ===" -ForegroundColor Cyan
        Set-WorkerReplicas -Count $workers -LogPath (Join-Path $RunDir "scale_$profileName.txt")
        $psProfile = Invoke-ComposeText -ComposeArgs @("ps")
        Write-Utf8File -Path (Join-Path $RunDir "compose_ps_$profileName.txt") -Content $psProfile

        $profileJobs = [System.Collections.Generic.List[object]]::new()
        $profileMetrics = [System.Collections.Generic.List[object]]::new()
        foreach ($size in @(512, 1400, 2200)) {
            for ($sample = 1; $sample -le $SamplesPerSize; $sample++) {
                $filename = "track1_${workers}w_${size}px_${sample}.png"
                $job = Get-HttpUpload -ImagePath $imageMap[$size] -Filename $filename `
                    -ProfileName $profileName -Workers $workers -Width $size -ResponseDirectory $ResponsesDir
                $profileJobs.Add($job)
                $allJobs.Add($job)
                Write-Host ("  upload {0}px #{1}: HTTP {2}, status {3}" -f $size, $sample, $job.http_status, $job.api_status)
                if ($job.http_status -eq 401) {
                    throw "API вернул HTTP 401. Токен недействителен/истёк или не передан. Ответ сохранён: $($job.response_file). Обнови token через /api/v1/auth/sessions и запусти скрипт заново."
                }
                if ($job.http_status -eq 403) {
                    throw "API вернул HTTP 403. Проверь, что ChatId принадлежит пользователю текущего токена. Ответ сохранён: $($job.response_file)."
                }
                Add-MetricSample -ProfileName $profileName -MetricList $profileMetrics
            }
        }

        Wait-ForJobs -Jobs $profileJobs -ProfileName $profileName -Timeout $TimeoutSeconds -MetricList $profileMetrics
        foreach ($metric in $profileMetrics) { $allMetrics.Add($metric) }

        $logPath = Join-Path $RunDir "worker_logs_$profileName.txt"
        $logText = Get-WorkerLogText -OutputPath $logPath
        Add-QueueLagFromLogs -Jobs $profileJobs.ToArray() -LogText $logText

        $profileAccepted = @($profileJobs | Where-Object { $_.http_status -eq 202 -and $_.attachment_id }).Count
        $profileReady = @($profileJobs | Where-Object { $_.final_status -eq "ready" }).Count
        $profileFailed = @($profileJobs | Where-Object { $_.final_status -eq "failed" -or $_.final_status -eq "timeout" }).Count
        $p95LagAll = Get-P95 -Values @($profileJobs | ForEach-Object { $_.queue_lag_seconds })
        $p95ReadyAll = Get-P95 -Values @($profileJobs | ForEach-Object { $_.upload_to_ready_seconds })
        $createdTimes = @($profileJobs | Where-Object { $_.attachment_id } | ForEach-Object { Convert-ToDto $_.created_at_utc })
        $readyTimes = @($profileJobs | Where-Object { $_.final_status -eq "ready" } | ForEach-Object { Convert-ToDto $_.ready_at_utc })
        $throughput = $null
        if ($createdTimes.Count -gt 0 -and $readyTimes.Count -gt 0) {
            $firstCreated = ($createdTimes | Sort-Object)[0]
            $lastReady = ($readyTimes | Sort-Object)[-1]
            $duration = ($lastReady - $firstCreated).TotalSeconds
            if ($duration -gt 0) { $throughput = [Math]::Round(($profileReady / $duration), 3) }
        }
        $profileMetricRows = $profileMetrics.ToArray()
        $maxQueue = 0
        $maxUnacked = 0
        $maxRss = 0.0
        foreach ($metric in $profileMetricRows) {
            if ($metric.queue_messages_ready -gt $maxQueue) { $maxQueue = $metric.queue_messages_ready }
            if ($metric.queue_messages_unacked -gt $maxUnacked) { $maxUnacked = $metric.queue_messages_unacked }
            if ($metric.total_worker_rss_mb -gt $maxRss) { $maxRss = $metric.total_worker_rss_mb }
        }

        $summaryRows.Add([pscustomobject]@{
            profile = $profileName; workers = $workers; prefetch_count = $prefetchValue; image_size_px = "ALL"
            samples = $profileJobs.Count; accepted_202 = $profileAccepted; ready = $profileReady; failed_or_timeout = $profileFailed
            throughput_jobs_per_sec = $throughput; p95_queue_lag_sec = $p95LagAll; p95_upload_to_ready_sec = $p95ReadyAll
            max_queue_ready = $maxQueue; max_queue_unacked = $maxUnacked; max_total_worker_rss_mb = $maxRss
        })

        foreach ($size in @(512, 1400, 2200)) {
            $sizeJobs = @($profileJobs | Where-Object { $_.image_size_px -eq $size })
            $sizeReady = @($sizeJobs | Where-Object { $_.final_status -eq "ready" }).Count
            $sizeCreated = @($sizeJobs | Where-Object { $_.attachment_id } | ForEach-Object { Convert-ToDto $_.created_at_utc })
            $sizeReadyTimes = @($sizeJobs | Where-Object { $_.final_status -eq "ready" } | ForEach-Object { Convert-ToDto $_.ready_at_utc })
            $sizeThroughput = ""
            if ($sizeCreated.Count -gt 0 -and $sizeReadyTimes.Count -gt 0) {
                $sizeFirst = ($sizeCreated | Sort-Object)[0]
                $sizeLast = ($sizeReadyTimes | Sort-Object)[-1]
                $sizeDuration = ($sizeLast - $sizeFirst).TotalSeconds
                if ($sizeDuration -gt 0) { $sizeThroughput = [Math]::Round(($sizeReady / $sizeDuration), 3) }
            }
            $summaryRows.Add([pscustomobject]@{
                profile = $profileName; workers = $workers; prefetch_count = $prefetchValue; image_size_px = $size
                samples = $sizeJobs.Count; accepted_202 = @($sizeJobs | Where-Object { $_.http_status -eq 202 -and $_.attachment_id }).Count
                ready = $sizeReady; failed_or_timeout = @($sizeJobs | Where-Object { $_.final_status -eq "failed" -or $_.final_status -eq "timeout" }).Count
                throughput_jobs_per_sec = $sizeThroughput; p95_queue_lag_sec = (Get-P95 -Values @($sizeJobs | ForEach-Object { $_.queue_lag_seconds }))
                p95_upload_to_ready_sec = (Get-P95 -Values @($sizeJobs | ForEach-Object { $_.upload_to_ready_seconds }))
                max_queue_ready = $maxQueue; max_queue_unacked = $maxUnacked; max_total_worker_rss_mb = $maxRss
            })
        }

        $profileManifest.Add([pscustomobject]@{
            profile = $profileName; worker_replicas = $workers; prefetch_count = $prefetchValue
            accepted = $profileAccepted; ready = $profileReady; failed_or_timeout = $profileFailed
            logs = $logPath; metric_samples = $profileMetrics.Count
        })
        Export-CsvEvidence -Rows $profileJobs.ToArray() -Path (Join-Path $RunDir "jobs_$profileName.csv")
        Export-CsvEvidence -Rows $profileMetrics.ToArray() -Path (Join-Path $RunDir "metrics_$profileName.csv")
    }

    # Backpressure/admission probe + recovery demonstration.
    Write-Host "`n=== Проверка admission/backpressure и восстановления ===" -ForegroundColor Cyan
    $stopText = Invoke-ComposeText -ComposeArgs @("stop", "worker")
    Write-Utf8File -Path (Join-Path $RunDir "workers_stopped_for_probe.txt") -Content $stopText
    Start-Sleep -Seconds 2

    for ($i = 1; $i -le $RecoveryProbeCount; $i++) {
        $filename = "track1_recovery_probe_$i.png"
        $job = Get-HttpUpload -ImagePath $imageMap[512] -Filename $filename `
            -ProfileName "recovery_probe" -Workers 0 -Width 512 -ResponseDirectory $ResponsesDir
        $probeJobs.Add($job)
        $allJobs.Add($job)
        Write-Host ("  admission probe #{0}: HTTP {1}, status {2}" -f $i, $job.http_status, $job.api_status)
        if ($job.http_status -eq 401) {
            throw "API вернул HTTP 401 во время recovery probe. Ответ сохранён: $($job.response_file). Обнови access token."
        }
        if ($job.http_status -eq 403) {
            throw "API вернул HTTP 403 во время recovery probe. Проверь ChatId. Ответ сохранён: $($job.response_file)."
        }
    }

    Start-Sleep -Seconds 5
    $queueBeforeRecovery = Get-QueueSnapshot
    Write-Utf8File -Path (Join-Path $RunDir "queue_while_workers_stopped.txt") -Content $queueBeforeRecovery.raw

    # Restore one worker first to drain the deliberately accumulated backlog.
    Set-WorkerReplicas -Count 1 -LogPath (Join-Path $RunDir "restart_workers_for_recovery.txt")
    $recoveryMetrics = [System.Collections.Generic.List[object]]::new()
    Wait-ForJobs -Jobs $probeJobs -ProfileName "recovery" -Timeout $TimeoutSeconds -MetricList $recoveryMetrics
    foreach ($metric in $recoveryMetrics) { $allMetrics.Add($metric) }
    $recoveryLogPath = Join-Path $RunDir "worker_logs_recovery.txt"
    $recoveryLogText = Get-WorkerLogText -OutputPath $recoveryLogPath
    Add-QueueLagFromLogs -Jobs $probeJobs.ToArray() -LogText $recoveryLogText

    $probeAccepted = @($probeJobs | Where-Object { $_.http_status -eq 202 -and $_.attachment_id }).Count
    $probeRejected = @($probeJobs | Where-Object { $_.http_status -in @(429, 503) }).Count
    $probeReady = @($probeJobs | Where-Object { $_.final_status -eq "ready" }).Count
    $probeFailed = @($probeJobs | Where-Object { $_.final_status -eq "failed" -or $_.final_status -eq "timeout" }).Count
    $backpressureStatus = if ($probeRejected -gt 0) {
        "admission_rejection_observed"
    } elseif ($probeAccepted -eq $RecoveryProbeCount) {
        "not_demonstrated_all_probe_uploads_accepted"
    } else {
        "inconclusive_check_probe_csv"
    }

    $recovery = [ordered]@{
        worker_replicas_during_probe = 0
        probe_requests = $RecoveryProbeCount
        accepted_http_202 = $probeAccepted
        rejected_http_429_or_503 = $probeRejected
        queue_ready_while_workers_stopped = $queueBeforeRecovery.queue_ready
        queue_unacked_while_workers_stopped = $queueBeforeRecovery.queue_unacked
        admission_backpressure_result = $backpressureStatus
        recovered_to_ready = $probeReady
        failed_or_timeout = $probeFailed
        recovery_completed = ($probeReady -eq $probeAccepted -and $probeFailed -eq 0)
        interpretation = $(if ($probeRejected -gt 0) {
            "API rejected some work while workers were stopped; inspect backpressure_probe.csv for exact HTTP codes."
        } elseif ($probeAccepted -eq $RecoveryProbeCount) {
            "All probe uploads were accepted. This does not demonstrate a bounded admission limit; it only demonstrates queued-work recovery."
        } else {
            "Some probe uploads did not produce a normal 202 response; inspect backpressure_probe.csv and response bodies."
        })
    }
    Write-Utf8File -Path (Join-Path $RunDir "recovery_and_backpressure.json") -Content ($recovery | ConvertTo-Json -Depth 8)

    $probeCsv = foreach ($job in $probeJobs) {
        [pscustomobject]@{
            filename = $job.filename; http_status = $job.http_status; api_status = $job.api_status
            attachment_id = $job.attachment_id; final_status = $job.final_status
            queue_lag_seconds = $job.queue_lag_seconds; upload_to_ready_seconds = $job.upload_to_ready_seconds
            response_body = $job.response_body
        }
    }
    Export-CsvEvidence -Rows @($probeCsv) -Path (Join-Path $RunDir "backpressure_probe.csv")

    $finalPs = Invoke-ComposeText -ComposeArgs @("ps")
    Write-Utf8File -Path (Join-Path $RunDir "compose_ps_before_restore.txt") -Content $finalPs
    $overallResult = "completed"
}
catch {
    $overallResult = "failed"
    $errorDetails = @(
        $_.ToString()
        "`nScriptStackTrace: $($_.ScriptStackTrace)"
        "`nPosition: $($_.InvocationInfo.PositionMessage)"
    ) -join "`n"
    Write-Utf8File -Path (Join-Path $RunDir "script_error.txt") -Content $errorDetails
    Write-Host "ОШИБКА: $($_.Exception.Message)" -ForegroundColor Red
}
finally {
    # Always restore the original worker replica count, leaving the stack usable.
    try {
        if ($originalWorkerCount -lt 1) { $originalWorkerCount = 1 }
        $restoreText = Invoke-ComposeText -ComposeArgs @(
            "up", "-d", "--no-deps", "--force-recreate", "--scale", "worker=$originalWorkerCount", "worker"
        )
        Write-Utf8File -Path (Join-Path $RunDir "restore_original_worker_count.txt") -Content $restoreText
    }
    catch {
        Write-Utf8File -Path (Join-Path $RunDir "restore_error.txt") -Content $_.ToString()
        Write-Host "Не удалось автоматически восстановить исходное количество worker. Выполни: docker compose up -d --scale worker=$originalWorkerCount worker" -ForegroundColor Yellow
    }

    try {
        $finalStatus = Invoke-ComposeText -ComposeArgs @("ps")
        Write-Utf8File -Path (Join-Path $RunDir "compose_ps_after.txt") -Content $finalStatus
    } catch { }

    if ($allJobs.Count -gt 0) {
        Export-CsvEvidence -Rows $allJobs.ToArray() -Path (Join-Path $RunDir "all_jobs.csv")
    }
    if ($allMetrics.Count -gt 0) {
        Export-CsvEvidence -Rows $allMetrics.ToArray() -Path (Join-Path $RunDir "metrics_timeseries.csv")
    }
    if ($summaryRows.Count -gt 0) {
        Export-CsvEvidence -Rows $summaryRows.ToArray() -Path (Join-Path $RunDir "summary_by_profile_and_size.csv")
    }
    if ($profileManifest.Count -gt 0) {
        Write-Utf8File -Path (Join-Path $RunDir "profile_manifest.json") -Content ($profileManifest.ToArray() | ConvertTo-Json -Depth 8)
    }
    try { Pop-Location } catch { }
}

Write-Host "`nРезультат: $overallResult" -ForegroundColor $(if ($overallResult -eq "completed") { "Green" } else { "Yellow" })
Write-Host "Доказательства: $RunDir"
Write-Host "Сводка: $(Join-Path $RunDir 'summary_by_profile_and_size.csv')"
Write-Host "Проверка backpressure/recovery: $(Join-Path $RunDir 'recovery_and_backpressure.json')"
Write-Host "Prefetch не менялся; найденное значение: $prefetchValue"

if ($overallResult -ne "completed") {
    throw "Скрипт завершился с ошибкой. См. script_error.txt в $RunDir"
}
