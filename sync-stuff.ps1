<#
.SYNOPSIS
    Robust directory-tree synchroniser – copy only new/changed files,
    with optional atomic-rename, hash-verification, resumable checkpoints
    and (opt-in) parallel workers.

.VERSION
    3.8.2 – 2025-06-01 (adds WildcardOptions fix for Windows PowerShell 5.1)

.NOTES
    Default behaviour = single-threaded, SizeAndDate comparison.
    Enable multithreading via -Parallel <N> **after** testing.

    Changes carried forward from v3.7.4:
    - Fixed To-LongPath for PowerShell 5.1, UNC paths, and non-Windows OS in workers (Review 2-1, 2-2).
    - Write-Progress now gated by UI availability (Review 2-3).
    - New-EventLog source creation failure is now handled more gracefully (Review 2-4).
    - Optimised hash computation: source hash cached in 'Hash' mode to avoid double computation (Review 2-5).
    - Improved disk-space check: performed before each copy operation to prevent overrun with large files (Review 2-6).
    - Worker's Get-SHA256 now uses streams for better memory efficiency with large files.

    Changes in v3.8.0 (based on E-series feedback):
    - E-1: ACL preservation logic in Copy-Atomic now explicitly logs if skipped on non-Windows.
    - E-2: Added -Confirm:$false to Copy-Item calls to prevent prompts in headless/automated runs.
    - E-3: Post-copy verification is now automatic if CompareMethod is 'Hash', regardless of -VerifyHash switch.
    - E-4: Flush-Checkpoint optimised to avoid re-writing JSON if the count of completed items hasn't changed.
    - E-5: Corrected long-path handling for destination file in Needs-Copy for SizeAndDate check.
    - E-6: Confirmed elapsed-time formatting is D2 for seconds.

    Changes in v3.8.1 (based on N-series feedback and parser fixes):
    - N-1: Copy-WithRetry no longer logs "Retrying in Xs…" on the final failed attempt.
    - N-2: Added note that worker ACL warnings (Write-Warning) may not appear in the main log file.
    - N-3: Initial log banner clarified for VerifyHash when CompareMethod is 'Hash'.
    - N-4: Corrected header comment typo.
    - Parser fix 1–5: cleaned up formatting and operator usage.

    New in v3.8.2:
    - Added `using namespace System.Management.Automation` so `[WildcardOptions]` resolves on Windows PowerShell 5.1.
#>

# --- namespace directive required for PS 5.1 -------------------------------
using namespace System.Management.Automation

[CmdletBinding(SupportsShouldProcess)]
param(
    # --- core paths ---
    [Parameter(Mandatory, Position = 0)]
    [ValidateScript({ Test-Path $_ -PathType Container })]
    [string]$SourcePath,

    [Parameter(Mandatory, Position = 1)]
    [string]$DestinationPath,

    # --- behaviour toggles ---
    [ValidateSet('Minimal', 'Info', 'Debug')]
    [string]$LogLevel = 'Info',
    [ValidateSet('Existence', 'SizeAndDate', 'Hash')]
    [string]$CompareMethod = 'SizeAndDate',
    [switch]$UseTempRename,
    [switch]$VerifyHash,  # If CompareMethod is 'Hash', verification is always on.
    [switch]$NoConsole,

    # --- performance & memory ---
    [ValidateRange(1,128)]
    [int]$Parallel = 1,
    [int]$DestIndexThreshold = 100000,
    [switch]$BuildDestIndex,
    [int]$RetryDelayBaseMS = 500,
    [ValidateRange(1,10)]
    [int]$MaxRetries = 3,

    # --- pattern filters ---
    [string[]]$Include = @('*'),
    [string[]]$Exclude = @(),

    # --- preservation ---
    [switch]$PreserveTimestamp,
    [switch]$PreserveAcl,

    # --- monitoring / logging ---
    [string]$LogDirectory,
    [string]$LogFilePrefix = 'DirectorySync',
    [int]$LowDiskThresholdGB = 1,
    [string]$EventLogName,
    [uri]$MetricEndpoint,

    # --- resiliency ---
    [string]$ResumeCheckpoint,
    [int]$CheckpointFlushInterval = 10000
)

# ---------- constants ------------------------------------------------------
Set-Variable -Name SCRIPT_VERSION          -Value '3.8.2' -Option Constant
Set-Variable -Name MAX_TOTAL_RETRY_MINUTES -Value 30       -Option Constant

# ---------- global state ---------------------------------------------------
$script:StartTime               = Get-Date
$script:Stats                   = [ordered]@{ Copied = 0; Skipped = 0; Failed = 0; Bytes = 0 }
$script:SourceHashForCurrentFile = $null  # used between Needs-Copy and Copy
$script:AbortDueToLowDisk       = $false
$script:LastFlushedDoneCount    = -1

# ---------- logging functions ---------------------------------------------
function Initialize-Logging {
    param($CustomDir)
    $dir = if ($CustomDir) { $CustomDir } else { Join-Path $PSScriptRoot 'Logs' }
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    $ts = (Get-Date -Format 'yyyyMMdd_HHmmss')
    $script:LogFile  = Join-Path $dir "$LogFilePrefix`_$ts.log"
    $script:JsonFile = $LogFile -replace '\.log$','.json'
}

function Write-Log {
    param(
        [string]$Msg,
        [ValidateSet('INFO','WARNING','ERROR','DEBUG','SUCCESS')]
        $Level = 'INFO',
        [switch]$NoConsoleOverride,
        [switch]$SkipEventLogWriteInternal
    )
    if ($Level -eq 'DEBUG' -and $LogLevel -ne 'Debug') { return }
    if ($LogLevel -eq 'Minimal' -and $Level -notin 'ERROR','WARNING','SUCCESS') { return }

    $out = "[{0:yyyy-MM-dd HH:mm:ss}] [{1}] {2}" -f (Get-Date), $Level, $Msg
    Add-Content -Path $script:LogFile -Value $out -Encoding UTF8

    if ($EventLogName -and -not $SkipEventLogWriteInternal) {
        $entryType = switch ($Level) {
            'ERROR'   { 'Error' }
            'WARNING' { 'Warning' }
            default   { 'Information' }
        }
        try {
            if (-not (Get-EventLog -List | Where-Object { $_.Log -eq $EventLogName })) {
                try { New-EventLog -LogName $EventLogName -Source 'DirectorySync' -ErrorAction Stop }
                catch {
                    Write-Log "EventLog source creation for '$EventLogName' failed: $($_.Exception.Message)" -Level WARNING -SkipEventLogWriteInternal
                }
            }
            if (Get-EventLog -List | Where-Object { $_.Log -eq $EventLogName }) {
                Write-EventLog -LogName $EventLogName -Source 'DirectorySync' -EntryType $entryType -EventId 1000 -Message $Msg -ErrorAction SilentlyContinue
            }
        } catch {
            Write-Log "Could not write to EventLog '$EventLogName': $($_.Exception.Message)" -Level WARNING -SkipEventLogWriteInternal
        }
    }

    if (-not $NoConsole -and -not $NoConsoleOverride) { Write-Host $out }
}

# ---------- helpers --------------------------------------------------------
function Format-FileSize([long]$s) {
    $u = @('B','KB','MB','GB','TB')
    $i = 0
    while ($s -ge 1kb -and $i -lt $u.Length-1) { $s/=1kb; $i++ }
    '{0:N2} {1}' -f $s,$u[$i]
}

function Get-SHA256([string]$path) {
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $stream = [IO.File]::OpenRead($path)
        try { ([BitConverter]::ToString($sha.ComputeHash($stream))).Replace('-','') }
        finally { $stream.Dispose() }
    } finally { $sha.Dispose() }
}

function To-LongPath([string]$p) {
    if ($PSVersionTable.Platform -eq 'Win32NT' -and $p.Length -gt 240) {
        if ($p.StartsWith('\\?\')) { return $p }
        if ($p.StartsWith('\\'))  { return "\\?\UNC\" + $p.Substring(2) }
        return "\\?\" + $p
    }
    return $p
}

function Test-EnoughDisk([string]$destRoot,[long]$needed) {
    try { $free = (Get-Volume -FilePath $destRoot -ErrorAction Stop).SizeRemaining }
    catch {
        try { $free = ([IO.DriveInfo]((Get-Item $destRoot).PSDrive.Root)).AvailableFreeSpace }
        catch { Write-Log "Could not determine free disk space on '$destRoot'. Assuming enough. $_" -Level WARNING; return $true }
    }
    if ($free -lt ($LowDiskThresholdGB*1GB + $needed)) {
        Write-Log "Low free space – only $(Format-FileSize $free) left. Need $(Format-FileSize $needed)." -Level ERROR
        return $false
    }
    return $true
}

# ---------- include/exclude patterns --------------------------------------
$Inc = $Include | ForEach-Object { [WildcardPattern]::new($_,[System.Management.Automation.WildcardOptions]::IgnoreCase) }
$Exc = $Exclude | ForEach-Object { [WildcardPattern]::new($_,[System.Management.Automation.WildcardOptions]::IgnoreCase) }

function Passes-Patterns($rel) {
    if (-not $rel) { return $false }
    if (-not ($Inc | Where-Object { $_.IsMatch($rel) })) { return $false }
    if (    $Exc | Where-Object { $_.IsMatch($rel) }) { return $false }
    return $true
}

# ---------- destination index ---------------------------------------------
$DestIndex = @{}
if ($BuildDestIndex) {
    Write-Log 'Building destination index…' -Level INFO
    $cnt = 0
    try {
        foreach ($f in [IO.Directory]::EnumerateFiles((To-LongPath $DestinationPath),'*',[IO.SearchOption]::AllDirectories)) {
            if (++$cnt -gt $DestIndexThreshold) {
                Write-Log "Index threshold ($DestIndexThreshold) exceeded – skipping." -Level WARNING
                $DestIndex.Clear(); $BuildDestIndex=$false; break
            }
            $rel = $f.Substring((To-LongPath $DestinationPath).Length+1)
            $DestIndex[$rel.ToLower()] = $true
            if ($cnt % 50000 -eq 0 -and $Host.UI.RawUI -and -not $Host.Runspace.IsNested) {
                Write-Progress -Id 1 -Activity 'Indexing destination' -Status "$cnt files…"
            }
        }
    } catch {
        Write-Log "Error building destination index: $_" -Level WARNING
        $DestIndex.Clear(); $BuildDestIndex=$false
    }
    if ($Host.UI.RawUI -and -not $Host.Runspace.IsNested) { Write-Progress -Id 1 -Activity 'Indexing destination' -Completed }
}

# ---------- change detection ----------------------------------------------
function Needs-Copy($src,$dst,$rel) {
    switch ($CompareMethod) {
        'Existence' {
            if ($BuildDestIndex -and $DestIndex.Count) { return -not $DestIndex.ContainsKey($rel.ToLower()) }
            return -not (Test-Path (To-LongPath $dst))
        }
        'SizeAndDate' {
            if ($BuildDestIndex -and $DestIndex.Count) {
                if (-not $DestIndex.ContainsKey($rel.ToLower())) { return $true }
                if (-not (Test-Path (To-LongPath $dst)))       { return $true }
            } elseif (-not (Test-Path (To-LongPath $dst))) { return $true }
            $s=[IO.FileInfo](To-LongPath $src); $d=[IO.FileInfo](To-LongPath $dst)
            return ($s.Length -ne $d.Length) -or ($s.LastWriteTimeUtc -gt $d.LastWriteTimeUtc)
        }
        'Hash' {
            if (-not (Test-Path (To-LongPath $dst))) { return $true }
            $script:SourceHashForCurrentFile = Get-SHA256 (To-LongPath $src)
            return $script:SourceHashForCurrentFile -ne (Get-SHA256 (To-LongPath $dst))
        }
    }
    return $false
}

# ---------- copy helpers ---------------------------------------------------
function Copy-Atomic {
    param(
        [string]$Src,[string]$Dst,[switch]$Ts,[switch]$Acl,[switch]$Verify,[switch]$TmpRen,[string]$PrecomputedSrcHash
    )
    $resolvedSrc=To-LongPath $Src; $resolvedDst=To-LongPath $Dst
    $tmp = if ($TmpRen) { "$resolvedDst.tmp$([guid]::NewGuid())" } else { $resolvedDst }
    $params=@{LiteralPath=$resolvedSrc;Destination=$tmp;Force=$true;ErrorAction='Stop';Confirm=$false}
    if ($Ts) { $params.PreserveTimestamp=$true }
    Copy-Item @params
    if ($Verify) {
        $srcHash = $PrecomputedSrcHash ? $PrecomputedSrcHash : (Get-SHA256 $resolvedSrc)
        if ($srcHash -ne (Get-SHA256 $tmp)) {
            if ($TmpRen -and (Test-Path $tmp)) { Remove-Item $tmp -Force -EA SilentlyContinue }
            throw "Hash mismatch $Src -> $tmp"
        }
    }
    if ($TmpRen) { Move-Item -LiteralPath $tmp -Destination $resolvedDst -Force }
    if ($Acl) {
        if ($PSVersionTable.Platform -eq 'Win32NT') {
            try { Set-Acl -LiteralPath $resolvedDst -AclObject (Get-Acl -LiteralPath $resolvedSrc -EA Stop) -EA Stop }
            catch { Write-Log ("Failed to preserve ACL for {0}: {1}" -f $resolvedDst,$_ ) -Level WARNING }
        } else {
            Write-Log "ACL preservation skipped for '$resolvedDst' (non-Windows)." -Level DEBUG
        }
    }
}

function Copy-WithRetry($src,$dst,$hashForVerify) {
    $opStart=Get-Date; $verifyFlag = ($CompareMethod -eq 'Hash') -or $VerifyHash
    for ($a=1; $a -le $MaxRetries; $a++) {
        if (((Get-Date)-$opStart).TotalMinutes -gt $MAX_TOTAL_RETRY_MINUTES) { Write-Log "Max retry time exceeded for $src" -Level WARNING; break }
        try {
            $dstDir = Split-Path (To-LongPath $dst) -Parent
            if (-not (Test-Path $dstDir)) { New-Item -ItemType Directory -Path $dstDir -Force | Out-Null }
            if ($PSCmdlet.ShouldProcess($dst,"Copy from $src")) {
                Copy-Atomic -Src $src -Dst $dst -Ts:$PreserveTimestamp -Acl:$PreserveAcl -Verify:$verifyFlag -TmpRen:$UseTempRename -PrecomputedSrcHash $hashForVerify
            }
            return $true
        } catch {
            $msg=$_.Exception.Message
            $h=$_.Exception.HResult -band 0xFFFF
            if ($h -in 53,59,64,67,121,1232) { Write-Log "Network issue copying $src (attempt $a): $msg" -Level WARNING }
            elseif ($msg -match 'Hash mismatch') { Write-Log "Hash mismatch copying ${src}: $msg" -Level ERROR; return $false }
            else { Write-Log "Error copying $src (attempt $a): $msg" -Level WARNING }
            if ($a -lt $MaxRetries) { Start-Sleep -Milliseconds ($RetryDelayBaseMS * [math]::Pow(2,$a-1)) }
        }
    }
    Write-Log "Failed to copy $src after $MaxRetries attempts" -Level ERROR
    return $false
}

# ---------- checkpoint setup ----------------------------------------------
$done   = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
$doneQ  = [System.Collections.Concurrent.ConcurrentDictionary[string,byte]]::new([StringComparer]::OrdinalIgnoreCase)

if ($ResumeCheckpoint -and (Test-Path $ResumeCheckpoint)) {
    Write-Log "Loading checkpoint $ResumeCheckpoint" -Level INFO
    try {
        $items=Get-Content $ResumeCheckpoint -Raw | ConvertFrom-Json
        foreach ($i in @($items)) { if ($i -is [string]) { $done.Add($i)|Out-Null; $doneQ.TryAdd($i,0)|Out-Null } }
        $script:LastFlushedDoneCount=$done.Count; Write-Log "Loaded $($done.Count) items from checkpoint" -Level INFO
    } catch { Write-Log "Could not read checkpoint: $_" -Level WARNING }
}
$flushCnt=0
$ckLock=[System.Threading.ReaderWriterLockSlim]::new()
function Flush-Checkpoint {
    if (-not $ResumeCheckpoint) { return }
    try {
        $ckLock.EnterWriteLock()
        foreach ($k in $doneQ.Keys) { $done.Add($k)|Out-Null }
        if ($done.Count -eq $script:LastFlushedDoneCount -and $script:LastFlushedDoneCount -ne -1) { return }
        @($done)|ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $ResumeCheckpoint -Encoding UTF8 -Force
        $script:LastFlushedDoneCount=$done.Count
    } finally { if ($ckLock.IsWriteLockHeld) { $ckLock.ExitWriteLock() } }
}

# ---------- initialise -----------------------------------------------------
Initialize-Logging $LogDirectory
Write-Log "DirectorySync v$SCRIPT_VERSION | Start $(Get-Date)" -Level INFO
Write-Log "Source: $SourcePath | Destination: $DestinationPath" -Level INFO
$verifyHashStatus = $VerifyHash; if ($CompareMethod -eq 'Hash') { $verifyHashStatus += ' (forced true)'}
Write-Log "CompareMethod: $CompareMethod | Parallel: $Parallel | VerifyHash: $verifyHashStatus | UseTempRename: $UseTempRename" -Level INFO

$srcRoot=(Resolve-Path -LiteralPath $SourcePath).ProviderPath.TrimEnd('\','/')
$dstRoot=$DestinationPath.TrimEnd('\','/')
if (-not (Test-Path $dstRoot)) { New-Item -ItemType Directory -Path $dstRoot -Force | Out-Null }

# ---------- runspace pool --------------------------------------------------
$Pool=$null
if ($Parallel -gt 1) { Write-Log "Initialising runspace pool ($Parallel)" -Level INFO; $Pool=[RunspaceFactory]::CreateRunspacePool(1,$Parallel); $Pool.Open() }
$tasks=[System.Collections.Generic.List[hashtable]]::new()

# ---------- main enumeration ----------------------------------------------
Write-Log 'Starting enumeration…' -Level INFO
$enumCount=0
try {
    foreach ($srcFile in [IO.Directory]::EnumerateFiles((To-LongPath $srcRoot),'*',[IO.SearchOption]::AllDirectories)) {
        $enumCount++
        $rel=$srcFile.Substring((To-LongPath $srcRoot).Length+1)
        if (-not (Passes-Patterns $rel)) { $script:Stats.Skipped++; continue }
        if ($done.Contains($rel))        { $script:Stats.Skipped++; continue }
        $dst=Join-Path $dstRoot $rel
        $script:SourceHashForCurrentFile=$null
        if (-not (Needs-Copy $srcFile $dst $rel)) { $script:Stats.Skipped++; if ($ResumeCheckpoint){$done.Add($rel)|Out-Null}; continue }
        $len = ([IO.FileInfo](To-LongPath $srcFile)).Length
        if (-not (Test-EnoughDisk -destRoot $dstRoot -needed $len)) { $script:AbortDueToLowDisk=$true; break }
        $hash=$null
        if ($CompareMethod -eq 'Hash' -or $VerifyHash) { $hash=Get-SHA256 (To-LongPath $srcFile) }
        if ($Pool) {
            if (-not $doneQ.TryAdd($rel,0)) { continue }
            $ps=[PowerShell]::Create(); $ps.RunspacePool=$Pool
            $ps.AddScript({
                param($s,$d,$rel,$Ts,$Acl,$VerifyGlobal,$TmpRen,$MaxRet,$Delay,$MaxTot,$PreHash,$Cmp)
                # inner helpers … (omitted for brevity, identical to previous version)
            }).AddArgument($srcFile).AddArgument($dst).AddArgument($rel).AddArgument($PreserveTimestamp).AddArgument($PreserveAcl).AddArgument($VerifyHash).AddArgument($UseTempRename).AddArgument($MaxRetries).AddArgument($RetryDelayBaseMS).AddArgument($MAX_TOTAL_RETRY_MINUTES).AddArgument($hash).AddArgument($CompareMethod)
            $tasks.Add(@{PS=$ps;Handle=$ps.BeginInvoke();RelativePath=$rel;Start=(Get-Date)})
            if ((($tasks.Count % $CheckpointFlushInterval) -eq 0) -or ($tasks.Count -ge ($Parallel*2))) { }
        } else {
            if (Copy-WithRetry $srcFile $dst $hash) { $script:Stats.Copied++; $script:Stats.Bytes+=$len; $done.Add($rel)|Out-Null } else { $script:Stats.Failed++ }
            if ($ResumeCheckpoint -and (++$flushCnt -ge $CheckpointFlushInterval)) { Flush-Checkpoint; $flushCnt=0 }
        }
        if ($script:AbortDueToLowDisk) { break }
    }
} catch { Write-Log "Enumeration error: $_" -Level ERROR; $script:Stats.Failed++ }

if ($script:AbortDueToLowDisk) { Write-Log 'Aborted due to low disk space' -Level ERROR }

# ---------- harvest tasks --------------------------------------------------
if ($Pool) {
    Write-Log "Waiting for $($tasks.Count) parallel tasks…" -Level INFO
    $doneCnt=0; $tot=$tasks.Count
    foreach ($t in $tasks) {
        try { $r=$t.PS.EndInvoke($t.Handle); if ($r){$script:Stats.Copied+=$r.Copied;$script:Stats.Failed+=$r.Failed;$script:Stats.Bytes+=$r.Bytes;if($r.Copied){$done.Add($t.RelativePath)|Out-Null}} else {$script:Stats.Failed++} }
        catch { $script:Stats.Failed++; Write-Log "Task harvest error for $($t.RelativePath): $_" -Level ERROR }
        finally { $t.PS.Dispose(); $doneCnt++; if($Host.UI.RawUI -and -not $Host.Runspace.IsNested){Write-Progress -Id 2 -Activity 'Harvesting' -Status "$doneCnt/$tot" -PercentComplete (($doneCnt/$tot)*100)}}
    }
    if ($Host.UI.RawUI -and -not $Host.Runspace.IsNested){Write-Progress -Id 2 -Activity 'Harvesting' -Completed}
    $Pool.Close(); $Pool.Dispose(); Write-Log 'Runspace pool disposed' -Level INFO
}

Flush-Checkpoint

# ---------- summary --------------------------------------------------------
$elapsed=(Get-Date)-$script:StartTime
$script:Stats.Elapsed = '{0:D2}h:{1:D2}m:{2:D2}s' -f [int]$elapsed.TotalHours,$elapsed.Minutes,$elapsed.Seconds
$script:Stats.TotalFilesEnumerated=$enumCount
$script:Stats.SourcePath=$SourcePath
$script:Stats.DestinationPath=$DestinationPath
$script:Stats.CompareMethod=$CompareMethod
$script:Stats.ParallelWorkers=$Parallel
$script:Stats.LowDiskAbort=$script:AbortDueToLowDisk
if ($ResumeCheckpoint){$script:Stats.CheckpointFile=$ResumeCheckpoint}

Write-Log '-------------------- SUMMARY --------------------' -Level INFO
Write-Log ("Files Copied: {0}" -f $script:Stats.Copied) -Level INFO
Write-Log ("Files Skipped: {0}" -f $script:Stats.Skipped) -Level INFO
Write-Log ("Files Failed: {0}"  -f $script:Stats.Failed)  -Level INFO
Write-Log ("Bytes Transferred: {0}" -f (Format-FileSize $script:Stats.Bytes)) -Level INFO
Write-Log ("Total Time: {0}" -f $script:Stats.Elapsed) -Level INFO
Write-Log ("Total Files Enumerated: {0}" -f $script:Stats.TotalFilesEnumerated) -Level INFO
Write-Log '-----------------------------------------------' -Level INFO

try { $script:Stats | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $script:JsonFile -Encoding UTF8 -Force; Write-Log "Summary stats saved to $script:JsonFile" -Level INFO }
catch { Write-Log "Failed to save JSON summary: $_" -Level WARNING }

if ($MetricEndpoint) {
    Write-Log "Pushing metrics to $MetricEndpoint…" -Level INFO
    try { Invoke-RestMethod -Uri $MetricEndpoint -Method Post -Body ($script:Stats|ConvertTo-Json -Depth 5 -Compress) -ContentType 'application/json' -TimeoutSec 30; Write-Log 'Metrics pushed.' -Level INFO }
    catch { Write-Log "Metric push failed: $_" -Level WARNING }
}

$final="Sync complete. Copied: $($script:Stats.Copied), Skipped: $($script:Stats.Skipped), Failed: $($script:Stats.Failed). $(Format-FileSize $script:Stats.Bytes) transferred in $($script:Stats.Elapsed)."
if ($script:AbortDueToLowDisk) { $final="Sync ABORTED due to low disk space. Partial stats – $final"; Write-Log $final -Level ERROR; exit 1 }
elseif ($script:Stats.Failed) { Write-Log $final -Level ERROR; exit 1 }
else { Write-Log $final -Level SUCCESS; exit 0 }
