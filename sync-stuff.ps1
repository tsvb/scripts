<#
.SYNOPSIS
    Synchronizes two directory trees, copying only missing or changed files with comprehensive logging.

.DESCRIPTION
    This script performs intelligent directory synchronization by comparing files based on existence, size,
    and modification time. It provides detailed logging, progress tracking, and robust error handling with
    retry capabilities.

    **Version 3.6 – change-log**
    • Fix: transcript path construction no longer causes a parse error.
    • Fix: destination-index keys honor case-sensitive file systems (Linux/macOS) instead of always lower-casing.
    • Fix: include/exclude pattern matching now operates on the *relative path* instead of only the leaf filename.
    • Add: global per-file retry timeout (`MAX_TOTAL_RETRY_MINUTES`).

.PARAMETER SourcePath
    The source directory path to sync from. Must exist.
.PARAMETER DestinationPath
    The destination directory path to sync to. Will be created if it doesn't exist.
.PARAMETER LogLevel
    Controls the verbosity of logging output. Options: Minimal, Info, Debug
.PARAMETER WhatIf
    Shows what operations would be performed without actually executing them.
.PARAMETER NoConsole
    Suppresses console output, logging only to file.
.PARAMETER MaxRetries
    Maximum number of retry attempts for failed file operations (default: 3).
.PARAMETER CompareMethod
    Method for determining if files need to be copied. Options: Existence, SizeAndDate, Hash
.PARAMETER LogDirectory
    Custom directory for log files. Defaults to 'Logs' subdirectory of script location.

.NOTES
    Author: Enhanced PowerShell Directory Sync
    Version: 3.6
    Requires: PowerShell 5.1 or later
    See repository README for full documentation.
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory, Position = 0)]
    [ValidateScript({ Test-Path $_ -PathType Container })]
    [string]$SourcePath,

    [Parameter(Mandatory, Position = 1)]
    [string]$DestinationPath,

    [ValidateSet('Minimal', 'Info', 'Debug')]
    [string]$LogLevel = 'Info',

    [switch]$NoConsole,
    [switch]$Transcript,

    [ValidateRange(1, 10)]
    [int]$MaxRetries = 3,

    [ValidateSet('Existence', 'SizeAndDate', 'Hash')]
    [string]$CompareMethod = 'SizeAndDate',

    [switch]$DisableDestinationIndex,

    [string[]]$Include = @('*'),
    [string[]]$Exclude = @(),

    [switch]$PreserveTimestamp,
    [switch]$PreserveAcl,

    [string]$LogDirectory
)

# ----- constants ----------------------------------------------------------------
Set-Variable -Name 'SCRIPT_VERSION'            -Value '3.6'     -Option Constant
Set-Variable -Name 'PROGRESS_UPDATE_INTERVAL'  -Value 100       -Option Constant
Set-Variable -Name 'PROGRESS_TIME_INTERVAL'    -Value 10        -Option Constant
Set-Variable -Name 'LOW_DISK_SPACE_WARNING_GB' -Value 1         -Option Constant
Set-Variable -Name 'RETRY_DELAY_BASE_MS'       -Value 500       -Option Constant
Set-Variable -Name 'DEST_INDEX_THRESHOLD'      -Value 100000    -Option Constant
Set-Variable -Name 'MAX_TOTAL_RETRY_MINUTES'   -Value 30        -Option Constant

# ----- case-sensitivity detection ----------------------------------------------
$script:IsCaseSensitiveFS = $false
try {
    $t = Join-Path $env:TEMP "PS_CaseTest_$([guid]::NewGuid())"
    New-Item -ItemType File -Path $t -Force | Out-Null
    $script:IsCaseSensitiveFS = -not (Test-Path $t.ToUpper())
    Remove-Item $t -Force -ErrorAction SilentlyContinue
} catch {
    $script:IsCaseSensitiveFS = $false
}

# totals
$script:TotalCopiedSize = 0
$script:StartTime       = Get-Date

# ----- logging helpers (unchanged except for version header omitted here for brevity) -----
function Initialize-Logging {
    param([string]$CustomLogDir)
    $logDir = if ($CustomLogDir) { $CustomLogDir } else { Join-Path $PSScriptRoot 'Logs' }
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
    $ts = Get-Date -Format 'yyyyMMdd_HHmmss'
    Join-Path $logDir "DirectorySync_$ts.log"
}

function Write-Log {
    param(
        [string]$Message,
        [ValidateSet('INFO','WARNING','ERROR','DEBUG','SUCCESS')][string]$Level='INFO',
        [switch]$NoConsoleOverride,
        [System.Management.Automation.ErrorRecord]$ErrorRecord
    )
    if ($Level -eq 'DEBUG' -and $LogLevel -ne 'Debug') {return}
    if ($LogLevel -eq 'Minimal' -and $Level -notin 'ERROR','WARNING','SUCCESS') {return}
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $logEntry = "[$ts] [$Level] $Message"
    if ($ErrorRecord -and $Level -eq 'ERROR' -and $LogLevel -eq 'Debug') {
        $logEntry += "`n    Exception: $($ErrorRecord.Exception.GetType().Name)"`
                  + "`n    Stack trace: $($ErrorRecord.ScriptStackTrace)"
    }
    Add-Content -Path $script:LogFile -Value $logEntry -Encoding UTF8
    if (-not $NoConsole -and -not $NoConsoleOverride) {
        $color = switch ($Level) {'ERROR'='Red';'WARNING'='Yellow';'SUCCESS'='Green';'DEBUG'='Magenta';default='White'}
        if ($PSVersionTable.PSVersion.Major -ge 7 -and $Host.UI.SupportsVirtualTerminal) {
            $ansi = switch ($Level) {'ERROR'='`e[31m';'WARNING'='`e[33m';'SUCCESS'='`e[32m';'DEBUG'='`e[35m';default='`e[37m'}
            Write-Output "$ansi$logEntry`e[0m"
        } else { Write-Host $logEntry -ForegroundColor $color }
    }
}

function Format-FileSize { param([long]$Size)
    $u=@('bytes','KB','MB','GB','TB');$i=0;$n=[double]$Size
    while($n -ge 1024 -and $i -lt $u.Length-1){$n/=1024;$i++}
    if($i -eq 0){"{0:N0} {1}" -f $n,$u[$i]}else{"{0:N2} {1}" -f $n,$u[$i]}
}

# ----- pattern helpers ----------------------------------------------------------
function New-CompiledPatterns {
    param([string[]]$Patterns)
    $out=@();foreach($p in $Patterns){try{$out+=[WildcardPattern]::new($p,[WildcardOptions]::IgnoreCase)}catch{Write-Log "Invalid pattern '$p': $_" -Level WARNING}}
    $out
}

function Test-FileMatchesPatterns {
    param(
        [string]$RelativePath,
        [WildcardPattern[]]$IncludePatterns,
        [WildcardPattern[]]$ExcludePatterns
    )
    # Include (must match one)
    $inMatch=$false;foreach($pat in $IncludePatterns){if($pat.IsMatch($RelativePath)){ $inMatch=$true; break }}
    if(-not $inMatch){return $false}
    # Exclude (must match none)
    foreach($pat in $ExcludePatterns){if($pat.IsMatch($RelativePath)){return $false}}
    return $true
}

# ----- copy with retry & global timeout ----------------------------------------
function Copy-FileWithRetry {
    param(
        [string]$SourcePath,[string]$DestinationPath,
        [int]$MaxAttempts=$MaxRetries,[switch]$PreserveTimestamp,[switch]$PreserveAcl)

    $opStart=Get-Date
    for($attempt=1;$attempt -le $MaxAttempts;$attempt++){
        # global per-file timeout
        if(((Get-Date)-$opStart).TotalMinutes -gt $MAX_TOTAL_RETRY_MINUTES){
            Write-Log "Aborting retries for $(Split-Path $SourcePath -Leaf) – exceeded global timeout" -Level ERROR
            return $false
        }
        try{
            $destDir=Split-Path $DestinationPath -Parent
            if(-not(Test-Path $destDir)){
                $ld= if($destDir.Length -gt 240){"\\?\$destDir"}else{$destDir}
                New-Item -ItemType Directory -Path $ld -Force | Out-Null
            }
            if($PSCmdlet.ShouldProcess($DestinationPath,"Copy $SourcePath")){
                $cp=[ordered]@{Destination=$DestinationPath;Force=$true}
                if($SourcePath.Length -gt 240 -or $DestinationPath.Length -gt 240){
                    $cp.LiteralPath="\\?\$SourcePath"
                    $cp.Destination="\\?\$DestinationPath"
                }else{$cp.Path=$SourcePath}
                if($PSVersionTable.PSVersion.Major -ge 7){ if($PreserveTimestamp){$cp.PreserveTimestamp=$true}; if($PreserveAcl){$cp.PreserveAcl=$true} }
                Copy-Item @cp
            }
            return $true
        }catch{
            Write-Log "Attempt $attempt failed for $(Split-Path $SourcePath -Leaf): $($_.Exception.Message)" -Level WARNING
            if($attempt -lt $MaxAttempts){Start-Sleep -Milliseconds ($RETRY_DELAY_BASE_MS*[math]::Pow(2,$attempt-1))} else {return $false}
        }
    }
}

# ----- main processing (only changed lines shown) ------------------------------
function Sync-Directories {
    $SourcePath      = $SourcePath.TrimEnd('/','\\')
    $DestinationPath = $DestinationPath.TrimEnd('/','\\')
    # … (unchanged banner logging) …

    # build destination index (updated key logic)
    $destFileIndex=@{}
    if(-not $DisableDestinationIndex){
        if($destinationFiles.Count -le $DEST_INDEX_THRESHOLD){
            foreach($file in $destinationFiles){
                $rel=$file.FullName.Substring($DestinationPath.Length+1)
                $key= if($script:IsCaseSensitiveFS){$rel}else{$rel.ToLower()}
                $destFileIndex[$key]=$file
            }
        }else{ Write-Log "Large destination tree – index skipped" -Level WARNING; $useIndex=$false }
    }

    foreach($src in $sourceFiles){
        $rel=$src.FullName.Substring($SourcePath.Length+1)
        # pattern test on *relative path*
        if(-not (Test-FileMatchesPatterns -RelativePath $rel -IncludePatterns $compiledInclude -ExcludePatterns $compiledExclude)){
            $stats.Skipped++; continue }
        # get destination file, honoring case sensitivity
        if($useIndex){
            $key= if($script:IsCaseSensitiveFS){$rel}else{$rel.ToLower()}
            $dest=$destFileIndex[$key]
        } else { $dest = Get-Item (Join-Path $DestinationPath $rel) -ErrorAction SilentlyContinue }
        # … (rest of loop unchanged) …
    }
}

# ----- transcript path fix in main execution block -----------------------------
try{
    $script:LogFile = Initialize-Logging -CustomLogDir $LogDirectory
    if($Transcript){
        $transcriptPath = $script:LogFile -replace '\.log$','_transcript.txt'
        Start-Transcript -Path $transcriptPath -Append
        Write-Log "Transcript started: $transcriptPath" -Level INFO
    }
    $exitCode = Sync-Directories
    if($Transcript){Stop-Transcript}
    exit $exitCode
}catch{
    Write-Log "CRITICAL SCRIPT ERROR: $($_.Exception.Message)" -Level ERROR
    if($Transcript){try{Stop-Transcript}catch{}}
    exit 2
}
