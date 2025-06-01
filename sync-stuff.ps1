<#
.SYNOPSIS
    Synchronizes two directory trees, copying only missing or changed files with comprehensive logging.

.DESCRIPTION
    This script performs intelligent directory synchronization by comparing files based on existence, size, 
    and modification time. It provides detailed logging, progress tracking, and robust error handling with 
    retry capabilities.

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

.EXAMPLE
    .\Sync-Directories.ps1 -SourcePath 'E:\ssd\Music\MP3' -DestinationPath 'E:\share\Music'

.EXAMPLE
    .\Sync-Directories.ps1 -SourcePath 'C:\Source' -DestinationPath 'D:\Backup' -WhatIf -LogLevel Debug

.EXAMPLE
    .\Sync-Directories.ps1 -SourcePath 'E:\Music' -DestinationPath 'F:\Music' -CompareMethod Hash -MaxRetries 5

.EXAMPLE
    .\Sync-Directories.ps1 -SourcePath 'E:\Music' -DestinationPath 'F:\Backup' -Include '*.mp3','*.flac' -Exclude 'desktop.ini','*.tmp'

.EXAMPLE
    .\Sync-Directories.ps1 -SourcePath 'E:\Music' -DestinationPath 'F:\Music' -DisableDestinationIndex -Transcript

.NOTES
    Author: Enhanced PowerShell Directory Sync
    Version: 3.2
    Requires: PowerShell 5.1 or later

.LINK
    https://github.com/yourusername/powershell-directory-sync
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

# Script configuration constants (read-only)
Set-Variable -Name 'SCRIPT_VERSION' -Value '3.2' -Option Constant
Set-Variable -Name 'PROGRESS_UPDATE_INTERVAL' -Value 100 -Option Constant
Set-Variable -Name 'PROGRESS_TIME_INTERVAL' -Value 10 -Option Constant
Set-Variable -Name 'LOW_DISK_SPACE_WARNING_GB' -Value 1 -Option Constant
Set-Variable -Name 'RETRY_DELAY_BASE_MS' -Value 500 -Option Constant
Set-Variable -Name 'DEST_INDEX_THRESHOLD' -Value 100000 -Option Constant

# Initialize script-wide variables
$script:TotalCopiedSize = 0
$script:StartTime = Get-Date

#region Logging Functions

function Initialize-Logging {
    param([string]$CustomLogDir)
    
    # Determine log directory
    $logDir = if ($CustomLogDir) { 
        $CustomLogDir 
    } else { 
        Join-Path $PSScriptRoot "Logs" 
    }
    
    # Create log directory if needed
    if (-not (Test-Path $logDir)) {
        [void](New-Item -ItemType Directory -Path $logDir -Force)
    }
    
    # Create timestamped log file
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $logFile = Join-Path $logDir "DirectorySync_$timestamp.log"
    
    return $logFile
}

function Write-Log {
    param(
        [string]$Message,
        [ValidateSet("INFO", "WARNING", "ERROR", "DEBUG", "SUCCESS")]
        [string]$Level = "INFO",
        [switch]$NoConsoleOverride
    )
    
    # Respect log level filtering - SUCCESS allowed in minimal mode
    if ($Level -eq "DEBUG" -and $LogLevel -ne "Debug") { return }
    if ($LogLevel -eq "Minimal" -and $Level -notin @("ERROR", "WARNING", "SUCCESS")) { return }
    
    $timeStamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "[$timeStamp] [$Level] $Message"
    
    # Write to log file
    Add-Content -Path $script:LogFile -Value $logEntry -Encoding UTF8
    
    # Write to console unless suppressed
    if (-not $NoConsole -and -not $NoConsoleOverride) {
        $color = switch ($Level) {
            "ERROR"   { "Red" }
            "WARNING" { "Yellow" }
            "SUCCESS" { "Green" }
            "DEBUG"   { "Magenta" }
            default   { "White" }
        }
        
        # Use ANSI colors in PS 7+ when supported, fallback to Write-Host
        if ($PSVersionTable.PSVersion.Major -ge 7 -and $Host.UI.SupportsVirtualTerminal) {
            $ansiColor = switch ($Level) {
                "ERROR"   { "`e[31m" }   # Red
                "WARNING" { "`e[33m" }   # Yellow  
                "SUCCESS" { "`e[32m" }   # Green
                "DEBUG"   { "`e[35m" }   # Magenta
                default   { "`e[37m" }   # White
            }
            Write-Output "${ansiColor}${logEntry}`e[0m"
        } else {
            Write-Host $logEntry -ForegroundColor $color
        }
    }
}

function Format-FileSize {
    param([long]$Size)
    
    $units = @("bytes", "KB", "MB", "GB", "TB")
    $unitIndex = 0
    $adjustedSize = [double]$Size
    
    while ($adjustedSize -ge 1024 -and $unitIndex -lt ($units.Length - 1)) {
        $adjustedSize /= 1024
        $unitIndex++
    }
    
    if ($unitIndex -eq 0) {
        return "{0:N0} {1}" -f $adjustedSize, $units[$unitIndex]  # No decimals for bytes
    } else {
        return "{0:N2} {1}" -f $adjustedSize, $units[$unitIndex]
    }
}

#endregion

#region File Comparison Functions

function Test-FileNeedsCopy {
    param(
        [System.IO.FileInfo]$SourceFile,
        [string]$DestinationPath,
        [string]$Method,
        [System.IO.FileInfo]$DestinationFile = $null,
        [switch]$SkipExpensiveOperations
    )
    
    # If no destination file provided or doesn't exist, copy is needed
    if (-not $DestinationFile) { 
        return $true 
    }
    
    switch ($Method) {
        'Existence' { 
            return $false  # File exists, don't copy
        }
        'SizeAndDate' {
            return ($SourceFile.Length -ne $DestinationFile.Length) -or 
                   ($SourceFile.LastWriteTimeUtc -gt $DestinationFile.LastWriteTimeUtc)
        }
        'Hash' {
            # Skip expensive hash operations during WhatIf
            if ($SkipExpensiveOperations) {
                Write-Log "Hash comparison skipped during WhatIf for $($SourceFile.Name)" -Level "DEBUG"
                return $true  # Assume it needs copying for WhatIf purposes
            }
            
            # Lazy comparison: check size/date first, then hash only if they match
            $sizeOrDateDifferent = ($SourceFile.Length -ne $DestinationFile.Length) -or 
                                  ($SourceFile.LastWriteTimeUtc -ne $DestinationFile.LastWriteTimeUtc)
            
            if ($sizeOrDateDifferent) {
                Write-Log "Hash comparison skipped for $($SourceFile.Name) due to size/date difference" -Level "DEBUG"
                return $true
            }
            
            # Perform expensive hash comparison only when size/date match
            Write-Log "Performing hash comparison for $($SourceFile.Name)" -Level "DEBUG"
            $sourceHash = Get-FileHash $SourceFile.FullName -Algorithm SHA256
            $destHash = Get-FileHash $DestinationFile.FullName -Algorithm SHA256
            return $sourceHash.Hash -ne $destHash.Hash
        }
    }
}

#endregion

function New-CompiledPatterns {
    param(
        [string[]]$Patterns
    )
    
    $compiledPatterns = @()
    foreach ($pattern in $Patterns) {
        try {
            $compiledPatterns += [WildcardPattern]::new($pattern, [System.Management.Automation.WildcardOptions]::IgnoreCase)
        }
        catch {
            Write-Log "Invalid pattern '$pattern': $($_.Exception.Message)" -Level "WARNING"
        }
    }
    return $compiledPatterns
}

function Test-FileMatchesPatterns {
    param(
        [string]$FilePath,
        [WildcardPattern[]]$IncludePatterns,
        [WildcardPattern[]]$ExcludePatterns
    )
    
    $fileName = Split-Path $FilePath -Leaf
    
    # Check include patterns (must match at least one)
    $matchesInclude = $false
    foreach ($pattern in $IncludePatterns) {
        if ($pattern.IsMatch($fileName)) {
            $matchesInclude = $true
            break
        }
    }
    
    if (-not $matchesInclude) { return $false }
    
    # Check exclude patterns (must not match any)
    foreach ($pattern in $ExcludePatterns) {
        if ($pattern.IsMatch($fileName)) {
            return $false
        }
    }
    
    return $true
}

function Copy-FileWithRetry {
    param(
        [string]$SourcePath,
        [string]$DestinationPath,
        [int]$MaxAttempts = $MaxRetries,
        [switch]$PreserveTimestamp,
        [switch]$PreserveAcl
    )
    
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            # Create destination directory if needed (with long path support)
            $destDir = Split-Path $DestinationPath -Parent
            if (-not (Test-Path $destDir)) {
                # Use long path prefix for paths >260 characters
                $longDestDir = if ($destDir.Length -gt 240) { "\\?\$destDir" } else { $destDir }
                [void](New-Item -ItemType Directory -Path $longDestDir -Force)
            }
            
            # Perform the copy with appropriate options
            if ($PSCmdlet.ShouldProcess($DestinationPath, "Copy file from $SourcePath")) {
                $copyParams = @{
                    Path = $SourcePath
                    Destination = $DestinationPath
                    Force = $true
                }
                
                # Add long path support if needed
                if ($SourcePath.Length -gt 240 -or $DestinationPath.Length -gt 240) {
                    $copyParams.LiteralPath = "\\?\$SourcePath"
                    $copyParams.Remove('Path')
                    $copyParams.Destination = "\\?\$DestinationPath"
                }
                
                # Add preservation options if available (PS 7+)
                if ($PSVersionTable.PSVersion.Major -ge 7) {
                    if ($PreserveTimestamp) { $copyParams.PreserveTimestamp = $true }
                    if ($PreserveAcl) { $copyParams.PreserveAcl = $true }
                }
                
                Copy-Item @copyParams
            }
            
            return $true  # Success
        }
        catch {
            Write-Log "Attempt $attempt failed for $(Split-Path $SourcePath -Leaf): $($_.Exception.Message)" -Level "WARNING"
            
            if ($attempt -lt $MaxAttempts) {
                $delay = $RETRY_DELAY_BASE_MS * [Math]::Pow(2, $attempt - 1)
                Start-Sleep -Milliseconds $delay
            } else {
                Write-Log "All $MaxAttempts attempts failed for $(Split-Path $SourcePath -Leaf)" -Level "ERROR"
                return $false
            }
        }
    }
}

#endregion

#region Main Processing Functions

function Get-DirectoryStats {
    param([string]$Path, [string]$Description)
    
    Write-Log "Enumerating $Description files..." -Level "INFO"
    $files = Get-ChildItem -Path $Path -File -Recurse -ErrorAction SilentlyContinue
    $totalSize = ($files | Measure-Object -Property Length -Sum).Sum
    
    Write-Log "$Description enumeration complete: $($files.Count) files found ($(Format-FileSize $totalSize))" -Level "SUCCESS"
    
    if ($LogLevel -eq "Debug") {
        $extensions = $files | Group-Object Extension | Sort-Object Count -Descending | Select-Object -First 5
        Write-Log "$Description file types (top 5):" -Level "DEBUG"
        foreach ($ext in $extensions) {
            $extSize = ($files | Where-Object Extension -eq $ext.Name | Measure-Object -Property Length -Sum).Sum
            Write-Log "  $($ext.Name): $($ext.Count) files ($(Format-FileSize $extSize))" -Level "DEBUG"
        }
    }
    
    return $files, $totalSize
}

function Sync-Directories {
    # Validate and normalize paths - handle both forward and back slashes
    $SourcePath = $SourcePath.TrimEnd('\', '/')
    $DestinationPath = $DestinationPath.TrimEnd('\', '/')
    
    Write-Log "=== DIRECTORY SYNC STARTED ===" -Level "INFO"
    Write-Log "Script Version: $SCRIPT_VERSION" -Level "INFO"
    Write-Log "Source Path: $SourcePath" -Level "INFO"
    Write-Log "Destination Path: $DestinationPath" -Level "INFO"
    Write-Log "Compare Method: $CompareMethod" -Level "INFO"
    Write-Log "What-If Mode: $WhatIfPreference" -Level "INFO"
    Write-Log "Disable Destination Index: $DisableDestinationIndex" -Level "INFO"
    Write-Log "Max Retries: $MaxRetries" -Level "INFO"
    Write-Log "Include Patterns: $($Include -join ', ')" -Level "DEBUG"
    Write-Log "Exclude Patterns: $($Exclude -join ', ')" -Level "DEBUG"
    Write-Log "Preserve Timestamp: $PreserveTimestamp" -Level "DEBUG"
    Write-Log "Preserve ACL: $PreserveAcl" -Level "DEBUG"
    
    # Pre-compile wildcard patterns for performance
    Write-Log "Compiling include/exclude patterns..." -Level "DEBUG"
    $compiledInclude = New-CompiledPatterns -Patterns $Include
    $compiledExclude = New-CompiledPatterns -Patterns $Exclude
    Write-Log "Compiled $($compiledInclude.Count) include patterns, $($compiledExclude.Count) exclude patterns" -Level "DEBUG"
    
    # Environment information
    Write-Log "PowerShell Version: $($PSVersionTable.PSVersion)" -Level "DEBUG"
    Write-Log "Computer: $env:COMPUTERNAME" -Level "DEBUG"
    Write-Log "User: $env:USERNAME" -Level "DEBUG"
    
    # Create destination if it doesn't exist
    if (-not (Test-Path $DestinationPath)) {
        Write-Log "Creating destination directory: $DestinationPath" -Level "WARNING"
        if ($PSCmdlet.ShouldProcess($DestinationPath, "Create directory")) {
            try {
                [void](New-Item -ItemType Directory -Path $DestinationPath -Force)
                Write-Log "Successfully created destination directory" -Level "SUCCESS"
            }
            catch {
                Write-Log "CRITICAL ERROR: Failed to create destination directory: $($_.Exception.Message)" -Level "ERROR"
                return 1
            }
        }
    }
    
    # Get source files
    $sourceFiles, $totalSourceSize = Get-DirectoryStats -Path $SourcePath -Description "source"
    
    # Get destination files for comparison
    $destinationFiles, $totalDestSize = Get-DirectoryStats -Path $DestinationPath -Description "destination"
    
    # Build destination file index for faster lookups (with memory optimization)
    $destFileIndex = @{}
    $useIndex = -not $DisableDestinationIndex
    
    if (-not $DisableDestinationIndex) {
        # Memory optimization: disable index for very large destination trees
        if ($destinationFiles.Count -gt $DEST_INDEX_THRESHOLD) {
            Write-Log "Large destination tree detected ($($destinationFiles.Count) files > $DEST_INDEX_THRESHOLD). Disabling index to conserve memory." -Level "WARNING"
            $useIndex = $false
        } else {
            Write-Log "Building destination file index..." -Level "DEBUG"
            foreach ($file in $destinationFiles) {
                $relativePath = $file.FullName.Substring($DestinationPath.Length + 1)
                $destFileIndex[$relativePath.ToLower()] = $file
            }
            Write-Log "Destination index built: $($destFileIndex.Count) entries" -Level "DEBUG"
        }
    } else {
        Write-Log "Destination index disabled by parameter" -Level "DEBUG"
    }
    
    # Process files
    $stats = @{
        Copied = 0
        Skipped = 0
        Errors = 0
        ProcessedCount = 0
        LastProgressUpdate = Get-Date
    }
    
    Write-Log "=== PROCESSING FILES ===" -Level "INFO"
    Write-Log "Processing $($sourceFiles.Count) source files with '$CompareMethod' comparison..." -Level "INFO"
    
    foreach ($sourceFile in $sourceFiles) {
        $stats.ProcessedCount++
        
        # Progress reporting (reduce noise in minimal mode)
        $now = Get-Date
        if ($stats.ProcessedCount % $PROGRESS_UPDATE_INTERVAL -eq 0 -or 
            ($now - $stats.LastProgressUpdate).TotalSeconds -ge $PROGRESS_TIME_INTERVAL) {
            $percentComplete = [math]::Round(($stats.ProcessedCount / $sourceFiles.Count) * 100, 1)
            $progressLevel = if ($LogLevel -eq "Minimal") { "SUCCESS" } else { "INFO" }
            Write-Log "Progress: $($stats.ProcessedCount)/$($sourceFiles.Count) files ($percentComplete%)" -Level $progressLevel
            $stats.LastProgressUpdate = $now
        }
        
        # Calculate relative path and destination
        $relativePath = $sourceFile.FullName.Substring($SourcePath.Length + 1)
        $destinationFilePath = Join-Path $DestinationPath $relativePath
        
        # Apply include/exclude pattern filtering using compiled patterns
        if (-not (Test-FileMatchesPatterns -FilePath $relativePath -IncludePatterns $compiledInclude -ExcludePatterns $compiledExclude)) {
            Write-Log "FILTER: $relativePath excluded by pattern matching" -Level "DEBUG"
            $stats.Skipped++
            continue
        }
        
        # Get destination file info using index if available
        $destinationFile = $null
        if ($useIndex) {
            $destinationFile = $destFileIndex[$relativePath.ToLower()]
        } else {
            $destinationFile = Get-Item $destinationFilePath -ErrorAction SilentlyContinue
        }
        
        # Check if file needs to be copied (skip expensive operations during WhatIf)
        if (-not (Test-FileNeedsCopy -SourceFile $sourceFile -DestinationPath $destinationFilePath -Method $CompareMethod -DestinationFile $destinationFile -SkipExpensiveOperations:$WhatIfPreference)) {
            Write-Log "SKIP: $relativePath ($(Format-FileSize $sourceFile.Length)) - no changes needed" -Level "DEBUG"
            $stats.Skipped++
            continue
        }
        
        # Early WhatIf short-circuit - avoid expensive copy retry logic
        if ($WhatIfPreference) {
            Write-Log "WHATIF: Would copy $relativePath ($(Format-FileSize $sourceFile.Length))" -Level "SUCCESS"
            $stats.Copied++
            $script:TotalCopiedSize += $sourceFile.Length
            continue
        }
        
        # Attempt to copy the file (only when not in WhatIf mode)
        $copyStartTime = Get-Date
        $copySuccess = Copy-FileWithRetry -SourcePath $sourceFile.FullName -DestinationPath $destinationFilePath -PreserveTimestamp:$PreserveTimestamp -PreserveAcl:$PreserveAcl
        
        if ($copySuccess) {
            $copyTime = (Get-Date) - $copyStartTime
            $copySpeed = if ($copyTime.TotalSeconds -gt 0) { 
                Format-FileSize ($sourceFile.Length / $copyTime.TotalSeconds) 
            } else { 
                "∞" 
            }
            
            Write-Log "SUCCESS: $relativePath ($(Format-FileSize $sourceFile.Length)) copied in $($copyTime.TotalSeconds.ToString('F2'))s ($copySpeed/s)" -Level "SUCCESS"
            $stats.Copied++
            $script:TotalCopiedSize += $sourceFile.Length
            
            # Verify copy
            if (Test-Path $destinationFilePath) {
                $destFile = Get-Item $destinationFilePath
                if ($destFile.Length -ne $sourceFile.Length) {
                    Write-Log "WARNING: Size mismatch after copy for $relativePath" -Level "WARNING"
                }
            }
        } else {
            $stats.Errors++
        }
    }
    
    # Generate final report
    Write-FinalReport -Stats $stats -TotalSourceSize $totalSourceSize -TotalDestSize $totalDestSize
    
    # Set exit code for script (don't return to avoid pipeline pollution)
    $script:ExitCode = if ($stats.Errors -eq 0) { 0 } else { 1 }
}

function Write-FinalReport {
    param($Stats, $TotalSourceSize, $TotalDestSize)
    
    $endTime = Get-Date
    $totalTime = $endTime - $script:StartTime
    $averageCopySpeed = if ($totalTime.TotalSeconds -gt 0 -and $script:TotalCopiedSize -gt 0) {
        Format-FileSize ($script:TotalCopiedSize / $totalTime.TotalSeconds)
    } else {
        "N/A"
    }
    
    Write-Log "=== SYNC OPERATION COMPLETED ===" -Level "INFO"
    Write-Log "=== FINAL STATISTICS ===" -Level "INFO"
    Write-Log "Duration: $($totalTime.Hours)h $($totalTime.Minutes)m $($totalTime.Seconds)s" -Level "INFO"
    Write-Log "Files copied: $($Stats.Copied)" -Level "SUCCESS"
    Write-Log "Files skipped: $($Stats.Skipped)" -Level "INFO"
    Write-Log "Files with errors: $($Stats.Errors)" -Level "INFO"
    Write-Log "Total data copied: $(Format-FileSize $script:TotalCopiedSize)" -Level "SUCCESS"
    Write-Log "Average copy speed: $averageCopySpeed/second" -Level "INFO"
    
    # Disk space analysis using modern CIM cmdlets
    try {
        $destinationDrive = Split-Path $DestinationPath -Qualifier
        $driveInfo = Get-CimInstance -ClassName Win32_LogicalDisk | Where-Object DeviceID -eq $destinationDrive
        if ($driveInfo) {
            $freeSpaceGB = [math]::Round($driveInfo.FreeSpace / 1GB, 2)
            $totalSpaceGB = [math]::Round($driveInfo.Size / 1GB, 2)
            $percentFree = [math]::Round(($driveInfo.FreeSpace / $driveInfo.Size) * 100, 1)
            
            Write-Log "Disk space on $destinationDrive - Free: ${freeSpaceGB} GB / Total: ${totalSpaceGB} GB ($percentFree% free)" -Level "INFO"
            
            if ($freeSpaceGB -lt $LOW_DISK_SPACE_WARNING_GB) {
                Write-Log "WARNING: Low disk space on destination drive!" -Level "WARNING"
            }
        }
    }
    catch {
        Write-Log "Could not retrieve disk space information: $($_.Exception.Message)" -Level "DEBUG"
    }
    
    # Final status
    if ($Stats.Errors -eq 0) {
        Write-Log "SYNC COMPLETED SUCCESSFULLY!" -Level "SUCCESS"
    } else {
        Write-Log "SYNC COMPLETED WITH ERRORS ($($Stats.Errors) files failed)" -Level "WARNING"
    }
    
    Write-Log "Log file: $script:LogFile" -Level "INFO"
    Write-Log "=== END OF SYNC OPERATION ===" -Level "INFO"
}

#endregion

#region Main Execution

try {
    # Initialize logging
    $script:LogFile = Initialize-Logging -CustomLogDir $LogDirectory
    
    # Start transcript if requested
    if ($Transcript) {
        $transcriptPath = $script:LogFile -replace '\.log

#endregion, '_transcript.txt'
        Start-Transcript -Path $transcriptPath -Append
        Write-Log "Transcript started: $transcriptPath" -Level "INFO"
    }
    
    # Run the sync operation
    $exitCode = Sync-Directories
    
    # Stop transcript if it was started
    if ($Transcript) {
        Stop-Transcript
    }
    
    exit $exitCode
}
catch {
    $errorMsg = "CRITICAL SCRIPT ERROR: $($_.Exception.Message)"
    if ($script:LogFile) {
        Write-Log $errorMsg -Level "ERROR"
    } else {
        Write-Error $errorMsg
    }
    
    # Stop transcript on error if it was started
    if ($Transcript) {
        try { Stop-Transcript } catch { }
    }
    
    exit 2
}

#endregion
