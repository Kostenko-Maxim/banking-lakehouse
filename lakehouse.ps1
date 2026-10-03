# Native Windows PowerShell 5.1 / PowerShell 7 alternative to Makefile.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('bootstrap', 'up', 'generate', 'demo', 'test', 'integration-test',
        'backfill', 'benchmark', 'schema-demo', 'maintenance', 'status', 'down', 'reset')]
    [string]$Command,
    [string]$Date = '2025-01-01',
    [ValidateSet('small', '1m', '10m')][string]$Profile = 'small',
    [ValidateRange(1, 100)][int]$Repeats = 3,
    [string]$Start,
    [string]$End,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Invoke-External {
    param([string]$Program, [string[]]$Arguments, [switch]$Quiet)
    if (-not $Quiet) { Write-Host ("{0} {1}" -f $Program, ($Arguments -join ' ')) }
    if ($DryRun) { return }
    # Docker writes normal build progress to stderr. Check its exit code explicitly.
    $savedPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Program @Arguments
        $nativeExitCode = $LASTEXITCODE
    }
    finally { $ErrorActionPreference = $savedPreference }
    if ($nativeExitCode -ne 0) {
        throw "$Program failed (exit $nativeExitCode). Command: $($Arguments -join ' ')"
    }
}

function Invoke-Compose {
    param([string[]]$Arguments)
    Invoke-External -Program docker -Arguments (@('compose') + $Arguments)
}

function Assert-Date {
    param([string]$Value, [string]$Name)
    $parsed = [datetime]::MinValue
    if (-not [datetime]::TryParseExact($Value, 'yyyy-MM-dd',
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::None, [ref]$parsed)) {
        throw "$Name must be a valid date in yyyy-MM-dd format."
    }
}

Push-Location -LiteralPath $PSScriptRoot
try {
    if ($Command -in @('generate', 'benchmark')) { Assert-Date $Date 'Date' }
    if ($Command -eq 'backfill') {
        Assert-Date $Start 'Start'
        Assert-Date $End 'End'
        if ([datetime]$Start -gt [datetime]$End) { throw 'Start must be before or equal to End.' }
    }
    $pythonCommand = 'python'
    if (Test-Path -LiteralPath '.venv/Scripts/python.exe') {
        $pythonCommand = Join-Path $PSScriptRoot '.venv/Scripts/python.exe'
    }
    if ($Command -notin @('test', 'integration-test') -and -not $DryRun) {
        if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
            throw 'Docker CLI is missing. Install Docker Desktop and open a new terminal.'
        }
        $null = Invoke-External -Program docker -Arguments @('info', '--format', '{{.ServerVersion}}') -Quiet
        if ($Command -ne 'bootstrap' -and -not (Test-Path -LiteralPath '.env')) {
            throw 'Run .\lakehouse.ps1 bootstrap first to create .env and prepare the images.'
        }
    }
    $cli = @('exec', '-T', 'airflow-scheduler', 'python', '-m', 'bank.cli')
    switch ($Command) {
        'bootstrap' {
            if (-not (Test-Path -LiteralPath '.env')) {
                if ($DryRun) { Write-Host 'Copy .env.example to .env (only if missing)' }
                else { Copy-Item -LiteralPath '.env.example' -Destination '.env' }
            }
            Invoke-Compose -Arguments @('config', '--quiet')
            Invoke-Compose -Arguments @('build', 'namenode', 'spark-master')
            Invoke-Compose -Arguments @('build', 'metastore', 'airflow-init')
            Invoke-Compose -Arguments @('pull', 'postgres', 'trino', 'db-init')
        }
        'up' {
            if (-not $DryRun) {
                $images = Invoke-External -Program docker -Arguments @('compose', 'config', '--images') -Quiet
                $localImages = Invoke-External -Program docker -Arguments @('image', 'ls', '--format', '{{.Repository}}:{{.Tag}}') -Quiet
                $missing = @($images | Sort-Object -Unique | Where-Object { $_ -notin $localImages })
                if ($missing.Count -gt 0) {
                    throw "Missing images: $($missing -join ', '). Run .\lakehouse.ps1 bootstrap first."
                }
            }
            Invoke-Compose -Arguments @('up', '-d', '--wait', '--wait-timeout', '600', '--no-build', '--pull', 'never')
        }
        'generate' { Invoke-Compose -Arguments ($cli + @('generate', '--date', $Date, '--profile', $Profile)) }
        'demo' { Invoke-Compose -Arguments ($cli + @('demo')) }
        'test' { Invoke-External -Program $pythonCommand -Arguments @('-m', 'pytest', '-q') }
        'integration-test' { Invoke-External -Program $pythonCommand -Arguments @('scripts/integration.py') }
        'backfill' { Invoke-Compose -Arguments ($cli + @('backfill', '--start', $Start, '--end', $End)) }
        'benchmark' { Invoke-Compose -Arguments ($cli + @('benchmark', '--date', $Date, '--profile', $Profile, '--repeats', "$Repeats")) }
        'schema-demo' { Invoke-Compose -Arguments ($cli + @('schema-demo')) }
        'maintenance' { Invoke-Compose -Arguments ($cli + @('maintenance')) }
        'status' {
            Invoke-Compose -Arguments @('ps')
            Invoke-Compose -Arguments @('stats', '--no-stream')
        }
        'down' { Invoke-Compose -Arguments @('down') }
        'reset' {
            if (-not $DryRun -and (Read-Host 'DELETE ALL banking-lakehouse volumes? Type DELETE') -cne 'DELETE') {
                throw 'Reset cancelled. No volumes were deleted.'
            }
            Invoke-Compose -Arguments @('down', '--volumes')
        }
    }
}
catch {
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}
finally { Pop-Location }
