#!/usr/bin/env pwsh

param(
  [Parameter(Position=0)] [string] $Command,
  [Parameter(ValueFromRemainingArguments = $true)] [string[]] $Args
)

$ErrorActionPreference = 'Stop'

function Write-Color($Text, $Color='Cyan') {
  Write-Host $Text -ForegroundColor $Color
}

function Show-Usage {
  Write-Host "Usage: .\run.ps1 <command> [options]" -ForegroundColor Yellow
  Write-Host "" 
  Write-Host "Commands:" -ForegroundColor Yellow
  Write-Host "  generate-sql   Generate SQL from a question"
  Write-Host "  execute-sql    Generate SQL and execute it on the database"
  Write-Host "  serve          Start FastAPI server (dev)"
  Write-Host "  help           Show this help"
  Write-Host "" 
  Write-Host "Examples:" -ForegroundColor Yellow
  Write-Host "  .\run.ps1 generate-sql 'CA par client'"
  Write-Host "  .\run.ps1 execute-sql 'CA par client' --db-url 'sqlite+aiosqlite:///test.db'"
  Write-Host "  .\run.ps1 serve"
}

if (-not $Command -or $Command -in @('help','-h','--help','/?')) {
  Show-Usage
  exit 0
}

switch ($Command) {
  'generate-sql' {
    Write-Color "🚀 Generating SQL..." 'Cyan'
    & python -m src.cli generate-sql @Args
    break
  }
  'execute-sql' {
    Write-Color "🗄️ Generating and executing SQL..." 'Cyan'
    & python -m src.cli execute-sql @Args
    break
  }
  'serve' {
    Write-Color "🚀 Starting FastAPI server..." 'Green'
    & uvicorn src.api:app --reload --host 0.0.0.0 --port 8000
    break
  }
  Default {
    Write-Color "⚠️ Unknown command: $Command" 'Red'
    Show-Usage
    exit 1
  }
}
