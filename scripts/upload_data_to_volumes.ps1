# ============================================================================
# Script: Upload raw data files to Databricks Unity Catalog Volume
# Author: Diego Mayorga
# Description: Automates data upload to /Volumes/workspace/default/bi_market_raw
# ============================================================================

Write-Host "🚀 Starting data upload to Databricks Unity Catalog Volume..." -ForegroundColor Cyan
Write-Host ""

# Configuration
$VOLUME_PATH = "/Volumes/workspace/default/bi_market_raw"
$LOCAL_DATA_PATH = "data/raw"

# Check if databricks CLI is installed
if (-not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Error: Databricks CLI not found. Please install it first:" -ForegroundColor Red
    Write-Host "   pip install databricks-cli" -ForegroundColor Yellow
    exit 1
}

# Function to upload files with progress
function Upload-Files {
    param (
        [string]$LocalPath,
        [string]$RemotePath,
        [string]$Description
    )
    
    Write-Host "📤 Uploading $Description..." -ForegroundColor Yellow
    
    try {
        if (Test-Path $LocalPath) {
            databricks fs cp $LocalPath $RemotePath --recursive --overwrite
            Write-Host "   ✅ $Description uploaded successfully" -ForegroundColor Green
        } else {
            Write-Host "   ⚠️  Warning: $LocalPath not found, skipping..." -ForegroundColor Yellow
        }
    } catch {
        Write-Host "   ❌ Error uploading $Description : $_" -ForegroundColor Red
    }
    
    Write-Host ""
}

# Upload Master_PDV
Upload-Files `
    -LocalPath "$LOCAL_DATA_PATH/Master_PDV/master_pdv_raw.csv" `
    -RemotePath "$VOLUME_PATH/Master_PDV/master_pdv_raw.csv" `
    -Description "Master_PDV (CSV)"

# Upload Master_Products
Upload-Files `
    -LocalPath "$LOCAL_DATA_PATH/Master_Products/product_master_raw.csv" `
    -RemotePath "$VOLUME_PATH/Master_Products/product_master_raw.csv" `
    -Description "Master_Products (CSV)"

# Upload Price_Audit (24 Excel files)
Upload-Files `
    -LocalPath "$LOCAL_DATA_PATH/Price_Audit" `
    -RemotePath "$VOLUME_PATH/Price_Audit" `
    -Description "Price_Audit (24 XLSX files)"

# Upload Sell-In (2 Excel files)
Upload-Files `
    -LocalPath "$LOCAL_DATA_PATH/Sell-In" `
    -RemotePath "$VOLUME_PATH/Sell-In" `
    -Description "Sell-In (2 XLSX files)"

Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host "✅ Data upload completed!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "📊 Volume location: $VOLUME_PATH" -ForegroundColor White
Write-Host "🔗 Access in Databricks: Catalog > workspace > default > bi_market_raw" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Open Databricks notebook: 01_bronze_ingestion.py" -ForegroundColor White
Write-Host "  2. Run all cells to create Bronze layer tables" -ForegroundColor White
Write-Host ""
