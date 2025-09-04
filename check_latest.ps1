# Check latest FINAL-TEST invoice
$response = Invoke-WebRequest -Uri "http://192.168.1.25:801/InvoiceManager/GetAllInvoicePendingAssignment" -Method POST -Headers @{
    "Accept"          = "application/json, text/plain, */*"
    "Accept-Encoding" = "gzip, deflate" 
    "Cookie"          = "ASP.NET_SessionId=gaiwuhwq5vfufbnkyxds0t0c"
} -Body "" -UseBasicParsing

$obj = $response.Content | ConvertFrom-Json | ConvertFrom-Json
$testInvoices = $obj.Data | Where-Object { $_.ReferenceNumber -like "FINAL-TEST-0904*" } | Sort-Object TimeStamp -Descending

if ($testInvoices) {
    $latest = $testInvoices[0]
    Write-Host "LATEST TEST INVOICE:"
    Write-Host "  Reference: $($latest.ReferenceNumber)"
    Write-Host "  Timestamp: $($latest.TimeStamp)"
    Write-Host "  Total: $($latest.Total)"
    Write-Host "  SubTotal: $($latest.SubTotal)"
    Write-Host "  GstTotal: $($latest.GstTotal)"
    Write-Host "  AdditionalCost: $($latest.AdditionalCost)"
    
    if ($latest.Total -gt 0) {
        Write-Host "SUCCESS! Non-zero amounts detected!"
    }
    else {
        Write-Host "STILL ZERO AMOUNTS!"
    }
}
else {
    Write-Host "No test invoices found!"
}