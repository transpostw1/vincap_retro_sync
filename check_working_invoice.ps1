# Check if the working format invoice has non-zero amounts
$response = Invoke-WebRequest -Uri "http://192.168.1.25:801/InvoiceManager/GetAllInvoicePendingAssignment" -Method POST -Headers @{
    "Accept"          = "application/json, text/plain, */*"
    "Accept-Encoding" = "gzip, deflate" 
    "Cookie"          = "ASP.NET_SessionId=gaiwuhwq5vfufbnkyxds0t0c"
} -Body "" -UseBasicParsing

$obj = $response.Content | ConvertFrom-Json | ConvertFrom-Json
$invoice = $obj.Data | Where-Object { $_.ReferenceNumber -eq "TEST-EXACT-WORKING-002" }

if ($invoice) {
    Write-Host "FOUND WORKING FORMAT INVOICE:" -ForegroundColor Green
    Write-Host "  Reference: $($invoice.ReferenceNumber)" -ForegroundColor Yellow
    Write-Host "  Total: $($invoice.Total)" -ForegroundColor $(if ($invoice.Total -gt 0) { "Green" } else { "Red" })
    Write-Host "  SubTotal: $($invoice.SubTotal)" -ForegroundColor $(if ($invoice.SubTotal -gt 0) { "Green" } else { "Red" })
    Write-Host "  GstTotal: $($invoice.GstTotal)" -ForegroundColor $(if ($invoice.GstTotal -gt 0) { "Green" } else { "Red" })
    Write-Host "  AdditionalCost: $($invoice.AdditionalCost)" -ForegroundColor $(if ($invoice.AdditionalCost -gt 0) { "Green" } else { "Red" })
    
    if ($invoice.Total -gt 0 -or $invoice.GstTotal -gt 0 -or $invoice.AdditionalCost -gt 0) {
        Write-Host "SUCCESS! WORKING FORMAT SHOWS NON-ZERO AMOUNTS!" -ForegroundColor Green
    }
    else {
        Write-Host "STILL ZERO AMOUNTS EVEN WITH WORKING FORMAT!" -ForegroundColor Red
    }
}
else {
    Write-Host "Invoice not found!" -ForegroundColor Red
}