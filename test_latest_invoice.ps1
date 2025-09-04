# Test the latest invoice created in Retro
param(
    [string]$ReferencePattern = "FINAL-TEST-0904*"
)

try {
    Write-Host "🔍 Checking for invoices matching pattern: $ReferencePattern"
    
    # Get all invoices and filter for our test pattern
    $response = curl -X POST "http://192.168.1.25:801/InvoiceManager/GetAllInvoicePendingAssignment" -H "Accept: application/json, text/plain, */*" -H "Accept-Encoding: gzip, deflate" -H "Cookie: ASP.NET_SessionId=gaiwuhwq5vfufbnkyxds0t0c" --data "" --compressed --silent
    
    if ($response) {
        $obj = $response | ConvertFrom-Json | ConvertFrom-Json
        $matchingInvoices = $obj.Data | Where-Object { $_.ReferenceNumber -like $ReferencePattern }
        
        if ($matchingInvoices) {
            Write-Host "✅ Found $($matchingInvoices.Count) matching invoice(s):"
            foreach ($invoice in $matchingInvoices) {
                Write-Host "📋 Invoice: $($invoice.ReferenceNumber)"
                Write-Host "   Total: $($invoice.Total)"
                Write-Host "   SubTotal: $($invoice.SubTotal)"  
                Write-Host "   GstTotal: $($invoice.GstTotal)"
                Write-Host "   AdditionalCost: $($invoice.AdditionalCost)"
                Write-Host "   Status: $($invoice.ApprovalStatus)"
                Write-Host "   Timestamp: $($invoice.TimeStamp)"
                Write-Host ""
            }
            
            # Show the latest one in detail
            $latest = $matchingInvoices | Sort-Object TimeStamp -Descending | Select-Object -First 1
            Write-Host "🔥 LATEST INVOICE DETAILS:"
            $latest | ConvertTo-Json -Depth 5
            
        }
        else {
            Write-Host "❌ No invoices found matching pattern: $ReferencePattern"
        }
    }
    else {
        Write-Host "❌ Failed to get response from Retro API"
    }
    
}
catch {
    Write-Host "💥 Error: $($_.Exception.Message)"
}