param (
    [string]$ReferenceNumber
)

# --- Headers for login ---
$headers = @{
    "Accept"          = "application/json, text/plain, */*"
    "Accept-Encoding" = "gzip, deflate"
    "Accept-Language" = "en-US,en;q=0.9"
    "Origin"          = "http://192.168.1.25:801"
    "Referer"         = "http://192.168.1.25:801/"
    "User-Agent"      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
}

# --- 1. Login Request ---
$loginResponse = Invoke-WebRequest `
    -Uri "http://192.168.1.25:801/Authentication/AuthenticateUser?userName=akshay&password=retroinv%40123" `
    -Method POST `
    -Headers $headers `
    -Body ""

# --- 2. Login request ---
$loginResponse = Invoke-WebRequest `
    -Uri "http://192.168.1.25:801/Authentication/AuthenticateUser?userName=akshay&password=retroinv%40123" `
    -Method POST `
    -Headers $headers `
    -Body ""

Write-Host "Login Raw Response (first 200 chars):"
$loginResponse.Content.Substring(0, [Math]::Min(200, $loginResponse.Content.Length))

Write-Host "Login Headers:"
$loginResponse.Headers | Out-String

# --- 3. Call Invoice API with cookie ---
$headers.Remove("Accept-Encoding")  # Let PowerShell handle decompression

$response = Invoke-WebRequest `
    -Uri "http://192.168.1.25:801/InvoiceManager/GetAllInvoicePendingAssignment" `
    -Method POST `
    -Headers $headers `
    -Body ""

# --- Debug: Show first 200 chars ---
Write-Host "Raw Response (first 200 chars):"
$response.Content.Substring(0, [Math]::Min(200, $response.Content.Length))

# --- 4. Parse JSON ---
try {
    $json = $response.Content | ConvertFrom-Json
    $invoice = $json.Data | Where-Object { $_.ReferenceNumber -like $ReferenceNumber }
    $invoice | ConvertTo-Json -Depth 10
}
catch {
    Write-Error "Failed to parse JSON response: $_"
}

