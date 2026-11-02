# GoFlow demo for Windows PowerShell 7.
# Start the stack first (make up, then make run-api and make run-worker in
# two terminals, or: make up-app), then run:  ./examples/demo.ps1
#
# Uses Invoke-RestMethod, PowerShell's built-in HTTP client, which turns
# JSON responses into objects automatically.

$api = "http://localhost:8080"

function Show($title, $obj) {
    Write-Host "`n=== $title" -ForegroundColor Cyan
    $obj | ConvertTo-Json -Depth 5
}

# 1. Create a job
$job = Invoke-RestMethod -Method Post -Uri "$api/v1/jobs" -ContentType "application/json" `
    -Body '{"type":"send_email","payload":{"to":"tisa@example.com","subject":"Hello"},"priority":5}'
Show "Created job $($job.id)" $job

# 2. Wait a moment, then look at it again: the worker has run it
Start-Sleep -Milliseconds 500
Show "Job $($job.id) after the worker ran it" (Invoke-RestMethod "$api/v1/jobs/$($job.id)")

# 3. A job that fails twice, then succeeds (retries with exponential backoff)
$flaky = Invoke-RestMethod -Method Post -Uri "$api/v1/jobs" -ContentType "application/json" `
    -Body '{"type":"flaky","payload":{"fail_attempts":2},"max_attempts":5}'
Write-Host "`nCreated flaky job $($flaky.id); waiting about 4 seconds for its retries..."
Start-Sleep -Seconds 4
Show "Flaky job $($flaky.id)" (Invoke-RestMethod "$api/v1/jobs/$($flaky.id)")

# 4. Idempotency: sending the same key twice returns the same job
$body = '{"type":"noop"}'
$a = Invoke-RestMethod -Method Post -Uri "$api/v1/jobs" -ContentType "application/json" -Headers @{ "Idempotency-Key" = "demo-$(Get-Random)" } -Body $body
Write-Host "`n=== Idempotency: first id $($a.id)" -ForegroundColor Cyan

# 5. Validation error (try/catch because PowerShell throws on 4xx responses)
try {
    Invoke-RestMethod -Method Post -Uri "$api/v1/jobs" -ContentType "application/json" -Body '{"type":"Bad Type"}'
} catch {
    Write-Host "`n=== Validation error (expected)" -ForegroundColor Cyan
    $_.ErrorDetails.Message
}

# 6. Overview
Show "Stats" (Invoke-RestMethod "$api/v1/stats")
Show "Workers" (Invoke-RestMethod "$api/v1/workers")
