$ErrorActionPreference = 'Stop'
$policy = 'HKLM:\SOFTWARE\Policies\Microsoft\Cryptography\Configuration\SSL\00010002'
Write-Output "PowerShell: $($PSVersionTable.PSVersion)"
Write-Output "Windows: $([Environment]::OSVersion.Version)"
Write-Output 'Effective cipher names before activation:'
$before = @(Get-TlsCipherSuite)
Write-Output "Cipher count: $($before.Count)"
$before | ForEach-Object { $_.Name }
$original = Get-ItemPropertyValue -Path $policy -Name Functions
Write-Output "Original policy Functions: $original"
Enable-TlsCipherSuite -Name 'TLS_RSA_WITH_AES_256_GCM_SHA384' -Position 0
Enable-TlsCipherSuite -Name 'TLS_RSA_WITH_AES_128_GCM_SHA256' -Position 0
Write-Output 'Cipher names after supported CNG activation:'
$after = @(Get-TlsCipherSuite)
Write-Output "Cipher count: $($after.Count)"
$after | ForEach-Object { $_.Name }
curl.exe --verbose --tls-max 1.2 --head --max-time 20 https://nfl.opinet.co.kr/
Write-Output "Native TLS after CNG activation: $LASTEXITCODE"

# Modify only this disposable runner's local policy through Microsoft's signed LGPO tool.
# Preserve every existing cipher and append the two TLS 1.2 AES-GCM RSA suites.
$toolRoot = Join-Path $env:RUNNER_TEMP 'optimization-lgpo'
New-Item -ItemType Directory -Path $toolRoot | Out-Null
$zip = Join-Path $toolRoot 'LGPO.zip'
Invoke-WebRequest -UseBasicParsing -Uri 'https://download.microsoft.com/download/8/5/c/85c25433-a1b0-4ffa-9429-7e023e7da8d8/LGPO.zip' -OutFile $zip
Write-Output "LGPO archive SHA256: $((Get-FileHash -Algorithm SHA256 $zip).Hash)"
Expand-Archive -Path $zip -DestinationPath $toolRoot
$lgpo = (Get-ChildItem $toolRoot -Recurse -Filter LGPO.exe | Select-Object -First 1).FullName
$signature = Get-AuthenticodeSignature $lgpo
Write-Output "LGPO signature: $($signature.Status); signer: $($signature.SignerCertificate.Subject)"
if ($signature.Status -ne 'Valid' -or $signature.SignerCertificate.Subject -notmatch 'Microsoft Corporation') {
    throw 'Microsoft LGPO signature verification failed'
}
$suites = [System.Collections.Generic.List[string]]::new()
foreach ($suite in ($original -split ',')) { if ($suite) { $suites.Add($suite) } }
foreach ($suite in @('TLS_RSA_WITH_AES_256_GCM_SHA384','TLS_RSA_WITH_AES_128_GCM_SHA256')) {
    if (-not $suites.Contains($suite)) { $suites.Add($suite) }
}
$updated = ($suites -join ',') + ','
if ($updated.Length -gt 1023) { throw 'Policy cipher list exceeds documented limit' }
$text = "Computer`r`nSoftware\Policies\Microsoft\Cryptography\Configuration\SSL\00010002`r`nFunctions`r`nSZ:$updated`r`n"
$policyFile = Join-Path $toolRoot 'optimization-ciphers.txt'
[System.IO.File]::WriteAllText($policyFile, $text, [System.Text.Encoding]::Unicode)
& $lgpo /t $policyFile
if ($LASTEXITCODE -ne 0) { throw 'LGPO policy import failed' }
gpupdate.exe /target:computer /force /wait:60
if ($LASTEXITCODE -ne 0) { throw 'Computer policy refresh failed' }
Write-Output "Updated policy Functions: $(Get-ItemPropertyValue -Path $policy -Name Functions)"
Write-Output 'Native TLS in a new process after supported policy import; reboot may still be required:'
curl.exe --verbose --tls-max 1.2 --head --max-time 20 https://nfl.opinet.co.kr/
Write-Output "Native TLS after LGPO: $LASTEXITCODE"
if ($LASTEXITCODE -ne 0) { throw 'Certificate-verified native TLS preflight failed after supported policy import' }
exit 0
