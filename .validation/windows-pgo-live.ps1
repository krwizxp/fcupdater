param(
    [Parameter(Mandatory=$true)][string]$Binary,
    [Parameter(Mandatory=$true)][string]$WorkDir
)
$ErrorActionPreference = "Stop"
$suite = "TLS_RSA_WITH_AES_256_GCM_SHA384"
$policyPath = "HKLM:\SOFTWARE\Policies\Microsoft\Cryptography\Configuration\SSL\00010002"
$localPath = "HKLM:\SYSTEM\CurrentControlSet\Control\Cryptography\Configuration\Local\SSL\00010002"
function Snapshot($path) {
    $key = Get-Item $path
    return @{path=$path;kind=$key.GetValueKind("Functions").ToString();value=$key.GetValue("Functions")}
}
function EffectiveNames {
    $values = Get-TlsCipherSuite
    return @($values | ForEach-Object { foreach($v in $_) { $v.Name } })
}
function IsSame($a,$b) {
    return $a.path -eq $b.path -and $a.kind -eq $b.kind -and ((ConvertTo-Json -InputObject $a.value -Compress) -ceq (ConvertTo-Json -InputObject $b.value -Compress))
}
function ProbeApp {
    $outFile = [System.IO.Path]::GetTempFileName()
    $errFile = [System.IO.Path]::GetTempFileName()
    try {
        $proc = Start-Process -FilePath $Binary -ArgumentList @("--verify") -WorkingDirectory $WorkDir -RedirectStandardOutput $outFile -RedirectStandardError $errFile -Wait -PassThru
        $output = (Get-Content $outFile -Raw) + (Get-Content $errFile -Raw)
        return @{code=$proc.ExitCode;output=$output}
    } finally {
        Remove-Item $outFile,$errFile -Force -ErrorAction SilentlyContinue
    }
}
$policyBefore = Snapshot $policyPath
$localBefore = Snapshot $localPath
$effectiveBefore = @(EffectiveNames)
$changed = $false
$restoreRequired = $false
try {
    $before = ProbeApp
    if ($before.code -eq 0) {
        Write-Output "Original native WinHTTP TLS and application path succeeded."
    } else {
        if (-not $before.output.Contains("Windows error 12175")) {
            throw "Native WinHTTP failed for a reason other than observed TLS 12175: $($before.output)"
        }
        Write-Output "Native WinHTTP 12175 confirmed; testing one bounded TLS 1.2 compatibility suite."
        if (@($policyBefore.value.Split(",") | Where-Object {$_}) -contains $suite) {
            throw "Compatibility suite already exists in original policy."
        }
        $names = @($policyBefore.value.Split(",") | Where-Object {$_})
        $adjusted = ($names + $suite) -join ","
        $restoreRequired = $true
        Set-ItemProperty -Path $policyPath -Name Functions -Value $adjusted -Type String
        Enable-TlsCipherSuite -Name $suite -Position ([uint32]::MaxValue)
        $effectiveAfter = @(EffectiveNames)
        if ($effectiveAfter -notcontains $suite) { throw "Compatibility suite did not become effective." }
        if (@($effectiveBefore | Where-Object {$effectiveAfter -notcontains $_}).Count -ne 0) {
            throw "Existing TLS cipher was removed."
        }
        if (@($effectiveAfter | Where-Object {$effectiveBefore -notcontains $_ -and $_ -ne $suite}).Count -ne 0) {
            throw "Unexpected TLS cipher became enabled."
        }
        $changed = $true
        $live = ProbeApp
        if ($live.code -ne 0) {
            throw "Strict WinHTTP product run failed with bounded suite: $($live.output)"
        }
        Write-Output "Native Windows release updater --verify completed with strict TLS."
    }
} finally {
    if ($restoreRequired) {
        try {
            if ($changed) { Disable-TlsCipherSuite -Name $suite -ErrorAction Stop }
            foreach ($snapshot in @($localBefore,$policyBefore)) {
                $key = [Microsoft.Win32.Registry]::LocalMachine.OpenSubKey($snapshot.path.Substring("HKLM:\".Length), $true)
                if ($null -eq $key) { throw "Unable to open registry key for restoration: $($snapshot.path)" }
                try {
                    $kind = [System.Enum]::Parse([Microsoft.Win32.RegistryValueKind],$snapshot.kind)
                    $key.SetValue("Functions",$snapshot.value,$kind)
                } finally {
                    $key.Dispose()
                }
            }
            $policyRestored = Snapshot $policyPath
            $localRestored = Snapshot $localPath
            $effectiveRestored = @(EffectiveNames)
            if (-not (IsSame $policyRestored $policyBefore) -or -not (IsSame $localRestored $localBefore)) {
                throw "Windows TLS policy registry snapshots did not restore exactly."
            }
            if ((ConvertTo-Json -InputObject $effectiveBefore -Compress) -cne (ConvertTo-Json -InputObject $effectiveRestored -Compress)) {
                throw "Windows TLS effective cipher order did not restore exactly."
            }
            $restored = ProbeApp
            if ($restored.code -eq 0 -or -not $restored.output.Contains("Windows error 12175")) {
                throw "Original WinHTTP 12175 rejection did not return after policy restoration: $($restored.output)"
            }
            Write-Output "Original TLS policy, order and strict WinHTTP rejection verified restored."
        } catch {
            Write-Output ("TLS restoration failed: "+$_.Exception.Message)
            throw
        }
    }
}
