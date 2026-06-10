#Requires -RunAsAdministrator
<#
  Set up THIS Windows PC to RECEIVE Enviro+ CSVs from the Pi over SSH.

  Installs the official Win32-OpenSSH server from GitHub (this deliberately
  avoids `Add-WindowsCapability -Online -Name OpenSSH.Server`, which can hang
  indefinitely when Windows Update / Features-on-Demand is policy-restricted),
  enables the service, opens the firewall, authorizes the Pi's public key, and
  creates the receive folder.

  1) Put your Pi's public key in $PiPublicKey below
     (on the Pi:  cat ~/.ssh/pc_key.pub).
  2) Run elevated:
     powershell -NoProfile -ExecutionPolicy Bypass -File .\pc_ssh_receiver_setup.ps1
#>

$PiPublicKey = 'ssh-ed25519 AAAA_REPLACE_WITH_YOUR_PI_PUBLIC_KEY pi-to-pc'
$ReceiveDir  = Join-Path $env:USERPROFILE 'enviro_incoming'

# --- 1. Install Win32-OpenSSH from GitHub ---
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$dest = 'C:\Program Files\OpenSSH'
if (-not (Test-Path (Join-Path $dest 'sshd.exe'))) {
    $zip = Join-Path $env:TEMP 'OpenSSH-Win64.zip'
    Invoke-WebRequest 'https://github.com/PowerShell/Win32-OpenSSH/releases/latest/download/OpenSSH-Win64.zip' -OutFile $zip -UseBasicParsing
    $ex = Join-Path $env:TEMP 'OpenSSH-extract'
    if (Test-Path $ex) { Remove-Item $ex -Recurse -Force }
    Expand-Archive $zip $ex -Force
    Move-Item (Get-ChildItem $ex -Directory | Select-Object -First 1).FullName $dest
}
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $dest 'install-sshd.ps1')
Set-Service sshd -StartupType Automatic
Start-Service sshd

# --- 2. Firewall: inbound TCP 22 ---
if (-not (Get-NetFirewallRule -Name 'OpenSSH-Server-In-TCP' -ErrorAction SilentlyContinue)) {
    New-NetFirewallRule -Name 'OpenSSH-Server-In-TCP' -DisplayName 'OpenSSH SSH Server (sshd)' `
        -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22 | Out-Null
}

# --- 3. Authorize the Pi key (admin accounts use administrators_authorized_keys) ---
$isAdmin = [bool]((Get-LocalGroupMember -SID 'S-1-5-32-544' -ErrorAction SilentlyContinue) |
                  Where-Object { $_.Name -ilike "*\$env:USERNAME" })
if ($isAdmin) {
    $akf = Join-Path $env:ProgramData 'ssh\administrators_authorized_keys'
} else {
    $akf = Join-Path $env:USERPROFILE '.ssh\authorized_keys'
}
$dir = Split-Path $akf
if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
if (-not (Test-Path $akf)) { New-Item -ItemType File -Path $akf -Force | Out-Null }
if ((Get-Content $akf -ErrorAction SilentlyContinue) -notcontains $PiPublicKey) {
    Add-Content -Path $akf -Value $PiPublicKey -Encoding ascii
}
if ($isAdmin) {
    # admin key file must be readable only by SYSTEM + Administrators
    icacls $akf /inheritance:r /grant '*S-1-5-18:F' /grant '*S-1-5-32-544:F' | Out-Null
}

# --- 4. Receive folder + summary ---
New-Item -ItemType Directory -Path $ReceiveDir -Force | Out-Null
Write-Host ('sshd: ' + (Get-Service sshd).Status)
Write-Host ('key file: ' + $akf)
Write-Host ('receive dir: ' + $ReceiveDir)
Write-Host ("On the Pi, set ~/.enviro_pc_dest to:  $env:USERNAME@<this-pc-ip>:enviro_incoming")
