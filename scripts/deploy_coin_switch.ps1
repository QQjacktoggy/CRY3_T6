# One command from meihan, run inside the cry3_t6 folder on any branch:
# copies main's scripts/t6_coin.sh to the VM and switches the coin. It never
# changes the local branch or working tree, the release pin, or the main
# service. Stops on the first failing step.
#
#   git fetch origin main; cmd /c "git show origin/main:scripts/deploy_coin_switch.ps1 > %TEMP%\deploy_coin_switch.ps1"; powershell -ExecutionPolicy Bypass -File $env:TEMP\deploy_coin_switch.ps1
#   (append ETH or BNB to switch to that coin; default BTC)
param([ValidateSet('BTC', 'ETH', 'BNB')][string]$Coin = 'BTC')
$ErrorActionPreference = 'Stop'

$Vm = @('cry3jack', '--project=project-f7b56371-5bd7-47cc-ad6', '--zone=asia-east1-a', '--tunnel-through-iap')
$Remote = '/home/jack_shih/cry3/scripts/t6_coin.sh'
$Local = Join-Path $env:TEMP 't6_coin.sh'

function Step($name, [scriptblock]$run) {
    Write-Host "== $name"
    & $run
    if ($LASTEXITCODE -ne 0) { throw "$name failed (exit $LASTEXITCODE)" }
}

Step 'git fetch origin main' { git fetch origin main }
# cmd redirection keeps the blob's bytes (LF, UTF-8); PowerShell 5's > would not.
Step 'read main t6_coin.sh' { cmd /c "git show origin/main:scripts/t6_coin.sh > `"$Local`"" }
if ((Get-Item $Local).Length -lt 1000) { throw "t6_coin.sh from origin/main looks wrong: $Local" }
Step 'copy to VM' { gcloud compute scp $Local "$($Vm[0]):/tmp/t6_coin.sh" $Vm[1..3] }
Step "install and switch to $Coin" {
    gcloud compute ssh @Vm --command="cd /tmp && sudo -n -u jack_shih install -m 755 /tmp/t6_coin.sh $Remote && rm -f /tmp/t6_coin.sh && sudo -n -u jack_shih sh -c 'cd /tmp && $Remote use $Coin'"
}
