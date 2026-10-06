# One command from meihan (~\Desktop\cry3_t6): put scripts/t6_coin.sh on the VM
# and switch the coin. Only copies that one file; no release pin or main-service
# restart. Stops on the first failing step.
#
#   powershell -ExecutionPolicy Bypass -File scripts\deploy_coin_switch.ps1        # BTC
#   powershell -ExecutionPolicy Bypass -File scripts\deploy_coin_switch.ps1 ETH
param([ValidateSet('BTC', 'ETH', 'BNB')][string]$Coin = 'BTC')
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

$Vm = @('cry3jack', '--project=project-f7b56371-5bd7-47cc-ad6', '--zone=asia-east1-a', '--tunnel-through-iap')
$Remote = '/home/jack_shih/cry3/scripts/t6_coin.sh'

function Step($name, [scriptblock]$run) {
    Write-Host "== $name"
    & $run
    if ($LASTEXITCODE -ne 0) { throw "$name failed (exit $LASTEXITCODE)" }
}

Step 'git pull' { git pull --ff-only }
if (-not (Test-Path scripts/t6_coin.sh)) { throw 'scripts/t6_coin.sh missing after git pull' }
Step 'copy to VM' { gcloud compute scp scripts/t6_coin.sh "$($Vm[0]):/tmp/t6_coin.sh" $Vm[1..3] }
Step "install and switch to $Coin" {
    gcloud compute ssh @Vm --command="cd /tmp && sudo -n -u jack_shih install -m 755 /tmp/t6_coin.sh $Remote && rm -f /tmp/t6_coin.sh && sudo -n -u jack_shih sh -c 'cd /tmp && $Remote use $Coin'"
}
