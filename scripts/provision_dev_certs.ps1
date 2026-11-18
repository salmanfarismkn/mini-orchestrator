param(
    [string[]]$NodeIds = @("worker-01", "worker-02"),
    [string]$OutputDir = (Join-Path $env:USERPROFILE ".mini-orchestrator\local-certs"),
    [int]$ValidityDays = 30,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Test-NodeId([string]$NodeId) {
    if ($NodeId -notmatch '^[-A-Za-z0-9_.]+$') {
        throw "Invalid node ID '$NodeId'. Use letters, digits, hyphen, underscore, or dot."
    }
}

function Assert-OutputAvailable([string]$Path) {
    if ((Test-Path -LiteralPath $Path) -and -not $Force) {
        throw "Refusing to overwrite '$Path'. Re-run with -Force to replace existing generated credentials."
    }
}

$openssl = Get-Command openssl -ErrorAction SilentlyContinue
if (-not $openssl) {
    throw "OpenSSL is required. Install OpenSSL and ensure it is on PATH."
}

if ($NodeIds.Count -eq 0) {
    throw "At least one node ID is required."
}

foreach ($nodeId in $NodeIds) {
    Test-NodeId $nodeId
}
if (($NodeIds | Select-Object -Unique).Count -ne $NodeIds.Count) {
    throw "Node IDs must be unique."
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
$caCert = Join-Path $OutputDir "ca.pem"
$caKey = Join-Path $OutputDir "ca-key.pem"
$controlPlaneCert = Join-Path $OutputDir "control-plane.pem"
$controlPlaneKey = Join-Path $OutputDir "control-plane-key.pem"

foreach ($path in @($caCert, $caKey, $controlPlaneCert, $controlPlaneKey)) {
    Assert-OutputAvailable $path
}
foreach ($nodeId in $NodeIds) {
    Assert-OutputAvailable (Join-Path $OutputDir "$nodeId.pem")
    Assert-OutputAvailable (Join-Path $OutputDir "$nodeId-key.pem")
    Assert-OutputAvailable (Join-Path $OutputDir "$nodeId.csr")
}

& openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 `
    -keyout $caKey -out $caCert -days $ValidityDays -nodes `
    -subj "/CN=mini-orchestrator-local-development-ca" `
    -addext "basicConstraints=critical,CA:TRUE" `
    -addext "keyUsage=critical,keyCertSign,cRLSign" | Out-Null

function Issue-Certificate {
    param(
        [string]$Name,
        [string]$CertificatePath,
        [string]$PrivateKeyPath,
        [string[]]$Uris
    )

    $csr = Join-Path $OutputDir "$Name.csr"
    $ext = Join-Path $OutputDir "$Name.ext"

    $sanParts = @("DNS:localhost", "IP:127.0.0.1")
    foreach ($uri in $Uris) {
        $sanParts += "URI:$uri"
    }

    & openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 `
        -keyout $PrivateKeyPath -out $csr -nodes `
        -subj "/CN=localhost" | Out-Null

    @(
        "subjectAltName = $($sanParts -join ',')",
        "keyUsage = critical,digitalSignature,keyEncipherment",
        "extendedKeyUsage = serverAuth,clientAuth",
        "basicConstraints = critical,CA:FALSE"
    ) | Set-Content -Encoding ascii $ext

    & openssl x509 -req -in $csr `
        -CA $caCert -CAkey $caKey -CAcreateserial -out $CertificatePath `
        -days $ValidityDays -sha256 -extfile $ext | Out-Null

    Remove-Item -LiteralPath $csr, $ext -Force
}

Issue-Certificate -Name "control-plane" -CertificatePath $controlPlaneCert `
    -PrivateKeyPath $controlPlaneKey -Uris @()

foreach ($nodeId in $NodeIds) {
    Issue-Certificate -Name $nodeId `
        -CertificatePath (Join-Path $OutputDir "$nodeId.pem") `
        -PrivateKeyPath (Join-Path $OutputDir "$nodeId-key.pem") `
        -Uris @("spiffe://mini-orchestrator/node/$nodeId")
}

Write-Host "Development CA: $caCert"
Write-Host "Control-plane certificate: $controlPlaneCert"
foreach ($nodeId in $NodeIds) {
    $cert = Join-Path $OutputDir "$nodeId.pem"
    $key = Join-Path $OutputDir "$nodeId-key.pem"
    Write-Host "Node $nodeId certificate: $cert"
    Write-Host "Node $nodeId private key: $key"
    Write-Host "Node $nodeId identity: spiffe://mini-orchestrator/node/$nodeId"

    & openssl verify -CAfile $caCert $cert
    & openssl x509 -in $cert -noout -text |
        Select-String -Pattern 'URI:spiffe://mini-orchestrator/node/', 'TLS Web Client Authentication', 'DNS:localhost', 'IP Address:127\.0\.0\.1' |
        ForEach-Object { Write-Host "  $($_.Line.Trim())" }
}

Write-Host "CA private key is $caKey; keep it outside node-agent credential directories."
