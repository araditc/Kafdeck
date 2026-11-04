param([string]$Go = 'go', [string]$OutputDirectory = (Join-Path $PSScriptRoot '../dist'),
    [string[]]$Targets = @('linux_amd64', 'linux_arm64', 'windows_amd64', 'darwin_amd64', 'darwin_arm64'))
$ErrorActionPreference = 'Stop'
$providerRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$targetRoot = [IO.Path]::GetFullPath($OutputDirectory)
$previousLocation = Get-Location
$previousGoos = $env:GOOS; $previousGoarch = $env:GOARCH; $previousCgo = $env:CGO_ENABLED
$previousMaxProcs = $env:GOMAXPROCS
try {
    Set-Location $providerRoot
    $env:GOMAXPROCS = '2'
    & $Go mod verify
    if ($LASTEXITCODE -ne 0) { throw 'Pinned module verification failed.' }
    & $Go test -p=2 ./...
    if ($LASTEXITCODE -ne 0) { throw 'Provider tests failed.' }
    $env:CGO_ENABLED = '0'
    foreach ($target in $Targets) {
        if ($target -notin @('linux_amd64', 'linux_arm64', 'windows_amd64', 'darwin_amd64', 'darwin_arm64')) { throw 'Unsupported build target.' }
        $parts = $target.Split('_'); $env:GOOS = $parts[0]; $env:GOARCH = $parts[1]
        $directory = Join-Path $targetRoot $target
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
        $name = 'terraform-provider-kafdeck_v0.8.0'
        if ($env:GOOS -eq 'windows') { $name += '.exe' }
        & $Go build -p=2 -trimpath -buildvcs=false -ldflags '-s -w -X main.version=0.8.0' -o (Join-Path $directory $name) .
        if ($LASTEXITCODE -ne 0) { throw "Provider build failed for $target" }
        # Package on the build host, preserving Unix executable mode even when
        # cross-compiling on Windows. Never execute a cross-target helper.
        $env:GOOS = $previousGoos; $env:GOARCH = $previousGoarch
        & $Go run -p=2 ./tools/package (Join-Path $directory $name) (Join-Path $targetRoot "terraform-provider-kafdeck_0.8.0_$target.zip")
        if ($LASTEXITCODE -ne 0) { throw "Provider packaging failed for $target" }
    }
    $lines = Get-ChildItem -LiteralPath $targetRoot -Filter '*.zip' | Sort-Object Name | ForEach-Object {
        '{0}  {1}' -f (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant(), $_.Name
    }
    [IO.File]::WriteAllLines((Join-Path $targetRoot 'terraform-provider-kafdeck_0.8.0_SHA256SUMS'), $lines)
    & $Go version -m (Join-Path $targetRoot 'windows_amd64/terraform-provider-kafdeck_v0.8.0.exe') | Set-Content (Join-Path $targetRoot 'build-modules.txt')
} finally {
    $env:GOOS = $previousGoos; $env:GOARCH = $previousGoarch; $env:CGO_ENABLED = $previousCgo
    $env:GOMAXPROCS = $previousMaxProcs
    Set-Location $previousLocation
}
