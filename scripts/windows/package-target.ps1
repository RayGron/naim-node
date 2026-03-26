param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('x64', 'arm64')]
  [string]$TargetArch
)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $PSCommandPath
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..\..')).Path
$buildScript = Join-Path $scriptDir 'build-target.ps1'
$buildDir = Join-Path $repoRoot "build\windows\$TargetArch"
$distDir = Join-Path $repoRoot "dist\windows\$TargetArch"
$packagePath = Join-Path $distDir "comet-node-windows-$TargetArch.zip"
$artifactNames = @(
  'comet-node.exe',
  'comet-controller.exe',
  'comet-hostd.exe',
  'comet-inferctl.exe',
  'comet-workerd.exe',
  'comet-common.lib'
)
$artifactPaths = @()

& $buildScript -TargetArch $TargetArch -BuildType Release
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

foreach ($searchRoot in @($buildDir, (Join-Path $buildDir 'Release'))) {
  if (-not (Test-Path $searchRoot)) {
    continue
  }

  foreach ($artifactName in $artifactNames) {
    $candidate = Join-Path $searchRoot $artifactName
    if ((Test-Path $candidate) -and ($artifactPaths -notcontains $candidate)) {
      $artifactPaths += $candidate
    }
  }
}

if ($artifactPaths.Count -eq 0) {
  throw "No packageable Windows artifacts were found under '$buildDir'."
}

New-Item -ItemType Directory -Force -Path $distDir | Out-Null
Remove-Item $packagePath -Force -ErrorAction SilentlyContinue
Compress-Archive -Path $artifactPaths -DestinationPath $packagePath -Force
Write-Output $packagePath
