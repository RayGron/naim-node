param(
  [Parameter()]
  [string]$TargetArch = ''
)

$ErrorActionPreference = 'Stop'

function Resolve-DefaultTargetArch {
  $osArchitecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
  switch ($osArchitecture) {
    'X64' { return 'x64' }
    'Arm64' { return 'arm64' }
    default { throw "Unsupported Windows host architecture '$osArchitecture'." }
  }
}

$scriptDir = Split-Path -Parent $PSCommandPath
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..\..')).Path
$buildScript = Join-Path $scriptDir 'build-target.ps1'
$TargetArch = if ($TargetArch) { $TargetArch } else { Resolve-DefaultTargetArch }
if ($TargetArch -notin @('x64', 'arm64')) {
  throw "Unsupported target architecture '$TargetArch'. Expected one of: x64, arm64."
}
$buildDir = Join-Path $repoRoot "build\windows\$TargetArch"
$distDir = Join-Path $repoRoot "dist\windows\$TargetArch"
$packagePath = Join-Path $distDir "naim-node-windows-$TargetArch.zip"
$artifactNames = @(
  'naim-node.exe',
  'naim-controller.exe',
  'naim-hostd.exe',
  'naim-inferctl.exe',
  'naim-workerd.exe',
  'naim-common.lib'
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
