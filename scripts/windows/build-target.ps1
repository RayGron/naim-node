param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('x64', 'arm64')]
  [string]$TargetArch,

  [Parameter()]
  [ValidateSet('Debug', 'Release', 'RelWithDebInfo', 'MinSizeRel')]
  [string]$BuildType = 'Debug'
)

$ErrorActionPreference = 'Stop'

function Resolve-VcpkgExe {
  param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot
  )

  $candidates = @()

  if ($env:VCPKG_ROOT) {
    $candidates += (Join-Path $env:VCPKG_ROOT 'vcpkg.exe')
  }

  $candidates += @(
    (Join-Path $RepoRoot '.tools\vcpkg\vcpkg.exe'),
    'E:\dev\tools\vcpkg\vcpkg.exe',
    (Join-Path $HOME 'vcpkg\vcpkg.exe')
  )

  foreach ($candidate in $candidates) {
    if ($candidate -and (Test-Path $candidate)) {
      return (Resolve-Path $candidate).Path
    }
  }

  $command = Get-Command vcpkg.exe -ErrorAction SilentlyContinue
  if ($command) {
    return $command.Source
  }

  throw "Unable to find vcpkg.exe. Set VCPKG_ROOT or add vcpkg.exe to PATH on Windows."
}

function Resolve-CMakeGenerator {
  param(
    [Parameter(Mandatory = $true)]
    [string]$CmakeExe
  )

  if ($env:COMET_WINDOWS_CMAKE_GENERATOR) {
    return $env:COMET_WINDOWS_CMAKE_GENERATOR
  }

  $helpText = & $CmakeExe --help 2>$null
  foreach ($candidate in @('Visual Studio 18 2026', 'Visual Studio 17 2022')) {
    if ($helpText -match [regex]::Escape($candidate)) {
      return $candidate
    }
  }

  return 'Visual Studio 17 2022'
}

$scriptDir = Split-Path -Parent $PSCommandPath
$repoRoot = (Resolve-Path (Join-Path $scriptDir '..\..')).Path
$triplet = "$TargetArch-windows"
$buildDir = Join-Path $repoRoot "build\windows\$TargetArch"
$installRoot = Join-Path $repoRoot "vcpkg_installed\$triplet-root"
$prefixPath = Join-Path $installRoot $triplet
$cmakeExe = if ($env:COMET_WINDOWS_CMAKE) { $env:COMET_WINDOWS_CMAKE } else { 'cmake.exe' }
$generator = Resolve-CMakeGenerator -CmakeExe $cmakeExe
$vcpkgExe = Resolve-VcpkgExe -RepoRoot $repoRoot

New-Item -ItemType Directory -Force -Path $buildDir | Out-Null
Remove-Item (Join-Path $buildDir 'CMakeCache.txt') -Force -ErrorAction SilentlyContinue
Remove-Item (Join-Path $buildDir 'CMakeFiles') -Recurse -Force -ErrorAction SilentlyContinue

& $vcpkgExe install `
  "--x-manifest-root=$repoRoot" `
  "--x-install-root=$installRoot" `
  '--x-wait-for-lock' `
  "--triplet=$triplet"

if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

& $cmakeExe `
  '-S' $repoRoot `
  '-B' $buildDir `
  '-G' $generator `
  '-A' $TargetArch `
  '-DCOMET_SKIP_VCPKG_TOOLCHAIN=ON' `
  "-DCMAKE_PREFIX_PATH=$prefixPath" `
  "-DVCPKG_TARGET_TRIPLET=$triplet" `
  "-DVCPKG_INSTALLED_DIR=$installRoot"

if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

& $cmakeExe '--build' $buildDir '--config' $BuildType
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}
