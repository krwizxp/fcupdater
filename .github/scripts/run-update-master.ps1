$ErrorActionPreference = "Stop"
function Require-NonEmpty([string] $value, [string] $name) {
  if ([string]::IsNullOrWhiteSpace($value)) {
    throw "$name is required."
  }
}
function Parse-Bool([string] $value, [string] $name) {
  if ($value -eq "true") {
    return $true
  }
  if ($value -eq "false") {
    return $false
  }
  throw "$name must be either 'true' or 'false'."
}
$artifactStem = $env:FCUPDATER_ARTIFACT_NAME
if ([string]::IsNullOrWhiteSpace($artifactStem)) {
  $artifactStem = "fcupdater-result"
}
$artifactStem = $artifactStem.Trim()
$artifactStem = $artifactStem -replace '[\\/:*?"<>|]', '-'
$masterPath = $env:FCUPDATER_MASTER_PATH
$sourcesPrefix = $env:FCUPDATER_SOURCES_PREFIX
Require-NonEmpty $masterPath "master_path"
Require-NonEmpty $sourcesPrefix "sources_prefix"
$skipDownload = Parse-Bool $env:FCUPDATER_SKIP_DOWNLOAD "skip_download"
$noChangeLog = Parse-Bool $env:FCUPDATER_NO_CHANGE_LOG "no_change_log"
$fastSave = Parse-Bool $env:FCUPDATER_FAST_SAVE "fast_save"
New-Item -ItemType Directory -Force artifacts | Out-Null
$outputPath = Join-Path $PWD ("artifacts\{0}.xlsx" -f $artifactStem)
$args = @(
  "--master", $masterPath,
  "--sources-dir", ".",
  "--sources-prefix", $sourcesPrefix,
  "--output", $outputPath
)
if ($skipDownload) {
  $args += "--skip-download"
}
if ($noChangeLog) {
  $args += "--no-change-log"
}
if ($fastSave) {
  $args += "--fast-save"
}
& .\target\release\fcupdater.exe @args
"artifact_path=$outputPath" >> $env:GITHUB_OUTPUT
