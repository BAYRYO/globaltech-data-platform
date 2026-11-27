# Synthesise each <name>.txt of a directory (UTF-8) to <name>.wav with a System.Speech voice.
# Usage: powershell -File scripts/demo/tts.ps1 -Dir DIR [-Voice NAME] [-Rate -10..10]
param(
  [Parameter(Mandatory = $true)][string]$Dir,
  [string]$Voice = "Microsoft Hortense Desktop",
  [int]$Rate = 0
)
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Speech
$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer
try {
  $synth.SelectVoice($Voice)
  $synth.Rate = $Rate
  Get-ChildItem -Path $Dir -Filter *.txt | Sort-Object Name | ForEach-Object {
    $synth.SetOutputToWaveFile([IO.Path]::ChangeExtension($_.FullName, ".wav"))
    $synth.Speak([IO.File]::ReadAllText($_.FullName, [Text.Encoding]::UTF8))
    $synth.SetOutputToNull()
  }
} finally {
  $synth.Dispose()
}
