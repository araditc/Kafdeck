#requires -Version 5.1
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot '../../deploy/windows/Offline-Tools.ps1')
$original=[Globalization.CultureInfo]::CurrentCulture
$checks=0
try {
    foreach($culture in @('en-US','tr-TR','fa-IR')) {
        [Globalization.CultureInfo]::CurrentCulture=[Globalization.CultureInfo]::GetCultureInfo($culture)
        foreach($path in @('C:\Kafdeck','C:\KafdeckInstallerLab-0123456789\installed','D:\SAFE\scripts')) {
            if(!(Test-OfflineInstallPathCharacters $path)){throw 'An ASCII path was incorrectly rejected.'};$checks++
        }
        foreach($path in @('C:\My Folder',('C:\'+[char]0x06a9),('C:\with'+[char]9+'tab'))) {
            if(Test-OfflineInstallPathCharacters $path){throw 'A whitespace/non-ASCII path was accepted.'};$checks++
        }
    }
} finally {[Globalization.CultureInfo]::CurrentCulture=$original}
Write-Host "PASS $checks culture-independent installation path checks."
