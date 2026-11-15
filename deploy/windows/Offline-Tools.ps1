# Shared by the installer and its isolated acceptance test. Windows PowerShell 5.1.
function Get-OfflineWindowsCredential([PSCredential]$Credential) {
    $current=[Security.Principal.WindowsIdentity]::GetCurrent()
    if(!$Credential){
        Write-Host "Windows Services will use the current account: $($current.Name)"
        $password=Read-Host -Prompt 'Enter the Windows login password for this account (not a PIN or Kafdeck password)' -AsSecureString
        if(!$password -or !$password.Length){throw 'No Windows password was entered. Run the installer again and enter the current Windows account password.'}
        try {$Credential=[PSCredential]::new($current.Name,$password.Copy())} finally {$password.Dispose()}
    }
    if(!$Credential.Password.Length){throw 'The Windows service account password cannot be empty.'}
    try {$sid=([Security.Principal.NTAccount]::new($Credential.UserName)).Translate([Security.Principal.SecurityIdentifier]).Value}
    catch {throw "Cannot resolve Windows account '$($Credential.UserName)'. Use '$($current.Name)'."}
    if($sid -ne $current.User.Value){throw "Windows account mismatch: received '$($Credential.UserName)', expected '$($current.Name)'. Use the installer account; existing state was preserved."}
    return $Credential
}
function New-OfflineSecret {
    $bytes=New-Object byte[] 32;$rng=[Security.Cryptography.RandomNumberGenerator]::Create()
    try {$rng.GetBytes($bytes)} finally {$rng.Dispose()}
    [BitConverter]::ToString($bytes).Replace('-','').ToLowerInvariant()
}
function Save-OfflineSecret([string]$Path,[string]$Value) {
    $secure=ConvertTo-SecureString $Value -AsPlainText -Force
    try {[IO.File]::WriteAllText($Path,($secure|ConvertFrom-SecureString),(New-Object Text.UTF8Encoding($false)))} finally {$secure.Dispose()}
}
function Read-OfflineSecret([string]$Path) {
    $secure=Get-Content -LiteralPath $Path -Raw|ConvertTo-SecureString
    try {[Net.NetworkCredential]::new('',$secure).Password} finally {$secure.Dispose()}
}
function Assert-OfflineChild([string]$Root,[string]$Relative) {
    $base=[IO.Path]::GetFullPath($Root).TrimEnd('\')+'\'
    $path=[IO.Path]::GetFullPath((Join-Path $Root $Relative))
    if([IO.Path]::IsPathRooted($Relative) -or !$path.StartsWith($base,[StringComparison]::OrdinalIgnoreCase)){throw 'Package path escapes its root.'}
    return $path
}
function Test-OfflineMedia([string]$Root) {
    $manifest=Get-Content -LiteralPath (Join-Path $Root 'bundle-manifest.json') -Raw|ConvertFrom-Json
    if($manifest.format -ne 1 -or !$manifest.files.Count){throw 'Invalid offline manifest.'}
    $seen=@{}
    foreach($file in $manifest.files){
        $path=Assert-OfflineChild $Root $file.path
        if($seen.ContainsKey($path) -or $file.sha256 -notmatch '^[0-9a-f]{64}$'){throw 'Invalid manifest entry.'}
        $seen[$path]=$true
        if(!(Test-Path -LiteralPath $path -PathType Leaf) -or (Get-Item -LiteralPath $path).Length -ne $file.bytes -or (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant() -ne $file.sha256){throw "Package verification failed: $($file.path)"}
    }
    foreach($file in Get-ChildItem -LiteralPath $Root -Recurse -File){
        if($file.FullName -ne (Join-Path $Root 'bundle-manifest.json') -and !$seen.ContainsKey($file.FullName)){throw 'An unlisted file is present in the package.'}
    }
    return $manifest
}
function Expand-OfflineArchive([string]$Archive,[string]$Destination) {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip=[IO.Compression.ZipFile]::OpenRead($Archive)
    try {foreach($entry in $zip.Entries){if($entry.FullName){$null=Assert-OfflineChild $Destination $entry.FullName}}} finally {$zip.Dispose()}
    [IO.Compression.ZipFile]::ExtractToDirectory($Archive,$Destination)
}
function ConvertTo-OfflineArgument([string]$Value) {
    # CommandLineToArgvW quoting; no shell, interpolation or command substitution.
    '"'+[regex]::Replace([regex]::Replace($Value,'(\\*)"','$1$1\"'),'(\\+)$','$1$1')+'"'
}
function Invoke-OfflineNative {
    param([string]$File,[string[]]$Arguments,[hashtable]$Environment=@{},[string]$Log,[int]$TimeoutSeconds=180)
    $info=[Diagnostics.ProcessStartInfo]::new($File)
    $info.Arguments=($Arguments|ForEach-Object {ConvertTo-OfflineArgument $_}) -join ' '
    $info.UseShellExecute=$false;$info.CreateNoWindow=$true;$info.RedirectStandardOutput=$true;$info.RedirectStandardError=$true
    foreach($name in $Environment.Keys){$info.EnvironmentVariables[$name]=[string]$Environment[$name]}
    $process=[Diagnostics.Process]::Start($info)
    $stdout=$process.StandardOutput.ReadToEndAsync();$stderr=$process.StandardError.ReadToEndAsync()
    if(!$process.WaitForExit($TimeoutSeconds*1000)){$process.Kill();throw "Timed out: $([IO.Path]::GetFileName($File))"}
    if(!$stdout.Wait(5000) -or !$stderr.Wait(5000)){
        $process.StandardOutput.Close();$process.StandardError.Close()
        throw 'A background child retained redirected output; do not capture a daemon launcher.'
    }
    $output=$stdout.GetAwaiter().GetResult();$errorText=$stderr.GetAwaiter().GetResult()
    if($Log){[IO.File]::WriteAllText($Log,$output+$errorText,(New-Object Text.UTF8Encoding($false)))}
    if($process.ExitCode -ne 0){throw "$([IO.Path]::GetFileName($File)) failed ($($process.ExitCode)). See $Log"}
    return $output
}
function Wait-OfflineUrl([string]$Url,[int]$Seconds=180) {
    $end=(Get-Date).AddSeconds($Seconds)
    do {
        try {return Invoke-RestMethod -Uri $Url -MaximumRedirection 0 -TimeoutSec 5 -ErrorAction Stop}catch {Start-Sleep -Seconds 2}
    }while((Get-Date) -lt $end)
    throw "Service did not become ready: $Url"
}
function Complete-OfflineIdentity([string]$Root,[string]$Origin) {
    $secrets=Join-Path $Root 'secrets'
    $recoveryFile=Join-Path $secrets 'identity-admin-password.dpapi'
    if(!(Test-Path -LiteralPath $recoveryFile)){Save-OfflineSecret $recoveryFile (New-OfflineSecret)}
    $permanent=Read-OfflineSecret $recoveryFile
    function Token([string]$Name,[string]$Password) {
        (Invoke-RestMethod -Uri "$Origin/realms/master/protocol/openid-connect/token" -Method Post -MaximumRedirection 0 -ContentType 'application/x-www-form-urlencoded' -Body @{client_id='admin-cli';grant_type='password';username=$Name;password=$Password} -TimeoutSec 15).access_token
    }
    $token=$null
    try {
        $token=Token 'kafdeck-identity-admin' $permanent
        $null=Invoke-RestMethod -Uri "$Origin/admin/realms/master" -Headers @{Authorization="Bearer $token"} -MaximumRedirection 0 -TimeoutSec 15
    }catch{$token=$null}
    if(!$token){
        $bootstrap=Read-OfflineSecret (Join-Path $secrets 'keycloak-bootstrap.dpapi')
        $token=Token 'kafdeck-bootstrap' $bootstrap
        $headers=@{Authorization="Bearer $token"}
        $users=Invoke-RestMethod -Uri "$Origin/admin/realms/master/users?username=kafdeck-identity-admin&exact=true" -Headers $headers -MaximumRedirection 0 -TimeoutSec 15
        $users=@($users)
        if(!$users.Count){
            $body=@{username='kafdeck-identity-admin';enabled=$true;credentials=@(@{type='password';value=$permanent;temporary=$false})}|ConvertTo-Json -Depth 6
            $null=Invoke-RestMethod -Uri "$Origin/admin/realms/master/users" -Headers $headers -Method Post -MaximumRedirection 0 -ContentType 'application/json' -Body $body -TimeoutSec 15
            $users=Invoke-RestMethod -Uri "$Origin/admin/realms/master/users?username=kafdeck-identity-admin&exact=true" -Headers $headers -MaximumRedirection 0 -TimeoutSec 15
            $users=@($users)
        }
        if($users.Count -ne 1){throw 'Cannot identify the installer-owned identity administrator.'}
        # A retry may find the account before its role assignment. Its exact
        # generated password must prove ownership before granting a role.
        $null=Token 'kafdeck-identity-admin' $permanent
        $role=Invoke-RestMethod -Uri "$Origin/admin/realms/master/roles/admin" -Headers $headers -MaximumRedirection 0 -TimeoutSec 15
        $body=ConvertTo-Json -InputObject @($role) -Depth 8
        $null=Invoke-RestMethod -Uri "$Origin/admin/realms/master/users/$($users[0].id)/role-mappings/realm" -Headers $headers -Method Post -MaximumRedirection 0 -ContentType 'application/json' -Body $body -TimeoutSec 15
        $token=Token 'kafdeck-identity-admin' $permanent
    }
    $headers=@{Authorization="Bearer $token"}
    $null=Invoke-RestMethod -Uri "$Origin/admin/realms/master" -Headers $headers -MaximumRedirection 0 -TimeoutSec 15
    $temporary=Invoke-RestMethod -Uri "$Origin/admin/realms/master/users?username=kafdeck-bootstrap&exact=true" -Headers $headers -MaximumRedirection 0 -TimeoutSec 15
    $temporary=@($temporary)
    foreach($user in $temporary){$null=Invoke-RestMethod -Uri "$Origin/admin/realms/master/users/$($user.id)" -Headers $headers -Method Delete -MaximumRedirection 0 -TimeoutSec 15}
    $path=Join-Path $secrets 'keycloak-bootstrap.dpapi'
    if(Test-Path -LiteralPath $path){Remove-Item -LiteralPath $path}
}
