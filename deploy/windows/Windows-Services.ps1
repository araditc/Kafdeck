# Windows PowerShell 5.1. WinSW is pinned and shipped with the offline medium.
function Write-OfflineServiceConfig([string]$Root,[ValidateSet('Api','Keycloak')][string]$Component) {
    $name=if($Component -eq 'Api'){'Kafdeck'}else{'Kafdeck-Keycloak'}
    $dependency=if($Component -eq 'Api'){'Kafdeck-Keycloak'}else{'Kafdeck-PostgreSQL'}
    $directory=Join-Path $Root ('services/'+$name)
    New-Item -ItemType Directory -Force $directory|Out-Null
    $exe=Join-Path $directory ($name+'.exe')
    $source=Join-Path $Root 'runtime/winsw.exe'
    if(!(Test-Path -LiteralPath $exe) -or (Get-FileHash -LiteralPath $exe).Hash -ne (Get-FileHash -LiteralPath $source).Hash){Copy-Item -LiteralPath $source -Destination $exe -Force}
    $xml=[xml]'<service />'
    $values=[ordered]@{
        id=$name;name=$name;description=('Kafdeck offline installation: '+$Component)
        executable="$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
        arguments="-NoProfile -NonInteractive -ExecutionPolicy RemoteSigned -File `"$(Join-Path $Root 'scripts/Offline-Run.ps1')`" -Component $Component"
        workingdirectory=$Root;startmode='Automatic';delayedAutoStart='true'
        depend=$dependency;stoptimeout='60 sec';stopparentprocessfirst='false'
        logpath=(Join-Path $Root 'logs');resetfailure='1 day'
    }
    foreach($key in $values.Keys){$node=$xml.CreateElement($key);$node.InnerText=$values[$key];$null=$xml.DocumentElement.AppendChild($node)}
    $failure=$xml.CreateElement('onfailure');$failure.SetAttribute('action','restart');$failure.SetAttribute('delay','30 sec');$null=$xml.DocumentElement.AppendChild($failure)
    $log=$xml.CreateElement('log');$log.SetAttribute('mode','roll-by-size')
    foreach($pair in @(@('sizeThreshold','10240'),@('keepFiles','4'))){$node=$xml.CreateElement($pair[0]);$node.InnerText=$pair[1];$null=$log.AppendChild($node)}
    $null=$xml.DocumentElement.AppendChild($log)
    $xml.Save([IO.Path]::ChangeExtension($exe,'.xml'))
    return $exe
}
function Grant-OfflineServiceLogon([Security.Principal.SecurityIdentifier]$Sid) {
    if(!('Kafdeck.Windows.ServicePolicy' -as [type])){
        Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
namespace Kafdeck.Windows {
 public static class ServicePolicy {
  [StructLayout(LayoutKind.Sequential)] struct Attributes { public uint Length; public IntPtr Root,Name; public uint Flags; public IntPtr Descriptor,Quality; }
  [StructLayout(LayoutKind.Sequential)] struct Unicode { public ushort Length,MaximumLength; public IntPtr Buffer; }
  [DllImport("advapi32.dll")] static extern uint LsaOpenPolicy(IntPtr system,ref Attributes attributes,uint access,out IntPtr policy);
  [DllImport("advapi32.dll")] static extern uint LsaAddAccountRights(IntPtr policy,byte[] sid,Unicode[] rights,uint count);
  [DllImport("advapi32.dll")] static extern uint LsaClose(IntPtr policy);
  [DllImport("advapi32.dll")] static extern uint LsaNtStatusToWinError(uint status);
  static void Check(uint status) { if(status!=0) throw new Win32Exception((int)LsaNtStatusToWinError(status)); }
  public static void Grant(byte[] sid) {
   var a=new Attributes();a.Length=(uint)Marshal.SizeOf(a);IntPtr policy;Check(LsaOpenPolicy(IntPtr.Zero,ref a,0x810,out policy));
   const string name="SeServiceLogonRight";var right=new Unicode();right.Buffer=Marshal.StringToHGlobalUni(name);right.Length=(ushort)(name.Length*2);right.MaximumLength=(ushort)(right.Length+2);
   try { Check(LsaAddAccountRights(policy,sid,new[]{right},1)); }
   finally { Marshal.FreeHGlobal(right.Buffer);LsaClose(policy); }
  }
 }
}
'@
    }
    $bytes=New-Object byte[] $Sid.BinaryLength;$Sid.GetBinaryForm($bytes,0)
    [Kafdeck.Windows.ServicePolicy]::Grant($bytes)
}
function Assert-OfflineServiceOwner([object]$Service,[string]$Executable,[string]$Sid) {
    if(!$Service){return}
    $path=$Service.PathName.Trim()
    if($path.StartsWith('"') -and $path.EndsWith('"')){$path=$path.Substring(1,$path.Length-2)}
    $path=$path.Replace('/','\')
    $expectedPath=[IO.Path]::GetFullPath($Executable)
    $account=$Service.StartName
    # SCM can report local accounts as .\user. NTAccount requires the machine name.
    if($account.StartsWith('.\')){$account=$env:COMPUTERNAME+$account.Substring(1)}
    $owner=try {([Security.Principal.NTAccount]::new($account)).Translate([Security.Principal.SecurityIdentifier]).Value}catch{''}
    if($path -ne $expectedPath){throw "Service $($Service.Name) executable does not match this installation. Expected '$expectedPath', registered '$path'. It was preserved."}
    if($owner -ne $Sid){throw "Service $($Service.Name) account '$($Service.StartName)' does not match the installation SID '$Sid'. It was preserved."}
}
function Set-OfflineServiceRecovery([string]$Name) {
    $sc=Join-Path $env:SystemRoot 'System32/sc.exe'
    $null=Invoke-OfflineNative -File $sc -Arguments @('failure',$Name,'reset=','86400','actions=','restart/30000/restart/60000/restart/60000')
    $null=Invoke-OfflineNative -File $sc -Arguments @('failureflag',$Name,'1')
}
function Install-OfflineWindowsServices([string]$Root,[PSCredential]$Credential) {
    $sid=([Security.Principal.NTAccount]::new($Credential.UserName)).Translate([Security.Principal.SecurityIdentifier])
    # Validate both names before changing either service or copying its wrapper.
    foreach($name in @('Kafdeck-Keycloak','Kafdeck')){
        $service=Get-CimInstance Win32_Service -Filter "Name='$name'"
        Assert-OfflineServiceOwner $service (Join-Path $Root "services/$name/$name.exe") $sid.Value
    }
    Grant-OfflineServiceLogon $sid
    foreach($component in @('Keycloak','Api')){
        $name=if($component -eq 'Api'){'Kafdeck'}else{'Kafdeck-Keycloak'}
        $dependency=if($component -eq 'Api'){'Kafdeck-Keycloak'}else{'Kafdeck-PostgreSQL'}
        $exe=Write-OfflineServiceConfig $Root $component
        if(!(Get-Service -Name $name -ErrorAction SilentlyContinue)){
            # PSCredential goes directly to SCM, never to XML or command arguments.
            $null=New-Service -Name $name -DisplayName $name -Description "Kafdeck offline: $component" -BinaryPathName ('"'+$exe+'"') -StartupType Automatic -DependsOn $dependency -Credential $Credential
        }
        $sc=Join-Path $env:SystemRoot 'System32/sc.exe'
        $null=Invoke-OfflineNative -File $sc -Arguments @('config',$name,'start=','delayed-auto','depend=',$dependency)
        Set-OfflineServiceRecovery $name
    }
    Set-OfflineServiceRecovery 'Kafdeck-PostgreSQL'
}
