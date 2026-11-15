# Internal certificate-authority and server certificate for this installation.
function New-KafdeckInstallCertificate {
    param([string]$ServerAddress,[string]$Secrets,[Security.SecureString]$Password)
    $utf8=New-Object Text.UTF8Encoding($false)
    $ca=New-SelfSignedCertificate -Type Custom -Subject "CN=Kafdeck Local CA $ServerAddress" -CertStoreLocation Cert:\CurrentUser\My -KeyAlgorithm RSA -KeyLength 3072 -HashAlgorithm SHA256 -KeyExportPolicy Exportable -KeyUsage CertSign,CRLSign -NotAfter (Get-Date).AddYears(3) -TextExtension @('2.5.29.19={critical}{text}CA=true&pathlength=0')
    $ip=$null;$san=if([Net.IPAddress]::TryParse($ServerAddress,[ref]$ip)){"IPAddress=$ServerAddress"}else{"DNS=$ServerAddress"}
    $leaf=New-SelfSignedCertificate -Type Custom -Subject "CN=$ServerAddress" -Signer $ca -CertStoreLocation Cert:\CurrentUser\My -KeyAlgorithm RSA -KeyLength 3072 -HashAlgorithm SHA256 -KeyExportPolicy Exportable -KeyUsage DigitalSignature,KeyEncipherment -NotAfter (Get-Date).AddYears(1) -TextExtension @("2.5.29.17={text}$san",'2.5.29.37={text}1.3.6.1.5.5.7.3.1')
    $pfx=Join-Path $Secrets 'server.pfx'
    Export-PfxCertificate -Cert $leaf -FilePath $pfx -Password $Password|Out-Null
    Export-PfxCertificate -Cert $ca -FilePath (Join-Path $Secrets 'server-ca-private.pfx') -Password $Password|Out-Null
    Export-Certificate -Cert $ca -FilePath (Join-Path $Secrets 'server-public.cer')|Out-Null
    $pem="-----BEGIN CERTIFICATE-----`n"+[Convert]::ToBase64String($ca.RawData,[Base64FormattingOptions]::InsertLineBreaks)+"`n-----END CERTIFICATE-----`n"
    [IO.File]::WriteAllText((Join-Path $Secrets 'server-public.pem'),$pem,$utf8)
    [pscustomobject]@{Certificate=$leaf;Authority=$ca;Pfx=$pfx}
}
