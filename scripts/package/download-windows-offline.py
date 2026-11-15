"""Stage original vendor media over verified HTTPS; never embed installation secrets."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import hashlib,json,re,urllib.request

root=Path(__file__).resolve().parents[2]/'artifacts/windows-offline/vendor'
root.mkdir(parents=True,exist_ok=True)
media=[
 ('java21.zip','https://aka.ms/download-jdk/microsoft-jdk-21.0.13.0.2-windows-x64.zip',None),
 ('postgresql17.zip','https://get.enterprisedb.com/postgresql/postgresql-17.12-1-windows-x64-binaries.zip',None),
 ('keycloak.zip','https://github.com/keycloak/keycloak/releases/download/26.8.1/keycloak-26.8.1.zip','4cd312c889af358a819a74a6c6f068f9ea159a6959e49924607cec95cfd4c1a4'),
 ('vc_redist.x64.exe','https://aka.ms/vc14/vc_redist.x64.exe',None),
 ('winsw.exe','https://github.com/winsw/winsw/releases/download/v2.12.0/WinSW.NET461.exe','b5066b7bbdfba1293e5d15cda3caaea88fbeab35bd5b38c41c913d492aadfc4f')]

def download(item):
 name,url,expected=item
 if name=='java21.zip':
  with urllib.request.urlopen(url+'.sha256sum.txt',timeout=60) as response:
   checksum=response.read().decode();expected=re.search(r'\b[0-9a-fA-F]{64}\b',checksum)[0].lower()
  (root/(name+'.publisher.sha256')).write_text(checksum)
 target=root/name
 if not target.exists():
  partial=target.with_suffix(target.suffix+'.partial')
  with urllib.request.urlopen(url,timeout=180) as response,partial.open('wb') as out:
   final=response.url
   while block:=response.read(1024*1024):out.write(block)
  partial.replace(target)
 else:final=url
 digest=hashlib.file_digest(target.open('rb'),'sha256').hexdigest()
 if expected and digest!=expected:raise RuntimeError('Publisher hash mismatch: '+name)
 row={'file':name,'url':url,'resolvedUrl':final,'bytes':target.stat().st_size,'sha256':digest,'pinnedPublisherHash':expected}
 print(json.dumps({'downloaded':name,'bytes':row['bytes'],'sha256':digest}),flush=True)
 return row

with ThreadPoolExecutor(max_workers=4) as pool:rows=list(pool.map(download,media))
(root/'winsw-LICENSE.txt').write_bytes(urllib.request.urlopen('https://raw.githubusercontent.com/winsw/winsw/v2.12.0/LICENSE.txt',timeout=60).read())
(root/'vendor-manifest.json').write_text(json.dumps(rows,indent=2))
