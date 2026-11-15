"""Assemble a portable, entirely offline Windows installation medium."""
from pathlib import Path
import argparse,hashlib,json,shutil,subprocess,zipfile,urllib.parse

parser=argparse.ArgumentParser();parser.add_argument('--stage-only',action='store_true');args=parser.parse_args()
repo=Path(__file__).resolve().parents[2];out=repo/'artifacts/windows-offline';vendor=out/'vendor';bundle=out/'bundle'
sha=lambda p:hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
vendors=json.loads((vendor/'vendor-manifest.json').read_text())
for row in vendors:row['resolvedUrl']=urllib.parse.urlsplit(row['resolvedUrl'])._replace(query='',fragment='').geturl()
for row in vendors:
 if sha(vendor/row['file'])!=row['sha256']:raise RuntimeError('Vendor medium changed')
bundle.mkdir(parents=True,exist_ok=True);(bundle/'media').mkdir(exist_ok=True)
def repack(source,destination,select=None):
 if destination.exists():return
 with zipfile.ZipFile(source) as src,zipfile.ZipFile(destination,'w',zipfile.ZIP_DEFLATED,compresslevel=6) as dst:
  files=[i for i in src.infolist() if not i.is_dir()]
  roots={i.filename.split('/')[0] for i in files}
  if len(roots)!=1:raise RuntimeError('Unexpected vendor archive root')
  prefix=next(iter(roots))+'/'
  for item in files:
   relative=item.filename[len(prefix):]
   if not relative or '..' in Path(relative).parts or relative.startswith('/'):raise RuntimeError('Unsafe vendor archive path')
   if select and not select(relative):continue
   dst.writestr(relative,src.read(item))
repack(vendor/'java21.zip',bundle/'media/java21.zip')
repack(vendor/'keycloak.zip',bundle/'media/keycloak.zip')
repack(vendor/'postgresql17.zip',bundle/'media/postgresql17.zip',lambda name:name.startswith(('bin/','lib/','share/')) or name in ('server_license.txt','commandlinetools_3rd_party_licenses.txt'))
shutil.copyfile(vendor/'vc_redist.x64.exe',bundle/'media/vc_redist.x64.exe')
shutil.copyfile(vendor/'winsw.exe',bundle/'media/winsw.exe')
shutil.copyfile(vendor/'winsw-LICENSE.txt',bundle/'WinSW-LICENSE.txt')
payload=bundle/'payload'
# The existing native publish has already passed its focused API/UI/SQLite smoke.
source=repo/'artifacts/windows-server/payload'
for folder in ('app','agent'):
 shutil.copytree(source/folder,payload/folder,dirs_exist_ok=True,ignore=shutil.ignore_patterns('*.pdb'))
(payload/'scripts').mkdir(parents=True,exist_ok=True);(payload/'docs').mkdir(exist_ok=True)
for file in (repo/'deploy/windows').glob('*.ps1'):shutil.copyfile(file,payload/'scripts'/file.name)
for name in ('windows-offline-install.fa.md','kafka-agent-java21.fa.md','windows-server-monitoring-advanced.fa.md'):
 shutil.copyfile(repo/'docs/operator'/name,payload/'docs'/name)
shutil.copyfile(repo/'deploy/windows/Install-Offline.ps1',bundle/'Install-Kafdeck.ps1')
shutil.copyfile(repo/'docs/operator/windows-offline-install.fa.md',bundle/'README.fa.md')
shutil.copyfile(repo/'LICENSE',bundle/'LICENSE')
evidence=out/'acceptance-evidence.json'
if evidence.exists():shutil.copyfile(evidence,bundle/'acceptance-evidence.json')
service_evidence=out/'windows-services-evidence.json'
if service_evidence.exists():shutil.copyfile(service_evidence,bundle/'windows-services-evidence.json')
(bundle/'THIRD-PARTY-NOTICES.txt').write_text('Included components:\nMicrosoft Build of OpenJDK 21.0.13.0.2: upstream license/legal files inside media/java21.zip; GPLv2 with Classpath Exception. Sources: https://github.com/microsoft/openjdk-jdk21u/tree/release/jdk-21.0.13.0.2_1\nPostgreSQL 17.12 (server and command-line tools): upstream server_license.txt and commandlinetools_3rd_party_licenses.txt inside media/postgresql17.zip. Source: https://www.postgresql.org/ftp/source/v17.12/\nKeycloak 26.8.1: upstream LICENSE.txt and notices inside media/keycloak.zip; source: https://github.com/keycloak/keycloak/tree/26.8.1\nMicrosoft Visual C++ Redistributable: original Microsoft-signed installer. Terms: https://visualstudio.microsoft.com/license-terms/\nWinSW 2.12.0 (.NET Framework 4.6.1 build): MIT, included WinSW-LICENSE.txt. Source: https://github.com/winsw/winsw/tree/v2.12.0\nKafdeck: included LICENSE. This is a local unsigned monitoring build, not an official signed release.\n',encoding='utf-8')
files=sorted(p for p in bundle.rglob('*') if p.is_file() and p.name!='bundle-manifest.json')
assert not any(p.suffix.lower() in ('.pfx','.key','.pem','.cer','.dpapi','.sql','.sqlite','.db') for p in files)
manifest={'format':1,'version':'1.0.0-rc.1+monitoring.20261115','installerCommit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'nativeRuntimeCommit':'183d98cf024c901c91d0776ff088543ea75782bd','deploymentMode':'windows-services','officialRelease':False,'vendors':vendors,'files':[{'path':p.relative_to(bundle).as_posix(),'bytes':p.stat().st_size,'sha256':sha(p)} for p in files]}
(bundle/'bundle-manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
if not args.stage_only:
 archive=out/'kafdeck-offline-win-x64.zip'
 with zipfile.ZipFile(archive,'w') as z:
  for p in sorted(bundle.rglob('*')):
   if p.is_file():z.write(p,p.relative_to(bundle).as_posix(),compress_type=zipfile.ZIP_STORED if p.suffix=='.zip' else zipfile.ZIP_DEFLATED,compresslevel=6)
 with zipfile.ZipFile(archive) as z:
  assert z.testzip() is None
  for row in manifest['files']:
   assert hashlib.sha256(z.read(row['path'])).hexdigest()==row['sha256']
 archive.with_suffix('.zip.sha256').write_text(sha(archive)+'  '+archive.name+'\n',encoding='ascii')
 print(json.dumps({'archive':str(archive),'bytes':archive.stat().st_size,'sha256':sha(archive),'filesVerified':len(files)}),flush=True)
else:print(json.dumps({'staged':str(bundle),'files':len(files)}),flush=True)
