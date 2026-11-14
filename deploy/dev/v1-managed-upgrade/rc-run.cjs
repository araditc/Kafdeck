'use strict';
// Exact unsigned archive consumer. Authentication of the GitHub producer/digest is
// an independent caller responsibility, not a property of a co-located manifest.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const {spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'../../..');
const [labArg,releasedArg,mediaArg,probeArg,source,manifestHash,inputHash]=process.argv.slice(2);
assert.equal(process.platform,'win32','This managed-upgrade reference admits win-x64 only.');
for(const value of [source])assert.match(value||'',/^[a-f0-9]{40}$/);
for(const value of [manifestHash,inputHash])assert.match(value||'',/^[a-f0-9]{64}$/);
const lab=path.resolve(labArg||''),media=path.resolve(mediaArg||'');
assert.ok(fs.existsSync(path.join(lab,'services-private.json')),'Explicit owned ready lab required.');
const freshRealm=JSON.parse(fs.readFileSync(path.join(lab,'rc-first-install-realm.json'),'utf8'));
assert.equal(freshRealm.sourceSha,source,'Prepare the dedicated fresh-install realm for this exact RC before starting Keycloak.');
assert.equal(freshRealm.isolatedFromUpgradeRealms,true);
assert.match(freshRealm.issuer,/^https:\/\/localhost:18444\/realms\/kafdeck-v1-rc-install-[a-f0-9]{12}-[a-f0-9]{12}$/);
function git(args){const r=spawnSync('git',args,{cwd:root,encoding:'utf8',windowsHide:true,maxBuffer:32*1024*1024});assert.equal(r.status,0,'Source-bound reuse query failed.');return r.stdout.trim();}
const baseline='0ea9db09688b375194016b6316dc6e5c45ac5499';
assert.equal(git(['rev-parse','HEAD']),source,'Use the exact selected source checkout.');
assert.equal(git(['status','--porcelain','--untracked-files=all']),'','Clean consumer source required.');
for(const name of git(['diff','--name-only',baseline,source,'--','src/backend']).split('\n').filter(Boolean)){
 assert.ok(name.endsWith('/packages.lock.json'),'Changed backend guard source needs fresh affected negative evidence: '+name);
 const before=JSON.parse(git(['show',baseline+':'+name])),after=JSON.parse(git(['show',source+':'+name]));
 for(const key of Object.keys(before.dependencies)){
  const external=x=>Object.fromEntries(Object.entries(x).filter(([,v])=>v.type!=='Project'));
  assert.deepEqual(external(before.dependencies[key]),external(after.dependencies[key]));
 }
}
const g06=path.join(root,'docs/implementation/evidence/v1.0-g06-upgrade-20261113.json');
assert.equal(require('node:crypto').createHash('sha256').update(fs.readFileSync(path.resolve(releasedArg))).digest('hex'),JSON.parse(fs.readFileSync(g06,'utf8')).releasedExecutableSha256,'Released executable must match retained signed-v0.9 proof.');
const destination=path.join(lab,'rc-installed-'+source.slice(0,12)+'-'+Date.now());
const python=process.env.KAFDECK_RC_PYTHON||'python';
const installed=spawnSync(python,[path.join(root,'scripts/release/v1_rc.py'),'install-api','--directory',media,'--source',source,'--input-sha256',inputHash,'--manifest-sha256',manifestHash,'--destination',destination],{cwd:root,encoding:'utf8',windowsHide:true,maxBuffer:4*1024*1024});
assert.equal(installed.status,0,'RC source/media verification failed before process launch: '+installed.stderr);
const result=JSON.parse(installed.stdout);assert.equal(result.rid,'win-x64');assert.equal(result.source,source);
const execution=spawnSync(process.execPath,[path.join(__dirname,'run.cjs'),lab,path.resolve(releasedArg),result.installedApi,path.resolve(probeArg),path.join(media,'rc-manifest.json')],{cwd:root,env:process.env,windowsHide:true,stdio:'inherit'});
process.exitCode=execution.status??1;
