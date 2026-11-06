import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
const { fingerprint } = createRequire(import.meta.url)('../scripts/v09-accessibility-manual-evidence.cjs');

function fixture(t) {
  const directory=fs.mkdtempSync(path.join(os.tmpdir(),'kafdeck-w79-fingerprint-'));
  t.after(()=>{assert.equal(path.dirname(directory),path.resolve(os.tmpdir()));assert.ok(path.basename(directory).startsWith('kafdeck-w79-fingerprint-'));fs.rmSync(directory,{recursive:true,force:true});});
  const git=(...args)=>execFileSync('git',['-c','safe.directory='+directory,...args],{cwd:directory,stdio:['ignore','pipe','pipe']});
  git('init','--quiet');
  const write=(relative,text)=>{const file=path.join(directory,relative);fs.mkdirSync(path.dirname(file),{recursive:true});fs.writeFileSync(file,text);};
  write('src/backend/Tracked.cs','class Tracked {}\n');write('tests/Tracked.cs','class Test {}\n');write('src/frontend/dist/app.js','asset\r\n');
  git('add','--','src/backend/Tracked.cs','tests/Tracked.cs');
  return {directory,git,write};
}

test('W79 fingerprints normalize tracked source checkout line endings but preserve exact asset bytes',t=>{
  const {directory,write}=fixture(t),before=fingerprint(directory);
  write('src/backend/Tracked.cs','class Tracked {}\r\n');write('tests/Tracked.cs','class Test {}\r\n');
  const windowsCheckout=fingerprint(directory);assert.equal(windowsCheckout.production.sha256,before.production.sha256);assert.equal(windowsCheckout.tests.sha256,before.tests.sha256);
  write('src/frontend/dist/app.js','asset\n');assert.notEqual(fingerprint(directory).assets.sha256,before.assets.sha256);
  write('src/backend/Tracked.cs','class Changed {}\n');assert.notEqual(fingerprint(directory).production.sha256,before.production.sha256);
});

test('W79 fingerprints exclude untracked private files and tracked build intermediates',t=>{
  const {directory,git,write}=fixture(t),before=fingerprint(directory);
  write('src/backend/Properties/launchSettings.json','{"private":"untracked fixture"}');write('src/backend/packages.lock.json','{"private":"untracked fixture"}');write('tests/Untracked.cs','class Private {}');
  write('src/backend/obj/generated.json','{"generated":true}');git('add','--force','--','src/backend/obj/generated.json');
  const after=fingerprint(directory);assert.equal(after.production.sha256,before.production.sha256);assert.equal(after.tests.sha256,before.tests.sha256);
  assert.deepEqual(after.production.files.map(file=>file.path),['src/backend/Tracked.cs']);assert.deepEqual(after.tests.files.map(file=>file.path),['tests/Tracked.cs']);
  write('src/backend/Properties/launchSettings.json','{"private":"different untracked fixture"}');assert.equal(fingerprint(directory).production.sha256,before.production.sha256);
});

test('W79 fingerprints reject a production baseline containing only untracked files',t=>{
  const {directory,git}=fixture(t);git('rm','--cached','--','src/backend/Tracked.cs');assert.throws(()=>fingerprint(directory),/fingerprint group empty/);
});
