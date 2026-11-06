'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const {execFileSync}=require('node:child_process');
const stages=['before-actions','catalog','submitted','reviewer-before-approval','approved','expired','replanned','unknown','recovered'];
const matrixCells=['catalog','submit','approve','expiry-replan','unknown-recovery','locale-fallback','keyboard-focus','semantics-at','contrast','mobile-reflow','preferences'];
const skipped=new Set(['node_modules','bin','obj','dist','.test-dist','TestResults','artifacts','.generated','.git']);
const digest=value=>crypto.createHash('sha256').update(value).digest('hex');
function fingerprint(root){
  function group(entries,assets=false){
    const files=[];
    function visit(relative){const absolute=path.join(root,relative);if(!fs.existsSync(absolute))return;const stat=fs.statSync(absolute);
      if(stat.isDirectory()){for(const name of fs.readdirSync(absolute).sort())if(assets||!skipped.has(name))visit(path.join(relative,name));}
      else files.push({path:relative.replaceAll('\\','/'),sha256:digest(fs.readFileSync(absolute))});
    }
    if(assets)entries.forEach(visit);
    else{
      // The index is the inventory boundary. Never discover private or unrelated
      // untracked files by recursively walking production/test directories.
      const tracked=execFileSync('git',['-c','safe.directory='+root,'ls-files','-z','--',...entries],{cwd:root,encoding:'utf8',stdio:['ignore','pipe','pipe']}).split('\0').filter(Boolean);
      for(const relative of tracked){
        if(relative.split('/').some(part=>skipped.has(part))||!/\.(?:tsx?|css|html|json|cs|csproj|props|targets|config|[cm]?js|go)$/.test(relative))continue;
        const content=fs.readFileSync(path.join(root,relative),'utf8').replace(/\r\n/g,'\n');
        files.push({path:relative,sha256:digest(content)});
      }
    }
    files.sort((left,right)=>left.path<right.path?-1:left.path>right.path?1:0);assert.ok(files.length,'fingerprint group empty');return {sha256:digest(JSON.stringify(files)),files};
  }
  return {production:group(['src/backend','src/frontend/src','src/frontend/index.html','src/frontend/package.json','src/frontend/package-lock.json','Directory.Build.props','Directory.Packages.props','global.json','NuGet.config']),
    tests:group(['tests','src/frontend/tests','src/frontend/scripts','deploy/dev/browser-g2/package.json','deploy/dev/browser-g2/package-lock.json']),assets:group(['src/frontend/dist'],true)};
}
function validate(record,current,artifactDirectory){
  const errors=[],require=(condition,code)=>{if(!condition)errors.push(code);},hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
  const safeRelative=value=>typeof value==='string'&&!path.isAbsolute(value)&&!value.split(/[\\/]/).includes('..')&&!value.includes('://');
  require(record?.schemaVersion===1,'schema-version');require(record?.status==='complete','manual-status-not-complete');
  for(const group of ['production','tests','assets']){
    require(hex(record?.sourceFingerprints?.[group]?.sha256),'source-'+group+'-missing');
    if(current)require(record?.sourceFingerprints?.[group]?.sha256===current[group].sha256,'source-'+group+'-changed');
  }
  const environment=record?.environment;
  require(typeof environment?.osBuild==='string'&&environment.osBuild.length>0,'os-build-missing');require(typeof environment?.browserVersion==='string'&&environment.browserVersion.length>0,'browser-version-missing');
  require(environment?.at?.name==='NVDA'&&typeof environment.at.version==='string'&&environment.at.version.length>0,'actual-at-version-missing');
  require(hex(environment?.at?.executableSha256)&&hex(environment?.at?.installerSha256),'at-tool-fingerprint-missing');require(environment?.at?.signatureStatus==='Valid','at-signature-not-valid');
  require(environment?.synthesizer?.name==='eSpeak NG','actual-synthesizer-missing');
  require(environment?.synthesizer?.speechMode==='talk'&&environment?.synthesizer?.volume>0,'actual-speech-output-not-enabled');
  for(const locale of ['en','fa'])require(environment?.synthesizer?.observedVoices?.[locale]?.available===true&&environment?.synthesizer?.observedVoices?.[locale]?.selected===true,'actual-voice-not-observed-'+locale);
  for(const locale of ['en','fa']){
    const rows=(record?.nativeWindowReturns??[]).filter(row=>row.locale===locale),row=rows[0];require(rows.length===1,'native-window-count-'+locale);
    require(row?.profile==='headed'&&row.actualWindowSwitch===true&&row.nativeEventsVerified===true,'native-window-not-headed-'+locale);
    require(row?.sessionHttpStatus===200&&row.opaqueScopeStable===true&&row.originalInputNodeAndFocusPreserved===true&&row.selectedMaterialAndInputsPreserved===true,'native-window-state-'+locale);
    require(Array.isArray(row?.actualWindowEvents)&&row.actualWindowEvents.some(event=>event.event==='blur')&&row.actualWindowEvents.some(event=>event.event==='focus'),'native-window-events-'+locale);
    require(Array.isArray(row?.artifacts)&&row.artifacts.length>0,'native-window-evidence-'+locale);
    for(const artifact of row?.artifacts??[]){const safe=typeof artifact==='string'&&!path.isAbsolute(artifact)&&!artifact.split(/[\\/]/).includes('..')&&!artifact.includes('://');require(safe&&hex(record?.artifactHashes?.[artifact]),'native-window-artifact-hash-'+locale);if(artifactDirectory&&safe){try{require(digest(fs.readFileSync(path.join(artifactDirectory,artifact)))===record?.artifactHashes?.[artifact],'native-window-artifact-content-'+locale);}catch{require(false,'native-window-artifact-missing-'+locale);}}}
  }
  const observations=Array.isArray(record?.observations)?record.observations:[];
  require(observations.length===stages.length*2,'unexpected-stage-count');
  for(const locale of ['en','fa'])for(const stage of stages){
    const key=locale+'-'+stage,matches=observations.filter(row=>row.locale===locale&&row.stage===stage);require(matches.length===1,'stage-count-'+key);const row=matches[0];if(!row)continue;
    require(row.outcome==='pass','stage-not-passed-'+key);require(Number.isFinite(Date.parse(row.observedAtUtc)),'stage-time-'+key);
    require(row.actor===(stage==='reviewer-before-approval'?'reviewer':'requester'),'stage-actor-'+key);
    const viewer=row.readSource==='NVDA Speech Viewer',vendor=row.readSource==='NVDA vendor speech I/O log';
    require((viewer||vendor)&&row.actualReaderRunning===true,'actual-at-output-'+key);
    require(row.voiceLanguage===locale,'stage-voice-'+key);require(typeof row.expected==='string'&&row.expected.trim().length>0&&typeof row.observed==='string'&&row.observed.trim().length>0,'stage-observation-'+key);
    require(Array.isArray(row.utterances)&&row.utterances.length>0&&row.utterances.every(value=>typeof value==='string'&&value.length>0&&value.length<=1024),'stage-utterances-'+key);
    if(viewer)require(Array.isArray(row.nativeKeys)&&row.nativeKeys.length>0,'native-readback-keys-'+key);
    if(vendor){
      const trace=row.vendorTrace;
      require(row.inputSource==='Playwright Chromium keyboard'&&row.nativeWindowsGestureClaimed===false&&(!row.nativeKeys||row.nativeKeys.length===0),'vendor-keyboard-provenance-'+key);
      require(Array.isArray(row.actualDriverKeyboardActions)&&row.actualDriverKeyboardActions.length>0&&row.actualDriverKeyboardActions.every(action=>typeof action.label==='string'&&Number.isInteger(action.tabs)&&action.tabs>=0&&!('value'in action)&&!('text'in action)),'vendor-driver-keyboard-actions-'+key);
      require(Number.isInteger(trace?.processId)&&trace.processId>0&&trace.readerVersion===environment?.at?.version,'vendor-reader-process-'+key);
      require(trace?.synthesizer==='eSpeak NG'&&typeof trace.synthesizerVersion==='string'&&trace.synthesizerVersion.length>0,'vendor-synthesizer-'+key);
      require(Number.isInteger(trace?.startByte)&&trace.startByte>=0&&Number.isInteger(trace?.endByte)&&trace.endByte>trace.startByte&&hex(trace.rawLogSha256),'vendor-source-offsets-hash-'+key);
      for(const file of [trace?.file,row.keyboardEvidenceFile])require(safeRelative(file)&&row.artifacts?.includes(file)&&hex(record?.artifactHashes?.[file]),'vendor-hashed-evidence-'+key);
      if(artifactDirectory&&safeRelative(row.keyboardEvidenceFile)){try{const keyboard=JSON.parse(fs.readFileSync(path.join(artifactDirectory,row.keyboardEvidenceFile),'utf8').replace(/^\uFEFF/,''));require(keyboard.locale===locale&&keyboard.stage===stage&&keyboard.sourceSha===record.observedSourceSha&&keyboard.inputSource===row.inputSource&&JSON.stringify(keyboard.keyboardActionsSincePrevious)===JSON.stringify(row.actualDriverKeyboardActions),'vendor-keyboard-artifact-binding-'+key);}catch{require(false,'vendor-keyboard-artifact-invalid-'+key);}}
    }
    require(row.authenticatedApplication===true,'authenticated-application-'+key);
    require(Array.isArray(row.artifacts)&&row.artifacts.length>0&&row.artifacts.every(value=>typeof value==='string'&&!path.isAbsolute(value)&&!value.split(/[\\/]/).includes('..')&&!value.includes('://')),'safe-artifact-reference-'+key);
    for(const artifact of row.artifacts??[]){
      const safe=typeof artifact==='string'&&!path.isAbsolute(artifact)&&!artifact.split(/[\\/]/).includes('..')&&!artifact.includes('://');require(safe&&hex(record?.artifactHashes?.[artifact]),'artifact-hash-'+key);
      if(artifactDirectory&&safe){try{require(digest(fs.readFileSync(path.join(artifactDirectory,artifact)))===record?.artifactHashes?.[artifact],'artifact-content-'+key);}catch{require(false,'artifact-missing-'+key);}}
    }
  }
  for(const cell of matrixCells){const row=record?.matrix?.[cell];require(row?.outcome==='pass','matrix-not-passed-'+cell);require(typeof row?.observed==='string'&&row.observed.trim().length>0,'matrix-observation-'+cell);require(Array.isArray(row?.evidence)&&row.evidence.length>0,'matrix-evidence-'+cell);}
  require(Array.isArray(record?.limitations)&&record.limitations.length>0,'explicit-limitations-missing');
  require(record?.cleanup?.ownedReaderStopped===true&&record.cleanup.noGlobalSettingsChanged===true&&record.cleanup.fixtureResumed===true,'cleanup-incomplete');
  // This rejects common accidental credentials, not every possible private datum.
  // A UI operator must review excerpts before they become public evidence.
  const excerpts=observations.flatMap(row=>[row.expected,row.observed,...(row.utterances??[])]).filter(value=>typeof value==='string').join('\n');
  require(!/(?:Bearer\s+[A-Za-z0-9._~-]{16,}|eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]+|(?:access_token|refresh_token|id_token|password)\s*[:=]\s*\S+)/i.test(excerpts),'credential-pattern-in-excerpt');
  return {valid:errors.length===0,errors};
}
module.exports={fingerprint,validate,stages,matrixCells};
if(require.main===module){
  try{const [file,repository]=process.argv.slice(2);if(!file)throw Error('input');const record=JSON.parse(fs.readFileSync(file,'utf8').replace(/^\uFEFF/,''));const result=validate(record,fingerprint(repository?path.resolve(repository):path.resolve(__dirname,'../../..')),path.dirname(path.resolve(file)));console.log(JSON.stringify(result));if(!result.valid)process.exitCode=3;}
  catch{console.error('V09_W79_MANUAL_EVIDENCE_INVALID_INPUT');process.exitCode=3;}
}
