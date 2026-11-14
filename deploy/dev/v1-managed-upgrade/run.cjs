'use strict';
// Actual released/candidate processes and real Keycloak/Kafka/DBs. The three
// operation snapshots are explicitly synthetic compatibility fixtures (W80 model).
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const {spawn,spawnSync}=require('node:child_process');
const root=path.resolve(__dirname,'../../..');
const lab=path.resolve(process.argv[2]||''),oldExe=path.resolve(process.argv[3]||''),candidate=path.resolve(process.argv[4]||''),probe=path.resolve(process.argv[5]||'');
assert.ok(lab&&fs.existsSync(path.join(lab,'services-private.json')),'Explicit ready owned fixture required.');
assert.ok(fs.existsSync(oldExe)&&fs.existsSync(candidate)&&fs.existsSync(probe),'Identified released/candidate/probe binaries required.');
const {chromium}=require(process.env.KAFDECK_BROWSER_PLAYWRIGHT||path.join(root,'deploy/dev/browser-g2/node_modules/playwright'));
const read=f=>JSON.parse(fs.readFileSync(f,'utf8').replace(/^\uFEFF/,'')),sha=f=>crypto.createHash('sha256').update(fs.readFileSync(f)).digest('hex');
const write=(f,x)=>fs.writeFileSync(f,JSON.stringify(x,null,2));
const rcManifest=process.argv[6]?read(path.resolve(process.argv[6])):null;
if(rcManifest){
 assert.equal(rcManifest.kind,'unsigned-v1-native-candidate');assert.equal(rcManifest.rid,'win-x64');assert.equal(rcManifest.signed,false);assert.equal(rcManifest.releaseReady,false);
 for(const [name,hash] of Object.entries(rcManifest.components['api-ui'].files)){assert.ok(!name.includes('..')&&!path.isAbsolute(name));assert.equal(sha(path.join(path.dirname(candidate),name)),hash,'Installed RC file mismatch: '+name);}
}
const reportName=rcManifest?'v1-rc-upgrade.json':'g06-upgrade.json';
const managed=read(path.join(lab,'managed/lab.json')),identity=read(path.join(lab,'identity/lab.json'));
assert.ok(new URL(managed.issuer).hostname==='localhost');
const api='https://localhost:18460',reportDirectory=path.join(root,'artifacts/stabilization-results');fs.mkdirSync(reportDirectory,{recursive:true});
const pg=path.join(lab,'..','..','deploy/dev/.generated/g2/postgres/pgsql/bin');
// pg tools can be selected explicitly; no connection to an operator database.
const pgBin=process.env.KAFDECK_G06_POSTGRES_BIN||path.resolve(root,'../../deploy/dev/.generated/g2/postgres/pgsql/bin');
let host,browser,phase='input',privateDirectory;
let lastPhase;const progress=setInterval(()=>{if(lastPhase!==phase){lastPhase=phase;console.log('G06 '+phase);}},1000);progress.unref();
const run=(exe,args,env)=>{
 const result=spawnSync(exe,args,{windowsHide:true,env:env||process.env,encoding:'utf8',maxBuffer:8*1024*1024});
 if(result.status!==0){fs.appendFileSync(path.join(lab,'commands-private.log'),String(result.stdout||'')+String(result.stderr||''));throw Error('Owned compatibility command failed: '+path.basename(exe));}
 return result.stdout;
};
function probeRun(args){return run('dotnet',[probe,...args]);}
async function stop(){if(!host)return;const p=host;host=null;if(p.exitCode===null){if(process.platform==='win32')spawnSync('taskkill',['/PID',String(p.pid),'/T','/F'],{windowsHide:true,stdio:'ignore'});else p.kill('SIGKILL');await new Promise(resolve=>p.exitCode!==null?resolve():p.once('exit',resolve));}}
async function start(executable,directory,expectFailure=false){
 await stop();const log=fs.openSync(path.join(directory,'api-private.log'),'a');
 const managedCandidate=executable===candidate&&path.extname(candidate).toLowerCase()==='.dll';
 host=spawn(managedCandidate?'dotnet':executable,managedCandidate?[candidate]:[],{cwd:directory,env:{...process.env,ASPNETCORE_CONTENTROOT:directory,ASPNETCORE_ENVIRONMENT:'Production'},windowsHide:true,stdio:['ignore',log,log]});fs.closeSync(log);
 const context=await browser.newContext({ignoreHTTPSErrors:true});
 try{for(let i=0;i<120;i++){
  if(host.exitCode!==null){if(expectFailure)return;throw Error('Owned API exited before readiness.');}
  try{const response=await context.request.get(api+'/healthz',{timeout:500});if(response.ok()){if(expectFailure)throw Error('Unsupported state unexpectedly started.');return;}}catch(error){if(expectFailure&&error.message.includes('unexpectedly'))throw error;}
  await new Promise(resolve=>setTimeout(resolve,100));
 }throw Error('Bounded API startup window exceeded.');}finally{await context.close();}
}
async function login(email,password){
 const context=await browser.newContext({ignoreHTTPSErrors:true,viewport:{width:1440,height:1000}}),page=await context.newPage();
 await page.goto(api+'/api/v1/auth/login');await page.locator('#username').fill(email);await page.locator('#password').fill(password);
 await Promise.all([page.waitForURL(url=>url.origin===api&&!url.pathname.includes('signin-oidc'),{timeout:30000}),page.locator('#kc-login').click()]);return {context,page};
}
async function post(context,url,value,method='POST'){
 const setup=url==='/api/v1/setup';const token=await context.request.get(api+(setup?'/api/v1/setup':'/api/v1/auth/csrf'));assert.equal(token.status(),200);const t=await token.json();const csrf=setup?{headerName:'X-Kafdeck-CSRF',requestToken:t.csrf}:t;
 const response=await context.request.fetch(api+url,{method,data:value,headers:{[csrf.headerName]:csrf.requestToken}});
 if(!response.ok())throw Error('Owned route rejected '+url+' status='+response.status());return response.status()===204?null:response.json();
}
async function subjects(context,id){const response=await context.request.get(api+'/api/v1/clusters/'+id+'/schemas/subjects');return {status:response.status(),text:await response.text()};}
function copyKeys(state,directory){
 const keys=path.join(directory,'keys');fs.mkdirSync(keys,{recursive:true});const ids=new Set();
 for(const target of state.Connections||[])for(const service of ['SchemaRegistry','Connect','KsqlDb'])if(target[service]?.ProtectedCredential){
  const raw=Buffer.from(target[service].ProtectedCredential,'base64url'),b=raw.subarray(4,20);assert.equal(b.length,16);
  const hex=Buffer.from([b[3],b[2],b[1],b[0],b[5],b[4],b[7],b[6],...b.subarray(8)]).toString('hex');ids.add(hex.slice(0,8)+'-'+hex.slice(8,12)+'-'+hex.slice(12,16)+'-'+hex.slice(16,20)+'-'+hex.slice(20));
 }
 const source=process.env.KAFDECK_G06_LEGACY_KEY_DIRECTORY||path.join(process.env.LOCALAPPDATA||'','ASP.NET/DataProtection-Keys');
 for(const id of ids)fs.copyFileSync(path.join(source,'key-'+id+'.xml'),path.join(keys,'key-'+id+'.xml'));
 assert.ok(ids.size>0);return keys;
}
function configure(provider,directory){
 const secret=path.join(lab,'managed/service-secret.txt');if(!fs.existsSync(secret))fs.writeFileSync(secret,managed.secret);
 const digest=path.join(directory,'digest.key');if(!fs.existsSync(digest))fs.writeFileSync(digest,crypto.randomBytes(32).toString('hex'));
 const settings={
  'Kafdeck:Deployment:ListenUrls:0':api,'Kafdeck:Deployment:AccessMode':'Oidc',
  'Kafdeck:Deployment:Oidc:Issuer':'https://localhost:18444/realms/kafdeck-managed-g06-'+provider,
  'Kafdeck:Deployment:Oidc:ClientId':'g06-browser','Kafdeck:Deployment:Oidc:Scopes:0':'openid',
  'Kafdeck:Deployment:Oidc:TrustedCaPemFile':identity.caPemFile,
  'Kafdeck:ManagedIdentity:Enabled':'true','Kafdeck:ManagedIdentity:StateFile':path.join(directory,'managed-state.json'),
  'Kafdeck:ManagedIdentity:ServiceClientId':managed.clientId,'Kafdeck:ManagedIdentity:ServiceSecretFile':secret,
  'Kafdeck:ConnectionWorkspace:Enabled':'true','Kafdeck:ConnectionWorkspace:RegistrationNetworks:0':'127.0.0.0/8',
  'Kafdeck:Administration:Mutations:Enabled':'true','Kafdeck:Administration:Mutations:BackgroundDataWorkersEnabled':'false',
  'Kafdeck:Administration:Mutations:Persistence:Provider':provider==='sqlite'?'Sqlite':'PostgreSql',
  'Kafdeck:Administration:Mutations:Persistence:ExecutionMode':'Standalone',
  'Kafdeck:Administration:Mutations:MaterialDigestKey':'file:'+digest,'Logging:LogLevel:Default':'Warning',
  'Kestrel:Certificates:Default:Path':path.join(lab,'identity/https.p12'),
  'Kestrel:Certificates:Default:Password':fs.readFileSync(path.join(lab,'identity/https-password.txt'),'utf8').trim()
 };
 if(provider==='sqlite')settings['Kafdeck:Administration:Mutations:Persistence:SqliteDatabasePath']=path.join(directory,'mutations.db');
 else settings['Kafdeck:Administration:Mutations:Persistence:ConnectionString']='file:'+path.join(lab,'postgres-connection.txt');
 return settings;
}
(async()=>{
 await new Promise((resolve,reject)=>{const socket=require('node:net').createServer();socket.once('error',reject);socket.listen(18460,'::',()=>socket.close(resolve));});
 browser=await chromium.launch({headless:true,...(process.env.KAFDECK_G2_BROWSER_EXECUTABLE?{executablePath:process.env.KAFDECK_G2_BROWSER_EXECUTABLE}:{})});
 const summary={releasedExecutableSha256:sha(oldExe),candidateDllSha256:!rcManifest?sha(candidate):undefined,candidateFileSha256:sha(candidate),developmentCandidate:!rcManifest,unsignedRc:!!rcManifest,rcSourceSha:rcManifest?.sourceSha,rcManifestSha256:process.argv[6]?sha(path.resolve(process.argv[6])):null,installedFromArchive:!!rcManifest,negativeGuardCasesReused:!!rcManifest,publicationClaimed:false,actualReleasedAndCandidateProcesses:true,genuineKeycloak:true,actualKafka:true,operationStatesAreSynthetic:true,providers:[]};
 try{
  phase='seed-reference-kafka';if(!fs.existsSync(path.join(lab,'kafka-reference-ready'))){probeRun(['kafka','127.0.0.1:19092']);fs.writeFileSync(path.join(lab,'kafka-reference-ready'),'20 topics / 100 records / 5 groups');}
  if(rcManifest){
   phase='rc-first-install';const fresh=path.join(lab,'rc-first-install-'+crypto.randomBytes(8).toString('hex'));fs.mkdirSync(fresh);
   const settings=configure('sqlite',fresh);settings['Kafdeck:DataProtection:KeyDirectory']=path.join(fresh,'keys');fs.mkdirSync(settings['Kafdeck:DataProtection:KeyDirectory']);write(path.join(fresh,'appsettings.json'),settings);
   fs.cpSync(path.join(path.dirname(candidate),'wwwroot'),path.join(fresh,'wwwroot'),{recursive:true});
   const issued=run(candidate,['--issue-setup-code'],{...process.env,ASPNETCORE_CONTENTROOT:fresh}).trim().split(/\s/).pop();
   await start(candidate,fresh);const bootstrap=await browser.newContext({ignoreHTTPSErrors:true});
   const email='rc-owner-'+crypto.randomBytes(8).toString('hex')+'@rc.example.invalid',password=crypto.randomBytes(18).toString('hex');
   await post(bootstrap,'/api/v1/setup',{code:issued,email,password});await bootstrap.close();const owner=await login(email,password);
   const users=await owner.context.request.get(api+'/api/v1/identity/users');assert.equal(users.status(),200);assert.ok((await users.text()).includes(email));
   const target=await post(owner.context,'/api/v1/connections/targets',{name:'RC fresh install',bootstrapServers:['127.0.0.1:19092'],protocol:'PLAINTEXT',mechanism:null});
   const topics=await owner.context.request.get(api+'/api/v1/clusters/'+target.clusterId+'/topics');assert.equal(topics.status(),200);
   summary.firstInstall={provider:'sqlite',nativeArchive:true,firstOwnerCreated:true,keycloakOwnerLogin:true,targetRegistration:true,actualKafkaTopicRead:true};
   await owner.context.close();await stop();
  }
  for(const provider of ['sqlite','postgres']){
   phase=provider+'-legacy-start';const directory=path.join(lab,provider);privateDirectory=directory;fs.mkdirSync(directory,{recursive:true});
   const checkpointFile=path.join(directory,'checkpoint-private.json');let checkpoint=fs.existsSync(checkpointFile)?read(checkpointFile):{email:'owner-'+provider+'@g06.example.invalid',password:crypto.randomBytes(18).toString('hex')};write(checkpointFile,checkpoint);
   const settings=configure(provider,directory),config=path.join(directory,'appsettings.json'),stateFile=path.join(directory,'managed-state.json');
   const saveConfig=()=>write(config,settings);saveConfig();fs.cpSync(path.join(path.dirname(oldExe),'wwwroot'),path.join(directory,'wwwroot'),{recursive:true});
   if(!checkpoint.backup){
    if(!fs.existsSync(stateFile)){
     const code=run(oldExe,['--issue-setup-code'],{...process.env,ASPNETCORE_CONTENTROOT:directory});checkpoint.code=code.trim().split(/\s/).pop();write(checkpointFile,checkpoint);
    }
    await start(oldExe,directory);const bootstrap=await browser.newContext({ignoreHTTPSErrors:true});
    if(!read(stateFile).OwnerId){phase=provider+'-first-owner';await post(bootstrap,'/api/v1/setup',{code:checkpoint.code,email:checkpoint.email,password:checkpoint.password});}
    await bootstrap.close();phase=provider+'-legacy-login';let actor=await login(checkpoint.email,checkpoint.password);
    if(!checkpoint.target){
     phase=provider+'-register-target';const target=await post(actor.context,'/api/v1/connections/targets',{name:'G06 reference',bootstrapServers:['127.0.0.1:19092'],protocol:'PLAINTEXT',mechanism:null});checkpoint.target=target.clusterId;write(checkpointFile,checkpoint);
    }
    if(!checkpoint.manager){const account=await post(actor.context,'/api/v1/identity/users',{email:'manager-'+provider+'@g06.example.invalid',password:crypto.randomBytes(18).toString('hex'),role:'manager',clusterIds:[checkpoint.target]});checkpoint.manager=account.id;write(checkpointFile,checkpoint);}
    phase=provider+'-protect-services';for(const slug of ['schema-registry','kafka-connect','ksqldb'])await post(actor.context,'/api/v1/connections/'+checkpoint.target+'/'+slug,{url:'http://127.0.0.1:19084',authentication:'basic',username:'g06-reference',secret:'g06-credential-fixture'},'PUT');
    phase=provider+'-legacy-read-service';const originalRead=await subjects(actor.context,checkpoint.target);assert.equal(originalRead.status,200);assert.ok(originalRead.text.includes('g06-reference-00-value'));
    const state=read(stateFile);checkpoint.owner=state.OwnerId;checkpoint.issuer=state.Issuer;checkpoint.keys=copyKeys(state,directory);write(checkpointFile,checkpoint);
    await actor.context.close();await stop();
    phase=provider+'-seed-compatibility-states';const location=provider==='sqlite'?path.join(directory,'mutations.db'):path.join(lab,'postgres-connection.txt');
    probeRun(['seed',provider,location,path.join(directory,'before-db.json'),'oidc:'+state.Issuer+'|'+state.OwnerId,checkpoint.target,'oidc:'+state.Issuer+'|'+checkpoint.manager]);
    // Old released process must also open the compatibility fixture before backup.
    await start(oldExe,directory);actor=await login(checkpoint.email,checkpoint.password);const users=await actor.context.request.get(api+'/api/v1/identity/users');assert.equal(users.status(),200);checkpoint.users=await users.json();await actor.context.close();await stop();
    phase=provider+'-backup';fs.copyFileSync(stateFile,path.join(directory,'state-backup.json'));
    if(provider==='sqlite')probeRun(['backup',provider,location,path.join(directory,'backup.db')]);
    else run(path.join(pgBin,'pg_dump.exe'),['-h','127.0.0.1','-p','15440','-U','kafdeck','-d','g06_upgrade','-Fc','-f',path.join(directory,'backup.pgdump')],{...process.env,PGPASSFILE:path.join(lab,'pgpass')});
    checkpoint.backup=true;write(checkpointFile,checkpoint);
   }
   const location=provider==='sqlite'?path.join(directory,'mutations.db'):path.join(lab,'postgres-connection.txt');
   if(rcManifest){
    phase=provider+'-released-backup-reference';await start(oldExe,directory);const legacyActor=await login(checkpoint.email,checkpoint.password);
    const legacyUsers=await legacyActor.context.request.get(api+'/api/v1/identity/users');assert.equal(legacyUsers.status(),200);
    const legacyValue=await legacyUsers.json();for(const field of ['users','clusterIds','clusterNames'])assert.deepEqual(legacyValue[field],checkpoint.users[field]);
    const legacyService=await subjects(legacyActor.context,checkpoint.target);assert.equal(legacyService.status,200);
    await legacyActor.context.close();await stop();
   }
   settings['Kafdeck:DataProtection:KeyDirectory']=checkpoint.keys;saveConfig();
   fs.cpSync(rcManifest?path.join(path.dirname(candidate),'wwwroot'):path.join(root,'src/frontend/dist'),path.join(directory,'wwwroot'),{recursive:true});
   phase=provider+'-candidate-start';await start(candidate,directory);let actor=await login(checkpoint.email,checkpoint.password);
   const users=await actor.context.request.get(api+'/api/v1/identity/users');assert.equal(users.status(),200);const currentUsers=await users.json();
   for(const field of ['users','clusterIds','clusterNames'])assert.deepEqual(currentUsers[field],checkpoint.users[field]);
   assert.deepEqual(currentUsers.pendingProvisionings,[]);
   const retained=await subjects(actor.context,checkpoint.target);assert.equal(retained.status,200);assert.ok(retained.text.includes('g06-reference-00-value'));
   if(rcManifest){const topics=await actor.context.request.get(api+'/api/v1/clusters/'+checkpoint.target+'/topics');assert.equal(topics.status(),200);}
   const session=await actor.context.request.get(api+'/api/v1/auth/session');assert.match((await session.json()).sessionScope,/^[a-f0-9]{64}$/);
   await actor.context.close();await stop();
   probeRun(['observe',provider,location,path.join(directory,'after-db.json')]);assert.deepEqual(read(path.join(directory,'after-db.json')),read(path.join(directory,'before-db.json')));
   assert.equal(sha(stateFile),sha(path.join(directory,'state-backup.json')));
   if(!rcManifest){
   phase=provider+'-unsupported-state';const legacy=fs.readFileSync(stateFile);write(stateFile,{...JSON.parse(legacy),FutureFormatVersion:2});const unsupportedHash=sha(stateFile);
   await start(candidate,directory,true);assert.equal(sha(stateFile),unsupportedHash);await stop();fs.writeFileSync(stateFile,legacy);
   phase=provider+'-missing-keyring';settings['Kafdeck:DataProtection:KeyDirectory']=path.join(directory,'missing-keys');fs.mkdirSync(settings['Kafdeck:DataProtection:KeyDirectory'],{recursive:true});saveConfig();
   await start(candidate,directory);actor=await login(checkpoint.email,checkpoint.password);const denied=await subjects(actor.context,checkpoint.target);assert.ok(denied.status!==200||!denied.text.includes('g06-reference-00-value'));await actor.context.close();await stop();assert.equal(sha(stateFile),sha(path.join(directory,'state-backup.json')));
   }
   phase=provider+'-verified-restore';
   fs.copyFileSync(path.join(directory,'state-backup.json'),stateFile);
   if(provider==='sqlite'){for(const suffix of ['-wal','-shm'])if(fs.existsSync(location+suffix))fs.unlinkSync(location+suffix);fs.copyFileSync(path.join(directory,'backup.db'),location);}
   else run(path.join(pgBin,'pg_restore.exe'),['-h','127.0.0.1','-p','15440','-U','kafdeck','-d','g06_upgrade','--clean','--if-exists','--no-owner',path.join(directory,'backup.pgdump')],{...process.env,PGPASSFILE:path.join(lab,'pgpass')});
   settings['Kafdeck:DataProtection:KeyDirectory']=checkpoint.keys;saveConfig();await start(candidate,directory);actor=await login(checkpoint.email,checkpoint.password);const restored=await subjects(actor.context,checkpoint.target);assert.equal(restored.status,200);assert.ok(restored.text.includes('g06-reference-00-value'));await actor.context.close();await stop();
   probeRun(['observe',provider,location,path.join(directory,'restored-db.json')]);assert.deepEqual(read(path.join(directory,'restored-db.json')),read(path.join(directory,'before-db.json')));
   summary.providers.push({provider,usersTargetsRolesRetained:true,stateUnchangedOnRead:true,protectedCredentialRestoredWithMatchingKeyring:true,missingKeyringDeniedWithoutClearingState:rcManifest?'reused G06 unchanged guard source':true,newerFormatRejectedWithoutWrite:rcManifest?'reused G06 unchanged guard source':true,actualProcessInterruptionAndVerifiedRestore:true,databaseSnapshots:read(path.join(directory,'restored-db.json')),legacyStateSha256:sha(path.join(directory,'state-backup.json')),candidateStateSha256:sha(stateFile)});
   write(path.join(reportDirectory,reportName),summary);
  }
  console.log(rcManifest?'V1-4 actual released-to-selected-unsigned-RC native upgrade/restore passed on both databases.':'G06 actual released-to-development-candidate upgrade/restore passed on both databases.');
 }finally{await stop();await browser.close();}
})().catch(error=>{fs.writeFileSync(path.join(lab,'upgrade-failure-private.log'),String(error.stack||error));console.error('G06_FAILED phase='+phase+'; private diagnostic retained.');process.exitCode=1;});
