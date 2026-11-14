'use strict';
// Run before starting the explicitly owned Keycloak fixture. Never reuse an
// upgrade realm: first-owner setup correctly rejects existing human accounts.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const lab=path.resolve(process.argv[2]||''),source=process.argv[3];
assert.match(source||'',/^[a-f0-9]{40}$/);
assert.ok(fs.existsSync(path.join(lab,'services-private.json')),'Existing owned G06 laboratory required.');
const read=p=>JSON.parse(fs.readFileSync(p,'utf8').replace(/^\uFEFF/,''));
const managed=read(path.join(lab,'managed/lab.json'));
const realm=read(path.join(lab,'keycloak/data/import/kafdeck-managed-g06-sqlite-realm.json'));
assert.equal(realm.realm,'kafdeck-managed-g06-sqlite');
const provisioner=realm.clients.find(c=>c.clientId===managed.clientId);
assert.ok(provisioner?.serviceAccountsEnabled&&provisioner.secret===managed.secret,'Expected private provisioner template.');
const browser=realm.clients.find(c=>c.clientId==='g06-browser');
assert.ok(browser?.publicClient&&browser.standardFlowEnabled&&browser.redirectUris?.includes('https://localhost:18460/signin-oidc'),'Exact owned PKCE browser redirect required.');
// Import IDs are realm-local fixture implementation details; omit them so the
// new realm cannot collide with retained service/client/user IDs.
function freshIds(value){if(Array.isArray(value))value.forEach(freshIds);else if(value&&typeof value==='object'){delete value.id;Object.values(value).forEach(freshIds);}}
freshIds(realm);
realm.realm='kafdeck-v1-rc-install-'+source.slice(0,12)+'-'+crypto.randomBytes(6).toString('hex');
realm.users=(realm.users||[]).filter(u=>u.serviceAccountClientId);
assert.ok(realm.users.some(u=>u.serviceAccountClientId===managed.clientId));
realm.registrationAllowed=false;realm.verifyEmail=false;
const destination=path.join(lab,'keycloak/data/import',realm.realm+'-realm.json');
fs.writeFileSync(destination,JSON.stringify(realm,null,2),{flag:'wx',mode:0o600});
const marker={sourceSha:source,realm:realm.realm,issuer:'https://localhost:18444/realms/'+realm.realm,isolatedFromUpgradeRealms:true};
fs.writeFileSync(path.join(lab,'rc-first-install-realm.json'),JSON.stringify(marker,null,2),{mode:0o600});
console.log(JSON.stringify({freshRealmPrepared:true,sourceSha:source,issuer:marker.issuer}));
