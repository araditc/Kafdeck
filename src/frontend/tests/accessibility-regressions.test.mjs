import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import {AppShell} from '../dist/test-source/app/AppShell.js';
import {StatusBadge} from '../dist/test-source/app/StatusBadge.js';
import {GovernanceOperationReview} from '../dist/test-source/features/governance/GovernanceOperationReview.js';
import {setLocale} from '../dist/test-source/shared/i18n.js';

function luminance(hex){const channel=value=>{const s=value/255;return s<=.04045?s/12.92:((s+.055)/1.055)**2.4;};return [.2126,.7152,.0722].reduce((sum,weight,index)=>sum+weight*channel(parseInt(hex.slice(1+index*2,3+index*2),16)),0);}
function contrast(a,b){const x=luminance(a),y=luminance(b);return (Math.max(x,y)+.05)/(Math.min(x,y)+.05);}
test('small semantic text and action states meet 4.5 contrast in both declared palettes',()=>{
  const css=fs.readFileSync(new URL('../src/app/kafdeck-theme.css',import.meta.url),'utf8');
  const variables=selector=>Object.fromEntries([...css.matchAll(selector)].flatMap(match=>[...match[1].matchAll(/--kafdeck-([a-z-]+):\s*(#[a-f0-9]{6})/gi)].map(pair=>[pair[1],pair[2]])));
  const light=variables(/:root\s*\{([\s\S]*?)\}/g),dark={...light,...variables(/\[data-bs-theme="dark"\]\s*\{([\s\S]*?)\}/g)};
  for(const [theme,tokens] of Object.entries({light,dark})){
    for(const family of ['warning','kbd','orange','secondary','blue','red','yellow','green'])assert.ok(contrast(tokens[family+'-text'],tokens[family+'-surface'])>=4.5,theme+': '+family);
    for(const family of ['primary-action','warning-action','danger-action','danger-solid'])assert.ok(contrast(tokens[family],tokens[family+'-on'])>=4.5,theme+': '+family);
    for(const surface of theme==='light'?['#ffffff','#fafafa']:['#1d273b','#242d3e']){
      for(const family of ['primary-action','warning-action','danger-action','table-link','table-link-hover'])assert.ok(contrast(tokens[family],surface)>=4.5,theme+': '+family+'/'+surface);
      assert.ok(contrast(tokens['control-edge'],surface)>=3,theme+': enabled entry/select boundary and indicator/'+surface);
    }
  }
});

test('brand uses natural visible title and subtitle with a redundant decorative logo in each locale',()=>{
  try{for(const locale of ['en','fa']){setLocale(locale,false);const html=renderToStaticMarkup(React.createElement(AppShell));const brand=html.match(/<div class="kafdeck-brand"[^>]*>/)?.[0];assert.ok(brand);assert.doesNotMatch(brand,/aria-label(?:ledby)?=/);assert.match(html,/class="kafdeck-brand-logo"[^>]*alt=""/);assert.match(html,/class="kafdeck-brand-name"[^>]*>Kafdeck<\/span>/);assert.match(html,locale==='en'?/class="kafdeck-brand-subtitle"[^>]*>Open Source Kafka Control Plane<\/span>/:/class="kafdeck-brand-subtitle"[^>]*>صفحه کنترل متن‌باز Kafka<\/span>/);}}
  finally{setLocale('en',false);}
});

test('status descriptions remain accessible plain text and overflow evidence is keyboard reachable',()=>{
  const badge=renderToStaticMarkup(React.createElement(StatusBadge,{kind:'denied',label:'Invariant.Denied'}));
  assert.doesNotMatch(badge,/aria-label=/);assert.match(badge,/Invariant\.Denied/);assert.match(badge,/class="visually-hidden"[^>]*>: The current operator or upstream policy denied access\./);
  const operation={operationId:'11111111-1111-4111-8111-111111111111',clusterId:'alpha',operationKind:'catalogMutation',riskClass:'moderate',state:'executionUnknown',previewHash:'a'.repeat(64),previewExpiresAtUtc:'2099-11-05T01:00:00Z',requiresIndependentApproval:false,requiresExecutionMaterial:false,confirmationMode:'explicit',confirmationChallenge:null,resultCode:null,safeProviderEvidence:{}};
  const html=renderToStaticMarkup(React.createElement(GovernanceOperationReview,{operation,material:{name:'<script>unsafe</script>'},busy:false,update(){},async execute(){throw Error('must not dispatch');},async run(){}}));
  const pre=[...html.matchAll(/<pre([^>]*)>/g)];assert.equal(pre.length,2);for(const match of pre)assert.match(match[1],/tabindex="0"/);assert.doesNotMatch(html,/<script\b/i);
});
