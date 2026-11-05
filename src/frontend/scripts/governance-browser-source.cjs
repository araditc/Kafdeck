'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),{execFileSync}=require('node:child_process');
module.exports=function(root){
  const git=(args)=>execFileSync('git',['-c','safe.directory='+root,...args],{cwd:root,encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();
  const assets=path.join(root,'src/frontend/dist/assets');
  return {sourceSha:git(['rev-parse','HEAD']),sourceWorktreeDirty:git(['status','--porcelain','--untracked-files=normal','--','src','tests'])!=='',
    compiledAssets:fs.readdirSync(assets).filter(file=>/\.(?:js|css)$/.test(file)).sort().map(file=>({file,sha256:crypto.createHash('sha256').update(fs.readFileSync(path.join(assets,file))).digest('hex')}))};
};
