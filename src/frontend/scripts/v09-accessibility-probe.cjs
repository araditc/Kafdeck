'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');

// All application inputs use the browser keyboard. Locators identify the intended
// target and observe focus; they never focus, click, fill or select a control.
module.exports=function accessibilityProbe(root,result){
  result.keyboard={inputMethod:'Playwright keyboard: Tab/Shift+Tab, Alt+ArrowDown, Enter, Space, Home, ArrowDown and typed text',actions:[]};
  result.automatedAccessibility={engine:'axe-core',runs:[]};
  let currentPage;
  async function safeFocus(page){return page.evaluate(()=>{
    const element=document.activeElement,knownIds=new Set(['cluster-selector','kafdeck-language','kafdeck-main','kafdeck-command-query','ownership-catalog-title','service-requests-title','service-request-heading']);
    return {documentHasFocus:document.hasFocus(),visibility:document.visibilityState,tag:element?.tagName??null,id:element?.id?(knownIds.has(element.id)?element.id:'nonregistered-id'):null,type:element?.getAttribute('type')??null,inertAncestor:!!element?.closest('[inert]'),dialogAncestor:!!element?.closest('[role="dialog"]')};
  });}
  async function diagnose(){if(!result.keyboard.current)return {unavailable:true};try{result.keyboard.current.active=await safeFocus(currentPage);}catch{result.keyboard.current.active={unavailable:true};}return result.keyboard.current;}
  async function tabTo(page,target,label){
    currentPage=page;
    result.keyboard.current={label,stage:'target-count',matchCount:await target.count(),tabs:0,focusTrail:[]};
    result.keyboard.current.stage='target-visible';
    await target.waitFor({state:'visible'});
    result.keyboard.current.matchCount=await target.count();
    assert.equal(result.keyboard.current.matchCount,1,'ambiguous keyboard target');
    result.keyboard.current.target=await target.evaluate(element=>({tag:element.tagName,type:element.getAttribute('type'),tabIndex:element.tabIndex,inertAncestor:!!element.closest('[inert]'),dialogAncestor:!!element.closest('[role="dialog"]')}));
    result.keyboard.current.stage='target-visible';
    await target.waitFor({state:'visible'});
    result.keyboard.current.stage='target-enabled';
    const deadline=Date.now()+30000;while(!await target.isEnabled()){if(Date.now()>deadline)throw Error('disabled keyboard target');await new Promise(resolve=>setTimeout(resolve,25));}
    result.keyboard.current.stage='keyboard-navigation';result.keyboard.current.initialActive=await safeFocus(page);
    let count=0;
    while(!await target.evaluate(element=>element===document.activeElement)){
      if(++count>1500)throw Error('keyboard target unreachable');
      await page.keyboard.press('Tab');
      result.keyboard.current.tabs=count;
      if(count<=12||count%25===0){result.keyboard.current.focusTrail.push({tab:count,...await safeFocus(page)});result.keyboard.current.focusTrail=result.keyboard.current.focusTrail.slice(-20);}
    }
    result.keyboard.current.stage='keyboard-reverse-sequence';
    // The first skip link is the document entry boundary. Shift+Tab there
    // legitimately leaves the document for browser chrome, unlike internal controls.
    const documentEntry=await target.evaluate(element=>element.matches('a[href="#kafdeck-main"]'));
    if(!documentEntry){
      await page.keyboard.press('Shift+Tab');await page.keyboard.press('Tab');
      assert.equal(await target.evaluate(element=>element===document.activeElement),true,'reverse keyboard sequence failed');
    }
    // Tab can initiate native smooth scrolling and CSS focus transitions. Observe
    // their completion; never scroll/focus the element programmatically to pass.
    result.keyboard.current.stage='keyboard-focus-settle';result.keyboard.current.settleSamples=[];
    let focus,previous='',stableSamples=0;const settleStarted=Date.now(),settleDeadline=settleStarted+2000;
    do{
      focus=await target.evaluate(element=>{
        const style=getComputedStyle(element),r=element.getBoundingClientRect(),hit=document.elementFromPoint(r.left+r.width/2,r.top+r.height/2);
        return {tag:element.tagName,type:element.getAttribute('type'),outlineStyle:style.outlineStyle,outlineWidth:style.outlineWidth,outlineColor:style.outlineColor,boxShadow:style.boxShadow,
          rect:{x:r.x,y:r.y,width:r.width,height:r.height,right:r.right,bottom:r.bottom},viewport:{width:innerWidth,height:innerHeight,scrollX,scrollY},topmostTag:hit?.tagName??null,topmostType:hit?.getAttribute('type')??null,
          centerExposed:hit===element||element.contains(hit),visible:r.width>0&&r.height>0&&r.top>=0&&r.left>=0&&r.bottom<=innerHeight+1&&r.right<=innerWidth+1};
      });
      result.keyboard.current.settleSamples.push({elapsedMs:Date.now()-settleStarted,rect:focus.rect,topmostTag:focus.topmostTag,visible:focus.visible,centerExposed:focus.centerExposed});result.keyboard.current.settleSamples=result.keyboard.current.settleSamples.slice(-8);
      const signature=JSON.stringify([focus.rect,focus.outlineColor,focus.boxShadow]);stableSamples=signature===previous?stableSamples+1:0;previous=signature;
      if(focus.visible&&focus.centerExposed&&stableSamples>=2)break;
      await new Promise(resolve=>setTimeout(resolve,40));
    }while(Date.now()<settleDeadline);
    focus.settleElapsedMs=Date.now()-settleStarted;focus.stableSamples=stableSamples;
    result.keyboard.current.focus=focus;result.keyboard.current.stage='keyboard-focus-viewport';assert.equal(focus.visible,true,'focused control clipped or outside viewport');
    result.keyboard.current.stage='keyboard-focus-obscured';
    assert.equal(focus.centerExposed,true,'focused control obscured at its center');
    result.keyboard.current.stage='keyboard-focus-indicator';
    assert.ok((focus.outlineStyle!=='none'&&parseFloat(focus.outlineWidth)>0)||focus.boxShadow!=='none','no computed focus indicator');
    result.keyboard.current.stage='keyboard-focus-stable';assert.ok(stableSamples>=2,'keyboard focus geometry or style did not settle');
    result.keyboard.actions.push({label,tabs:count,reverseSequenceVerified:!documentEntry,firstDocumentEntry:documentEntry,focus});
    result.keyboard.current.stage='keyboard-action-ready';
  }
  async function activate(page,target,label){await tabTo(page,target,label);await page.keyboard.press('Enter');}
  async function text(page,target,value,label){await tabTo(page,target,label);await page.keyboard.press('ControlOrMeta+A');await page.keyboard.type(value);assert.equal(await target.inputValue(),value);}
  async function select(page,target,value,label){
    await tabTo(page,target,label);
    const index=await target.locator('option').evaluateAll((options,value)=>options.findIndex(option=>option.value===value),value);
    // Explicitly open first: Enter on a collapsed Chromium select opens a popup
    // even after Home/ArrowDown already changed its value, consuming later Tab.
    assert.ok(index>=0);await page.keyboard.press('Alt+ArrowDown');await page.keyboard.press('Home');for(let i=0;i<index;i++)await page.keyboard.press('ArrowDown');await page.keyboard.press('Enter');assert.equal(await target.inputValue(),value);
  }
  async function check(page,target,label){await tabTo(page,target,label);if(!await target.isChecked())await page.keyboard.press('Space');assert.equal(await target.isChecked(),true);}
  async function scan(page,label,evidencePath){
    // Local source only. Never load a remote CDN or relax the application's CSP.
    const axePath=require.resolve('axe-core/axe.min.js',{paths:[path.join(root,'deploy/dev/browser-g2')]});
    await page.addScriptTag({content:fs.readFileSync(axePath,'utf8')});
    const scan=await page.evaluate(async()=>{
      const output=await window.axe.run(document,{runOnly:{type:'tag',values:['wcag2a','wcag2aa','wcag21a','wcag21aa','wcag22aa']}});
      const safeRule=rule=>({id:rule.id,impact:rule.impact,helpUrl:rule.helpUrl,nodes:rule.nodes.map(node=>({target:node.target,failureSummary:node.failureSummary}))});
      // Read only actual incomplete-node style bindings after axe completes.
      // Never collect text, values, attributes, image URLs or API material.
      const canvas=document.createElement('canvas');canvas.width=1;canvas.height=1;
      const context=canvas.getContext('2d',{willReadFrequently:true,colorSpace:'srgb'});
      const color=value=>{
        let rgba=null;
        if(context&&CSS.supports('color',value)){
          context.clearRect(0,0,1,1);context.fillStyle=value;context.fillRect(0,0,1,1);
          const pixel=context.getImageData(0,0,1,1).data;rgba=[pixel[0],pixel[1],pixel[2],pixel[3]/255];
        }
        return {computed:value,rgba};
      };
      const incompleteColorBindings=output.incomplete.filter(rule=>rule.id==='color-contrast').flatMap(rule=>rule.nodes.map(node=>{
        let element=null,scope=document;
        try{for(const selector of node.target){if(typeof selector!=='string'){element=null;break;}element=scope.querySelector(selector);if(!element)break;scope=element.shadowRoot||element;}}catch{element=null;}
        if(!element)return {ruleId:rule.id,target:node.target,resolved:false,measuredAfterAxeRun:true};
        const style=getComputedStyle(element),backgroundChain=[];
        for(let ancestor=element;ancestor&&backgroundChain.length<16;ancestor=ancestor.parentElement){
          const binding=getComputedStyle(ancestor);
          backgroundChain.push({tag:ancestor.tagName,background:color(binding.backgroundColor),backgroundImagePresent:binding.backgroundImage!=='none',opacity:binding.opacity});
        }
        return {ruleId:rule.id,target:node.target,resolved:true,measuredAfterAxeRun:true,rgbaConversion:'unattached 1px sRGB canvas',
          tag:element.tagName,disabled:element.matches(':disabled'),foreground:color(style.color),background:color(style.backgroundColor),opacity:style.opacity,
          borderColors:{blockStart:color(style.borderBlockStartColor),blockEnd:color(style.borderBlockEndColor),inlineStart:color(style.borderInlineStartColor),inlineEnd:color(style.borderInlineEndColor)},
          backgroundImagePresent:style.backgroundImage!=='none',backgroundPosition:style.backgroundPosition,backgroundSize:style.backgroundSize,
          padding:{blockStart:style.paddingBlockStart,blockEnd:style.paddingBlockEnd,inlineStart:style.paddingInlineStart,inlineEnd:style.paddingInlineEnd},backgroundChain};
      }));
      return {version:output.testEngine.version,violations:output.violations.map(safeRule),incomplete:output.incomplete.map(safeRule),incompleteColorBindings,passes:output.passes.length,inapplicable:output.inapplicable.length};
    });
    const layout=await page.evaluate(()=>({language:document.documentElement.lang,direction:document.documentElement.dir,viewport:innerWidth,documentWidth:document.documentElement.scrollWidth}));
    const screenshot=path.basename(evidencePath).replace(/\.json$/,'')+'-'+label+'.png';
    await page.screenshot({path:path.join(path.dirname(evidencePath),screenshot),fullPage:true});
    result.automatedAccessibility.runs.push({label,...layout,...scan,screenshot});
    assert.equal(scan.violations.length,0,'axe violation');assert.ok(layout.documentWidth<=layout.viewport+1,'document horizontal overflow');
  }
  async function preferences(page,locale,evidencePath){
    result.browserPreferenceEmulation??=[];
    const originalViewport=page.viewportSize();let spacingStyle;
    try{
      for(const preference of ['reduced-motion','forced-colors','text-spacing']){
        await page.emulateMedia({reducedMotion:preference==='reduced-motion'?'reduce':null,forcedColors:preference==='forced-colors'?'active':null});
        if(preference==='text-spacing')spacingStyle=await page.addStyleTag({content:'* { line-height: 1.5 !important; letter-spacing: .12em !important; word-spacing: .16em !important; } p { margin-block-end: 2em !important; }'});
        for(const width of [1440,320]){
          await page.setViewportSize({width,height:width===320?800:1000});
          await tabTo(page,page.locator('#kafdeck-language'),locale+'-'+preference+'-'+width+'-focus');
          const actual=await page.evaluate(()=>{
            const paragraph=document.querySelector('main p'),style=paragraph?getComputedStyle(paragraph):null,size=style?parseFloat(style.fontSize):0;
            const durationMs=value=>value.split(',').map(part=>parseFloat(part)*(part.trim().endsWith('ms')?1:1000));
            return {reducedMotion:matchMedia('(prefers-reduced-motion: reduce)').matches,forcedColors:matchMedia('(forced-colors: active)').matches,
              paragraph:style?{lineHeightRatio:parseFloat(style.lineHeight)/size,letterSpacingEm:parseFloat(style.letterSpacing)/size,wordSpacingEm:parseFloat(style.wordSpacing)/size,paragraphEndMarginEm:parseFloat(style.marginBlockEnd)/size}:null,
              maximumButtonTransitionMs:Math.max(0,...[...document.querySelectorAll('button')].slice(0,100).flatMap(button=>durationMs(getComputedStyle(button).transitionDuration)))};
          });
          result.browserPreferenceEmulation.push({locale,preference,width,provenance:'Chromium per-page media emulation / injected user text-spacing override; no Windows settings changed',...actual});
          if(preference==='reduced-motion'){assert.equal(actual.reducedMotion,true);assert.ok(actual.maximumButtonTransitionMs<=.011,'reduced-motion transition remains active');}
          if(preference==='forced-colors')assert.equal(actual.forcedColors,true);
          if(preference==='text-spacing'){assert.ok(actual.paragraph);assert.ok(actual.paragraph.lineHeightRatio>=1.499);assert.ok(actual.paragraph.letterSpacingEm>=.119);assert.ok(actual.paragraph.wordSpacingEm>=.159);assert.ok(actual.paragraph.paragraphEndMarginEm>=1.999);}
          await scan(page,locale+'-'+preference+'-'+width,evidencePath);
        }
        if(spacingStyle){await spacingStyle.evaluate(element=>element.remove());spacingStyle=null;}
      }
    }finally{
      if(spacingStyle)await spacingStyle.evaluate(element=>element.remove());
      await page.emulateMedia({reducedMotion:null,forcedColors:null});if(originalViewport)await page.setViewportSize(originalViewport);
    }
  }
  return {tabTo,activate,text,select,check,scan,preferences,diagnose,getCurrentPage:()=>currentPage};
};
