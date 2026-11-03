#!/usr/bin/env node
import assert from 'node:assert/strict';
import {spawn, execFileSync} from 'node:child_process';
import {existsSync, mkdtempSync, openSync, closeSync, readFileSync, writeFileSync} from 'node:fs';
import {join, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const helper=fileURLToPath(new URL('./pstack_verify.py',import.meta.url));
const run=process.argv[2]&&resolve(process.argv[2]);
if(!run)throw new Error('Usage: node scripts/pstack_web.mjs <owned-run-directory>');
const state=JSON.parse(readFileSync(join(run,'pstack-run.json'),'utf8'));
const doctor=()=>JSON.parse(execFileSync(process.env.PSTACK_PYTHON||'python3',[helper,'doctor',run],{encoding:'utf8',timeout:20000}));
assert.equal(doctor().ok,true);
const scratch=join(run,'pstack-scratch');
const evidence=mkdtempSync(join(run,'pstack-evidence','pstack-browser-'));
const chrome=process.env.PSTACK_CHROME_BIN||[
 '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
 '/Applications/Chromium.app/Contents/MacOS/Chromium',
 '/usr/bin/google-chrome','/usr/bin/chromium','/usr/bin/chromium-browser',
].find(p=>existsSync(p));
if(!chrome)throw new Error('Chrome/Chromium not found; set PSTACK_CHROME_BIN. Browser coverage is BLOCKED, not replaced by API checks.');
const profile=mkdtempSync(join(scratch,'pstack-chrome-'));
const browserLog=openSync(join(evidence,'pstack-browser.log'),'w');
const browser=spawn(chrome,[
 '--headless=new','--remote-debugging-address=127.0.0.1','--remote-debugging-port=0',
 `--user-data-dir=${profile}`,'--no-first-run','--no-default-browser-check','--disable-sync',
 '--disable-background-networking','--disable-component-update','--disable-extensions',
 '--disable-default-apps','--metrics-recording-only','--password-store=basic','--use-mock-keychain',
 '--no-proxy-server','--window-size=1440,1100','--lang=en-US','about:blank',
],{stdio:['ignore',browserLog,browserLog],env:{PATH:process.env.PATH,HOME:join(scratch,'home'),LANG:'en_US.UTF-8'}});
closeSync(browserLog);
let spawnError;
browser.on('error',error=>{spawnError=error;});
const alive=()=>!spawnError&&browser.exitCode===null&&browser.signalCode===null;
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const errors=[],network=[],blocked=[],actions=[];
let socket,session,nextId=0,navigations=0;
const pending=new Map();
const send=(method,params={},sessionId=session)=>new Promise((resolve,reject)=>{
 const id=++nextId,timeout=setTimeout(()=>{pending.delete(id);reject(new Error(`CDP timed out: ${method}`));},15000);
 pending.set(id,{resolve,reject,timeout});
 socket.send(JSON.stringify({id,method,params,...(sessionId?{sessionId}:{})}));
});
const evaluate=async expression=>{
 const result=await send('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});
 if(result.exceptionDetails)throw new Error(result.exceptionDetails.exception?.description||result.exceptionDetails.text);
 return result.result.value;
};
const until=async(expression,label,condition=()=>true)=>{
 const end=Date.now()+20000;let last;
 while(Date.now()<end){
  try{last=await evaluate(expression);if(last&&condition())return last;}
  catch(error){if(!/context|navigat|interrupted/i.test(error.message))throw error;}
  await delay(100);
 }
 throw new Error(`UI condition failed: ${label}; last=${JSON.stringify(last)}`);
};
const snapshotExpression=`(()=>{
 const text=s=>document.querySelector(s)?.textContent.trim()||'';
 const number=s=>Number(text(s).replace(/[^0-9.-]/g,''));
 return {provider:document.querySelector('#usageProvider')?.value,
  preset:document.querySelector('#usageRangePreset')?.value,
  from:document.querySelector('#usageRangeFrom')?.value,to:document.querySelector('#usageRangeTo')?.value,
  tokens:number('#summaryTotalCard .value'),today:number('#summaryTodayCard .value'),
  spend:number('#summaryCostCard .value'),spendText:text('#summaryCostCard .value'),
  refresh:text('#autoRecalcStatus'),chart:text('#usageChartNote'),
  models:text('#usageBreakdownTableBody'),daily:text('#dailyUsageTableBody'),
  providerOptions:[...document.querySelectorAll('#usageProvider option')].map(o=>o.value)};
})()`;
const snapshot=()=>evaluate(snapshotExpression);
const readyExpression="document.readyState==='complete'&&document.querySelector('#autoRecalcStatus')?.textContent.startsWith('Auto refresh: next at')&&!document.querySelector('#lastRefreshTime')?.textContent.includes('--')&&!!document.querySelector('#usageProvider')";
const total=(provider='combined',from='',to='')=>{
 const records=state.records.filter(r=>(provider==='combined'||r.provider===provider)&&(!from||r.date>=from)&&(!to||r.date<=to));
 return {tokens:records.reduce((n,r)=>n+r.tokens,0),cost:Math.round(records.reduce((n,r)=>n+r.cost,0)*100)/100};
};
const choose=async(selector,value)=>{
 const before=await snapshot();
 await evaluate(`(()=>{const e=document.querySelector(${JSON.stringify(selector)});if(!e)throw Error('Missing user control');e.value=${JSON.stringify(value)};if(e.value!==${JSON.stringify(value)})throw Error('Option/date rejected');e.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 await until(readyExpression,'ready after control change');
 actions.push({control:selector,value,before,after:await snapshot()});
};
const capture=async name=>{
 const png=await send('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});
 writeFileSync(join(evidence,name),Buffer.from(png.data,'base64'));
};
let outcome;
try{
 let active;
 for(let i=0;i<150;i++){
  if(!alive())throw new Error(`Isolated Chrome exited: ${spawnError?.message||'inspect pstack-browser.log'}`);
  const path=join(profile,'DevToolsActivePort');
  if(existsSync(path)){active=readFileSync(path,'utf8').trim().split('\n');if(active.length>=2)break;}
  await delay(100);
 }
 assert(active&&/^\d+$/.test(active[0])&&active[1].startsWith('/devtools/browser/'),'Chrome debug endpoint unavailable');
 socket=new WebSocket(`ws://127.0.0.1:${active[0]}${active[1]}`);
 socket.addEventListener('message',event=>{
  const message=JSON.parse(event.data);
  if(message.id){const waiter=pending.get(message.id);if(waiter){pending.delete(message.id);clearTimeout(waiter.timeout);message.error?waiter.reject(new Error(message.error.message)):waiter.resolve(message.result);}return;}
  if(message.method==='Page.frameNavigated'&&!message.params.frame.parentId&&message.params.frame.url.startsWith(state.url+'/'))navigations++;
  if(message.method==='Runtime.exceptionThrown')errors.push(message.params.exceptionDetails.exception?.description||message.params.exceptionDetails.text);
  if(message.method==='Runtime.consoleAPICalled'&&message.params.type==='error')errors.push(message.params.args.map(a=>a.value||a.description).join(' '));
  if(message.method==='Network.responseReceived'&&new URL(message.params.response.url).pathname==='/recalc'){
   const response=message.params.response;
   network.push({url:response.url,status:response.status,serverTiming:Object.entries(response.headers).find(([k])=>k.toLowerCase()==='server-timing')?.[1]||null});
  }
  if(message.method==='Fetch.requestPaused'){
   const url=new URL(message.params.request.url);
   const allow=url.origin===new URL(state.url).origin||['data:','about:'].includes(url.protocol);
   if(!allow)blocked.push(url.origin);
   send(allow?'Fetch.continueRequest':'Fetch.failRequest',{requestId:message.params.requestId,...(!allow?{errorReason:'BlockedByClient'}:{})},message.sessionId).catch(()=>{});
  }
 });
 await new Promise((resolve,reject)=>{socket.addEventListener('open',resolve,{once:true});socket.addEventListener('error',reject,{once:true});});
 const {targetId}=await send('Target.createTarget',{url:'about:blank'},null);
 ({sessionId:session}=await send('Target.attachToTarget',{targetId,flatten:true},null));
 await send('Page.enable');await send('Runtime.enable');await send('Network.enable');
 await send('Fetch.enable',{patterns:[{urlPattern:'*'}]});
 await send('Emulation.setDeviceMetricsOverride',{width:1440,height:1100,deviceScaleFactor:1,mobile:false});
 await send('Page.navigate',{url:state.url+'/'});
 await until(readyExpression,'initial real recalc/reload',()=>network.length>0&&navigations>=2);
 await choose('#usageRangePreset','all');
 let view=await snapshot();assert.equal(view.provider,'combined');assert.equal(view.tokens,total().tokens);
 assert.deepEqual([...view.providerOptions].sort(),['claude','codex','combined','pi']);
 assert.equal(view.spend,total().cost);assert(!view.spendText.includes('*'));
 assert(view.models.includes('gpt-5.4')&&view.models.includes('claude-sonnet-4-6'));
 actions.push({step:'pstack-baseline',view});await capture('pstack-dashboard-combined.png');
 await choose('#usageProvider','pi');await choose('#usageRangePreset','all');
 view=await snapshot();assert.equal(view.tokens,total('pi').tokens);assert.equal(view.spend,total('pi').cost);
 assert(view.models.includes('gpt-5.4')&&!view.models.includes('claude-sonnet-4-6'));
 const oldNavigationCount=navigations,oldRecalcCount=network.length;
 await send('Page.reload');
 await until(`(${readyExpression})&&document.querySelector('#usageProvider')?.value==='pi'`,'provider persists through user reload',()=>navigations>=oldNavigationCount+2&&network.length>oldRecalcCount);
 view=await snapshot();assert.equal(view.tokens,total('pi').tokens);actions.push({step:'pstack-provider-persistence',view});
 await capture('pstack-dashboard-pi.png');
 await choose('#usageProvider','combined');await choose('#usageRangePreset','all');
 await choose('#usageRangeFrom',state.today);await choose('#usageRangeTo',state.today);
 view=await snapshot();assert.equal(view.from,state.today);assert.equal(view.to,state.today);
 assert.equal(view.tokens,total('combined',state.today,state.today).tokens);assert.equal(view.spend,total('combined',state.today,state.today).cost);
 actions.push({step:'pstack-current-day-range',view});
 await choose('#usageRangeFrom',state.gapDate);await choose('#usageRangeTo',state.gapDate);
 view=await snapshot();assert.equal(view.tokens,0);assert.equal(view.spend,0);actions.push({step:'pstack-empty-range',view});
 await evaluate("document.querySelector('#usageRangeClear').click()");
 view=await snapshot();assert.equal(view.preset,'all');assert.equal(view.tokens,total().tokens);
 actions.push({step:'pstack-reset',view});
 assert(network.length>0&&network.every(r=>r.status===200&&r.serverTiming),'Real browser refresh and timing evidence missing');
 assert.equal(errors.length,0,errors.join('\n'));assert.equal(doctor().ok,true);
 outcome={ok:true,origin:state.url,browser:(await send('Browser.getVersion',{},null)).product,actions,recalcResponses:network,blockedExternalOrigins:[...new Set(blocked)],runtimeErrors:errors};
}catch(error){
 const lastSnapshot=socket?.readyState===1?await snapshot().catch(()=>null):null;
 outcome={ok:false,error:error.stack,lastSnapshot,actions,recalcResponses:network,blockedExternalOrigins:[...new Set(blocked)],runtimeErrors:errors};
 process.exitCode=1;
}finally{
 if(socket?.readyState===1){try{await send('Browser.close',{},null);}catch{}}
 if(alive()){await Promise.race([new Promise(resolve=>browser.once('exit',resolve)),delay(3000)]);}
 if(alive())browser.kill('SIGTERM');
 if(alive())await Promise.race([new Promise(resolve=>browser.once('exit',resolve)),delay(2000)]);
 if(alive()){browser.kill('SIGKILL');await new Promise(resolve=>browser.once('exit',resolve));}
 for(const waiter of pending.values()){clearTimeout(waiter.timeout);waiter.reject(new Error('Owned browser closed'));}pending.clear();
 socket?.close();
 outcome={...outcome,ownedBrowserExited:!alive()};
 writeFileSync(join(evidence,'pstack-browser-results.json'),JSON.stringify(outcome,null,2)+'\n');
 console.log(JSON.stringify({ok:outcome.ok,actions:actions.length,recalcResponses:network.length,ownedBrowserExited:outcome.ownedBrowserExited,evidence:join(evidence,'pstack-browser-results.json'),error:outcome.error},null,2));
}
