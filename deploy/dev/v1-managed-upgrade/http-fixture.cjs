'use strict';
// Explicit controlled HTTP providers for UI/reference credential recovery only.
// This is not evidence of genuine Schema Registry / Connect / ksqlDB execution.
const http=require('node:http'),fs=require('node:fs'),path=require('node:path');
const lab=path.resolve(process.argv[2]||'');if(!fs.existsSync(path.join(lab,'services-private.json')))throw Error('Ready owned fixture required.');
const subjects=Array.from({length:20},(_,i)=>'g06-reference-'+String(i).padStart(2,'0')+'-value');
const schema=JSON.stringify({type:'record',name:'Reference',fields:[{name:'message',type:'string'}]});
const expected='Basic '+Buffer.from('g06-reference:g06-credential-fixture').toString('base64');
const counts={authorized:0,denied:0};
const server=http.createServer(async(req,res)=>{
 const url=new URL(req.url,'http://127.0.0.1:19084'),p=url.pathname;
 const send=(value,status=200)=>{res.writeHead(status,{'Content-Type':'application/json'});res.end(JSON.stringify(value));};
 if(req.headers.authorization!==expected){counts.denied++;return send({error_code:401,message:'Reference credential required'},401);}counts.authorized++;
 if(p==='/__reference-stats')return send(counts);
 if(p==='/subjects')return send(subjects);
 if(/^\/subjects\/[^/]+\/versions$/.test(p))return send([1]);
 if(/^\/subjects\/[^/]+\/versions\/(1|latest)$/.test(p))return send({subject:decodeURIComponent(p.split('/')[2]),version:1,id:1,schemaType:'AVRO',schema,references:[]});
 if(p.startsWith('/schemas/ids/'))return send({schemaType:'AVRO',schema,references:[]});
 if(p.startsWith('/config'))return send({compatibilityLevel:'BACKWARD'});
 if(p==='/')return send({version:'controlled-reference',commit:'fixture',kafka_cluster_id:'reference'});
 if(p==='/connectors')return send(['g06-reference-source','g06-reference-sink']);
 if(p==='/connector-plugins')return send([{class:'org.apache.kafka.connect.file.FileStreamSourceConnector',type:'source',version:'fixture'}]);
 if(/^\/connectors\/[^/]+\/status$/.test(p))return send({name:p.split('/')[2],connector:{state:'RUNNING',worker_id:'127.0.0.1:19084'},tasks:[{id:0,state:'RUNNING',worker_id:'127.0.0.1:19084'}],type:'source'});
 if(/^\/connectors\/[^/]+\/config$/.test(p))return send({'connector.class':'org.apache.kafka.connect.file.FileStreamSourceConnector','tasks.max':'1',topic:'g06-reference-00'});
 if(p==='/info')return send({KsqlServerInfo:{version:'0.28.2-reference',kafkaClusterId:'controlled-reference',ksqlServiceId:'g06-fixture'}});
 if(p==='/ksql'){
  let text='';for await(const chunk of req)text+=chunk;const body=JSON.parse(text);
  if(body.ksql?.toUpperCase().includes('SHOW TABLES'))return send([{tables:[{name:'G06_REFERENCE_TABLE',topic:'g06-reference-01',keyFormat:'KAFKA',valueFormat:'JSON',isWindowed:false}]}]);
  return send([{streams:[{name:'G06_REFERENCE_STREAM',topic:'g06-reference-00',keyFormat:'KAFKA',valueFormat:'JSON'}]}]);
 }
 if(p==='/query-stream'){
  res.writeHead(200,{'Content-Type':'application/vnd.ksqlapi.delimited.v1'});res.end(JSON.stringify({queryId:'g06-reference',columnNames:['MESSAGE'],columnTypes:['STRING']})+'\n'+JSON.stringify(['g06-e01-reference'])+'\n');return;
 }
 send({error_code:404,message:'Unsupported reference fixture path'},404);
});
server.listen(19084,'127.0.0.1',()=>console.log('Controlled HTTP reference providers ready on numeric loopback.'));
const timer=setInterval(()=>{if(fs.existsSync(path.join(lab,'stop-http'))){clearInterval(timer);server.closeAllConnections();server.close();}},1000);
