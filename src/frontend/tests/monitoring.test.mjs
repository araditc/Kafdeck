import test from 'node:test';
import assert from 'node:assert/strict';
import {parseMeasurements} from '../dist/test-source/features/monitoring/measurements.js';
import {offsetRates} from '../dist/test-source/features/monitoring/offsetRates.js';

test('client latency needs actual samples and converts microseconds to milliseconds per broker',()=>{
  const capture={name:'producer-a',type:'producer',time:1710000000,replyq:0,brokers:{
    a:{nodeid:1,rtt:{cnt:2,avg:2100,p99:3700}},b:{nodeid:2,rtt:{cnt:0,avg:0,p99:0}}}};
  const series=parseMeasurements(JSON.stringify(capture));
  assert.deepEqual(series.find(s=>s.metric==='rttP99'),{source:'producer-a / broker 1',metric:'rttP99',points:[{time:1710000000000,value:3.7}]});
  assert.ok(!series.some(s=>s.source.endsWith('broker 2')));
});
test('workload import keeps sample identity, accepts zero, rejects ambiguous or duplicate samples',()=>{
  const sample={source:'writer-1',metric:'queueWaitP95',time:'2026-01-01T00:00:00Z',value:0};
  const raw=JSON.stringify(sample);
  assert.equal(parseMeasurements(raw)[0].points[0].value,0);
  assert.throws(()=>parseMeasurements(raw+'\n'+raw),ex=>ex.kind==='duplicate');
  assert.throws(()=>parseMeasurements(JSON.stringify({...sample,time:'2026-01-01T00:00:00'})),ex=>ex.kind==='format');
});
test('rate requires comparable offsets on exactly the same partitions across adjacent complete observations',()=>{
  const p={topic:'orders',partition:0,committedOffset:4,endOffset:8};
  const a={at:1000,lag:{groupId:'workers',isPartial:false,partitions:[p]}};
  const b={at:11000,lag:{...a.lag,partitions:[{...p,committedOffset:6,endOffset:14}]}};
  assert.deepEqual(offsetRates(a,b),{committed:0.2,end:0.6});
  assert.equal(offsetRates(a,{...b,at:101000}),null);
  for(const change of [{groupId:'different'},{isPartial:true},{partitions:[{...p,committedOffset:1}]},{partitions:[{...p,partition:1}]}])
    assert.equal(offsetRates(a,{...b,lag:{...b.lag,...change}}),null);
});
