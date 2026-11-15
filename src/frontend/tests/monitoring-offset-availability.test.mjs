import test from 'node:test';
import assert from 'node:assert/strict';
import {independentOffsetRates,comparableConsumerOffsets} from '../dist/test-source/features/monitoring/offsetRates.js';

test('end growth remains measurable before a consumer commits, without inventing committed growth',()=>{
  const p={topic:'orders',partition:0,committedOffset:null,endOffset:0,lag:null};
  const lag={groupId:'default-group',isPartial:true,partitions:[p],limitations:[{code:'consumer_offset_missingcommittedoffset'}]};
  const a={at:1000,lag},b={at:11000,lag:{...lag,partitions:[{...p,endOffset:5}]}};
  assert.equal(comparableConsumerOffsets(lag,true),true);
  assert.deepEqual(independentOffsetRates(a,b),{end:0.5,committed:null});
  assert.deepEqual(independentOffsetRates(a,{...b,lag}),{end:0,committed:null});
  assert.equal(independentOffsetRates(a,{...b,lag:{...lag,partitions:[{...p,partition:1}]}}),null);
  assert.equal(comparableConsumerOffsets({...lag,limitations:[]},true),false);
});
test('authorization gaps, reset offsets and changed partition coverage cannot produce false rates',()=>{
  const p={topic:'orders',partition:0,committedOffset:4,endOffset:9};
  const lag={groupId:'workers',isPartial:false,partitions:[p],limitations:[]},a={at:1000,lag};
  const b={at:11000,lag:{...lag,partitions:[{...p,endOffset:3,committedOffset:2}]}};
  assert.deepEqual(independentOffsetRates(a,b),{end:null,committed:null});
  assert.equal(independentOffsetRates(a,{...b,lag:{...b.lag,isPartial:true,limitations:[{code:'consumer_offset_unauthorized'}]}}),null);
  assert.equal(independentOffsetRates(a,{...b,at:100000}),null);
});
