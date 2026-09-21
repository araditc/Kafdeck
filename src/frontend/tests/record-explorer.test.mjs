import assert from 'node:assert/strict';
import test from 'node:test';
import {
  activeRecordPageQuery,
  withRecordProjectionPreference,
} from '../dist/test-source/features/records/RecordExplorer.js';

test('structured record view explicitly requests server-side decoding', () => {
  const base = { partition: 2, anchor: 'earliest', direction: 'forward', maxRecords: 100 };

  assert.equal(withRecordProjectionPreference(base, 'structured').decode, true);
  assert.equal(withRecordProjectionPreference(base, 'text').decode, false);
  assert.equal(withRecordProjectionPreference(base, 'hex').decode, false);
});

test('export and pagination retain the query that produced the displayed page', () => {
  const base = { partition: 2, offset: 10, direction: 'forward', maxRecords: 100, decode: true };
  const displayed = { partition: 2, offset: 210, direction: 'forward', maxRecords: 100, decode: true };

  assert.equal(activeRecordPageQuery(displayed, base), displayed);
  assert.equal(activeRecordPageQuery(null, base), base);
});
