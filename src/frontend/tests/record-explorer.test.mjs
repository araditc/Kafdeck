import assert from 'node:assert/strict';
import test from 'node:test';
import {
  activeRecordPageQuery,
  withRecordProjectionPreference,
} from '../dist/test-source/features/records/RecordExplorer.js';

test('structured record view explicitly requests server-side decoding', () => {
  const base = { partition: 2, anchor: 'earliest', direction: 'forward', maxRecords: 100 };

  const schema = withRecordProjectionPreference(base, 'structured');
  assert.equal(schema.decode, true);
  assert.equal(schema.serdeFormat, undefined);

  const cbor = withRecordProjectionPreference(base, 'structured', 'cbor');
  assert.equal(cbor.decode, false);
  assert.equal(cbor.serdeFormat, 'cbor');

  const xml = withRecordProjectionPreference(base, 'structured', 'xml');
  assert.equal(xml.decode, false);
  assert.equal(xml.serdeFormat, 'xml');

  const messagePack = withRecordProjectionPreference(base, 'structured', 'messagePack');
  assert.equal(messagePack.decode, false);
  assert.equal(messagePack.serdeFormat, 'messagePack');

  assert.equal(withRecordProjectionPreference(base, 'text', 'cbor').decode, false);
  assert.equal(withRecordProjectionPreference(base, 'text', 'cbor').serdeFormat, undefined);
  assert.equal(withRecordProjectionPreference(base, 'hex', 'xml').decode, false);
  assert.equal(withRecordProjectionPreference(base, 'hex', 'xml').serdeFormat, undefined);
});

test('export and pagination retain the query that produced the displayed page', () => {
  const base = { partition: 2, offset: 10, direction: 'forward', maxRecords: 100, decode: true };
  const displayed = { partition: 2, offset: 210, direction: 'forward', maxRecords: 100, decode: true };

  assert.equal(activeRecordPageQuery(displayed, base), displayed);
  assert.equal(activeRecordPageQuery(null, base), base);
});
