import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { ReadViewsExplorer } from '../dist/test-source/features/readviews/ReadViewsExplorer.js';

test('v0.4 read-view UI exposes Consumers, Schemas and Ecosystem landmarks', () => {
  const markup = renderToStaticMarkup(
    React.createElement(ReadViewsExplorer, { clusterId: 'prod' }),
  );

  assert.match(markup, /id="consumers"/);
  assert.match(markup, /Consumer groups/);
  assert.match(markup, /id="schemas"/);
  assert.match(markup, /Schema Registry/);
  assert.match(markup, /id="ecosystem"/);
  assert.match(markup, /Ecosystem read views/);
  assert.match(markup, /bounded single-statement SELECT queries only/);
  assert.match(markup, /DDL, DML, persistent-query creation/);
  assert.doesNotMatch(markup, /generic provider forwarding is allowed/i);
});

test('v0.4 initial UI has no mutation controls', () => {
  const markup = renderToStaticMarkup(
    React.createElement(ReadViewsExplorer, { clusterId: 'prod' }),
  );

  for (const forbidden of [
    'Reset offsets',
    'Commit offsets',
    'Restart connector',
    'Pause connector',
    'Resume connector',
    'Delete connector',
    'Create schema',
    'Delete schema',
    'SQL editor',
    'Execute SQL',
  ]) {
    assert.doesNotMatch(markup, new RegExp(forbidden, 'i'));
  }
});

test('v0.7 Connect view starts from the bounded profile catalog instead of assuming one worker', () => {
  const markup = renderToStaticMarkup(
    React.createElement(ReadViewsExplorer, { clusterId: 'prod' }),
  );

  assert.match(markup, /Loading configured Connect profiles/);
  assert.doesNotMatch(markup, /provider URL/i);
  assert.doesNotMatch(markup, /HTTP method/i);
});


test('v0.7 Connect profile switch clears request-scoped plugin material', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );
  const start = source.indexOf('const openConnectProfile = async');
  const end = source.indexOf('const openConnector = async', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);

  const profileSwitch = source.slice(start, end);
  assert.match(profileSwitch, /setPluginConfiguration\(''\)/);
  assert.match(profileSwitch, /setPluginFieldValues\(\{\}\)/);
  assert.match(profileSwitch, /setPluginValidation\(null\)/);
});


test('v0.7 W55 Connect auto-restart UI remains governed and bounded', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /Bounded auto-restart/);
  assert.match(source, /previewConnectAutoRestartPolicy/);
  assert.match(source, /applyConnectAutoRestartPolicy/);
  assert.match(source, /Governed operation/);
  assert.match(source, /autoRestartOperation\.state !== 'ready'/);
  assert.doesNotMatch(source, /fetch\([^\n]*restart/i);
});


test('v0.7 W55 policy lookup cannot break read-only Connect detail when mutation mode is absent', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );
  const start = source.indexOf('const openConnector = async');
  const end = source.indexOf('const previewAutoRestart = async', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);

  const openConnector = source.slice(start, end);
  assert.match(openConnector, /getConnectProfileConnector/);
  assert.match(openConnector, /getConnectAutoRestartPolicy/);
  assert.match(openConnector, /reason instanceof MutationApiProblem && reason\.status === 404/);
  assert.doesNotMatch(openConnector, /Promise\.all/);
});


test('v0.7 W56 controlled SerDe UI is bounded, local-only and clears request material', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /Controlled SerDe tooling/);
  assert.match(source, /CBOR, XML and MessagePack/);
  assert.match(source, /decodeControlledSerde/);
  assert.match(source, /encodeControlledSerde/);
  assert.match(source, /setSerdePayloadBase64\(''\)/);
  assert.match(source, /setSerdeStructuredJson\(''\)/);
  assert.doesNotMatch(source, /provider URL/i);
  assert.doesNotMatch(source, /import\([^)]*plugin/i);
  assert.doesNotMatch(source, /script engine/i);
});


test('v0.7 Connect profile loads ignore stale responses after profile switches', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /const connectLoadGeneration = useRef\(0\)/);
  assert.match(source, /const generation = \+\+connectLoadGeneration\.current/);
  assert.match(source, /if \(generation !== connectLoadGeneration\.current\) return/);
  assert.match(source, /if \(generation === connectLoadGeneration\.current\)/);
});

test('v0.7 initial Connect profile rejection is generation-fenced', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  const start = source.indexOf('void kafdeckApi.listConnectProfiles');
  const end = source.indexOf('void kafdeckApi.getKsqlInfo', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);

  const initialLoad = source.slice(start, end);
  assert.match(
    initialLoad,
    /initialConnectGeneration === connectLoadGeneration\.current/,
  );
  assert.match(
    initialLoad,
    /setConnectError\(readViewError\(reason\)\)/,
  );
});


test('v0.8 W64 consumer analytics UI is bounded, truth-preserving and operator-triggered', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /Operational analytics/);
  assert.match(source, /getConsumerOperationalAnalytics/);
  assert.match(source, /getConsumerOperationalTrend\(clusterId, groupId\)/);
  assert.match(source, /points\.slice\(-100\)/);
  assert.match(source, /Operator-triggered lag SLO/);
  assert.match(source, /getConsumerOperationalSlo/);
  assert.match(source, /Evaluate bounded SLO/);
  assert.match(source, /Missing provider evidence remains unavailable/);
  assert.match(source, /item\.value \?\? 'Unavailable'/);
  assert.doesNotMatch(source, /setInterval\(/);
  assert.doesNotMatch(source, /WebSocket/);
  assert.doesNotMatch(source, /mutate Kafka/i);
});

test('v0.8 W64 opening a consumer cannot lose core read detail if analytics fails', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );
  const start = source.indexOf('const openConsumer = async');
  const end = source.indexOf('const evaluateConsumerSlo = async', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);

  const openConsumer = source.slice(start, end);
  assert.match(openConsumer, /setGroupDetail\(detail\); setGroupLag\(lag\); setGroupDiagnostics\(diagnostics\)/);
  assert.match(openConsumer, /setOperationalError\(readViewError\(reason\)\)/);
  assert.match(openConsumer, /setOperationalBusy\(false\)/);
});


test('v0.8 W64 consumer analytics responses are selection-generation fenced', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /const consumerLoadGeneration = useRef\(0\)/);
  assert.match(source, /const generation = \+\+consumerLoadGeneration\.current/);
  assert.match(source, /if \(generation !== consumerLoadGeneration\.current\) return/);
  assert.match(source, /if \(generation === consumerLoadGeneration\.current\)/);

  const start = source.indexOf('const openConsumer = async');
  const end = source.indexOf('const evaluateConsumerSlo = async', start);
  assert.notEqual(start, -1);
  assert.notEqual(end, -1);
  const openConsumer = source.slice(start, end);

  assert.match(openConsumer, /setOperationalAnalytics\(live\)/);
  assert.match(openConsumer, /setOperationalTrend\(trend\)/);
  assert.match(openConsumer, /generation !== consumerLoadGeneration\.current/);
});

test('v0.8 W64 SLO result stays bound to immutable evaluated inputs', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /\+\+sloEvaluationGeneration\.current; setSloThreshold\(Number\(event\.target\.value\)\); setOperationalSlo\(null\)/);
  assert.match(source, /\+\+sloEvaluationGeneration\.current; setSloTarget\(Number\(event\.target\.value\)\); setOperationalSlo\(null\)/);
  assert.match(source, /operationalSlo\.definition\.maximumGoodValue/);
  assert.match(source, /operationalSlo\.definition\.targetFraction/);
  assert.match(source, /operationalSlo\.fromUtc/);
  assert.match(source, /operationalSlo\.toUtc/);
});


test('v0.8 W64 analytics requests defer history point caps to the server', () => {
  const apiSource = readFileSync(
    new URL('../src/shared/api.ts', import.meta.url),
    'utf8',
  );
  const viewSource = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(apiSource, /maxPoints\?: number/);
  assert.match(apiSource, /if \(maxPoints !== undefined\) params\.set\('maxPoints', String\(maxPoints\)\)/);
  assert.match(viewSource, /getConsumerOperationalTrend\(clusterId, groupId\)/);
  assert.doesNotMatch(viewSource, /getConsumerOperationalTrend\(clusterId, groupId, 100\)/);
  assert.doesNotMatch(viewSource, /target,\s*100,/);
});

test('v0.8 W64 SLO input changes invalidate in-flight evaluations', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /const sloEvaluationGeneration = useRef\(0\)/);
  assert.match(source, /const sloGeneration = \+\+sloEvaluationGeneration\.current/);
  assert.match(source, /sloGeneration !== sloEvaluationGeneration\.current/);
  assert.match(source, /\+\+sloEvaluationGeneration\.current; setSloThreshold/);
  assert.match(source, /\+\+sloEvaluationGeneration\.current; setSloTarget/);
});


test('v0.8 W64 SLO errors are isolated from live/trend analytics failures', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /const \[sloError, setSloError\] = useState<ReadViewProblem \| null>\(null\)/);
  assert.match(source, /setSloError\(readViewError\(reason\)\)/);
  assert.match(source, /\{sloError && <ReadViewProblemNotice problem=\{sloError\} \/>\}/);

  const thresholdHandler = source.match(/id="consumer-slo-threshold"[^\n]+onChange=\{event => \{([^}]+)\}\}/)?.[1] ?? '';
  const targetHandler = source.match(/id="consumer-slo-target"[^\n]+onChange=\{event => \{([^}]+)\}\}/)?.[1] ?? '';

  assert.match(thresholdHandler, /setSloError\(null\)/);
  assert.match(targetHandler, /setSloError\(null\)/);
  assert.doesNotMatch(thresholdHandler, /setOperationalError\(null\)/);
  assert.doesNotMatch(targetHandler, /setOperationalError\(null\)/);
});


test('v0.8 W64 SLO busy state is isolated from analytics loading', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /const \[activeSloRequests, setActiveSloRequests\] = useState\(0\)/);
  assert.match(source, /const sloBusy = activeSloRequests > 0/);
  assert.match(source, /setActiveSloRequests\(count => count \+ 1\)/);
  assert.match(source, /setActiveSloRequests\(count =>/);
  assert.match(source, /disabled=\{operationalBusy \|\| sloBusy\}/);
  assert.match(source, /\{sloBusy \? 'Evaluating…' : 'Evaluate bounded SLO'\}/);

  const thresholdHandler = source.match(/id="consumer-slo-threshold"[^\n]+onChange=\{event => \{([^}]+)\}\}/)?.[1] ?? '';
  const targetHandler = source.match(/id="consumer-slo-target"[^\n]+onChange=\{event => \{([^}]+)\}\}/)?.[1] ?? '';

  assert.doesNotMatch(thresholdHandler, /setActiveSloRequests/);
  assert.doesNotMatch(targetHandler, /setActiveSloRequests/);
  assert.doesNotMatch(thresholdHandler, /setOperationalBusy\(false\)/);
  assert.doesNotMatch(targetHandler, /setOperationalBusy\(false\)/);
});


test('v0.8 W64 stale SLO invalidation cannot re-enable evaluation while requests remain active', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  assert.match(source, /setActiveSloRequests\(count => count \+ 1\)/);
  assert.match(source, /Math\.max\(0, count - 1\)/);
  assert.match(source, /const sloBusy = activeSloRequests > 0/);

  const start = source.indexOf('const evaluateConsumerSlo = async');
  const end = source.indexOf('const openSubject = async', start);
  const evaluation = source.slice(start, end);

  assert.match(evaluation, /sloGeneration !== sloEvaluationGeneration\.current/);
  assert.match(evaluation, /finally \{\s*setActiveSloRequests/);
});


test('v0.8 W64 consumer and cluster switches preserve physical SLO request counts', () => {
  const source = readFileSync(
    new URL('../src/features/readviews/ReadViewsExplorer.tsx', import.meta.url),
    'utf8',
  );

  const effectStart = source.indexOf('useEffect(() => {');
  const effectEnd = source.indexOf('const runKsqlQuery = async', effectStart);
  const clusterReset = source.slice(effectStart, effectEnd);

  const consumerStart = source.indexOf('const openConsumer = async');
  const consumerEnd = source.indexOf('const evaluateConsumerSlo = async', consumerStart);
  const consumerSwitch = source.slice(consumerStart, consumerEnd);

  assert.match(clusterReset, /\+\+sloEvaluationGeneration\.current/);
  assert.match(consumerSwitch, /\+\+sloEvaluationGeneration\.current/);
  assert.doesNotMatch(clusterReset, /setActiveSloRequests\(0\)/);
  assert.doesNotMatch(consumerSwitch, /setActiveSloRequests\(0\)/);
});
