import assert from 'node:assert/strict';
import test from 'node:test';
import {ApiProblem} from '../dist/test-source/shared/api.js';
import {setLocale, translate} from '../dist/test-source/shared/i18n.js';
import {messages} from '../dist/test-source/shared/locales/messages.js';
import {readViewError, readViewFailure, readViewProblemText, ReadViewInputError, readViewProblemMessages} from '../dist/test-source/features/readviews/readViewProblem.js';
import {applicationProblem, applicationProblemCopies, problemPresentation, markApplicationError, ownedErrorPresentation} from '../dist/test-source/shared/presentationProblem.js';
import {boundedScope, governanceProblem} from '../dist/test-source/features/governance/governanceApi.js';
import {MutationApiProblem} from '../dist/test-source/features/mutations/mutationApi.js';

test('original local validation and governance status errors re-render after a language switch',()=>{
  try {
    setLocale('en',false);
    let validation;
    try {boundedScope('../invalid');} catch(reason) {validation=reason;}
    assert.ok(validation instanceof Error);
    const denied=new MutationApiProblem(403,'provider-private-prose','ScopeDenied');
    const fallback=markApplicationError(new ApiProblem(500,'Kafdeck request failed.',null),'Kafdeck request failed.');
    const cachedFallback=readViewError(fallback);
    for(const locale of ['en','fa','en']){
      setLocale(locale,false);
      assert.equal(governanceProblem(validation),translate(locale,'Use the exact authorized cluster identifier.'));
      assert.equal(governanceProblem(denied),translate(locale,'The current operator is not authorized for this scope or action.'));
      assert.doesNotMatch(governanceProblem(denied),/provider-private-prose/);
      assert.equal(readViewProblemText(cachedFallback),translate(locale,'Kafdeck request failed.'));
    }
    // A provider cannot acquire ownership by supplying a matching key or property.
    const raw=Object.assign(new SyntaxError('Use the exact authorized cluster identifier.'),{copy:'Use the exact authorized cluster identifier.'});
    assert.equal(ownedErrorPresentation(raw),null);
    setLocale('fa',false);
    assert.equal(readViewProblemText(readViewError(raw)),raw.message);
    const external=new Error('Policy ID is required.');
    assert.equal(governanceProblem(external),external.message);
  } finally {setLocale('en',false);}
});

test('cached application errors change locale while external prose with identical spelling remains raw',()=>{
  try {
    setLocale('en',false);
    const cached=applicationProblemCopies.map(copy=>({owned:applicationProblem(copy),external:{raw:copy},copy}));
    for(const locale of ['en','fa','en']) {
      setLocale(locale,false);
      for(const {owned,external,copy} of cached){
        assert.ok(Object.hasOwn(messages,copy),copy);
        assert.equal(problemPresentation(owned),translate(locale,copy));
        assert.equal(problemPresentation(external),copy);
      }
      for(const raw of ["Unexpected token '<', not valid JSON",'urn:provider:403 012.5000 <exact>'])assert.equal(problemPresentation({raw}),raw);
    }
  } finally {setLocale('en',false);}
});

test('cached read-view application problems follow locale without replacing backend messages',()=>{
  try {
    setLocale('en',false);
    const cases=[[401,'denied'],[403,'denied'],[404,'unconfigured'],[501,'unsupported'],[502,'unavailable'],[503,'unavailable'],[504,'unavailable']];
    const cached=cases.map(([status,copy])=>({problem:readViewError(new ApiProblem(status,'external prose',null)),copy}));
    for(const [key,text] of Object.entries(readViewProblemMessages))assert.ok(Object.hasOwn(messages,text),key);
    cached.push({problem:readViewError(null),copy:'failed'});
    cached.push({problem:readViewFailure(null,'autoRestartPreview'),copy:'autoRestartPreview'});
    for(const locale of ['en','fa','en']){
      setLocale(locale,false);
      for(const {problem,copy} of cached){assert.equal(problem.copy,copy);assert.equal(readViewProblemText(problem),translate(locale,readViewProblemMessages[copy]));}
    }
    const raw=['Not authorized for this read view.',"Unexpected token '<', not valid JSON",'provider-012.5000'];
    for(const reason of [...raw.map(message=>new Error(message)),new ApiProblem(409,raw[2],null)]){
      const problem=readViewError(reason);
      for(const locale of ['fa','en']){setLocale(locale,false);assert.equal(readViewProblemText(problem),reason.message);}
    }
    // Auto-restart historically exposes Error prose verbatim, including HTTP errors.
    const external=new ApiProblem(403,raw[2],null);
    assert.equal(readViewProblemText(readViewFailure(external,'autoRestartApply')),external.message);
  } finally {setLocale('en',false);}
});

test('cached local configuration errors keep exact interpolation while changing language',()=>{
  try {
    setLocale('en',false);
    const value='<script>012.5000</script>';
    const problem=readViewError(new ReadViewInputError('configurationDuplicate',[value]));
    assert.equal(readViewProblemText(problem),'Duplicate configuration key: '+value);
    setLocale('fa',false);
    assert.equal(readViewProblemText(problem),translate('fa','Duplicate configuration key: {0}',[value]));
    assert.ok(readViewProblemText(problem).includes('\u2068'+value+'\u2069'));
  } finally {setLocale('en',false);}
});
