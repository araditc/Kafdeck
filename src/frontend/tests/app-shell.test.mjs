import assert from 'node:assert/strict';
import test from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { AppShell } from '../dist/test-source/app/AppShell.js';

test('AppShell exposes the product identity through semantic markup', () => {
  const markup = renderToStaticMarkup(React.createElement(AppShell));

  assert.match(markup, /<main[^>]+aria-labelledby="kafdeck-title"/);
  assert.match(markup, /<h1 id="kafdeck-title">Kafdeck<\/h1>/);
  assert.match(markup, /Open Source Kafka Control Plane/);
});
