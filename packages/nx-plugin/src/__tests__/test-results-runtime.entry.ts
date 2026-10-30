import assert from 'node:assert/strict';
import { join } from 'node:path';
import { type ClosedTestResult, isClosedTestResult, TEST_RESULTS_FILE } from '@smoothbricks/nx-plugin/test-results';
import { taskDirectory } from '@smoothbricks/nx-plugin/test-results/host';

const record = {
  runId: '00000000-0000-4000-8000-000000000000',
  id: 'bundle::closed',
  status: 'pass',
  source: 'native',
  durationMs: null,
  sourceRef: null,
  causeRef: null,
  cursor: 0,
} satisfies ClosedTestResult;

assert.ok(isClosedTestResult(record), 'the precompiled public guard must admit the valid closed record');
assert.equal(isClosedTestResult({ ...record, unexpected: true }), false, 'the public guard must remain strict');
assert.equal(TEST_RESULTS_FILE, 'test-results.jsonl');
assert.equal(taskDirectory('/owned-data', 'bundle:test'), join('/owned-data', 'bounded-exec', 'bundle%3Atest'));
console.log('public test-result production bundle: compiled guard and host entry passed');
