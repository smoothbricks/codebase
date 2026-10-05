import { expect, it } from 'bun:test';
import { TRACE_DB_DIRECTORY, TRACE_DB_FILENAME } from '@smoothbricks/lmao/sqlite';
import { TRACE_DB_ARTIFACT_GLOB } from './ci-workflow.js';

it('uploads the trace sink from where lmao writes it, WAL sidecars included', () => {
  expect(TRACE_DB_ARTIFACT_GLOB).toBe(`packages/*/${TRACE_DB_DIRECTORY}/${TRACE_DB_FILENAME}*`);
});
