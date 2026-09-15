/**
 * The emit-excluded entry point again, but importing a module from every one of
 * this fixture's eight source directories, because ttsc opens one directory
 * watcher per project directory to prove a generation reusable.
 *
 * Nine discriminating validators — one per imported module plus this file's own
 * — are the proof the transform still reached all of them.
 */
import typia from 'typia';
import { isPayload0 } from '../src/mod0/index.ts';
import { isPayload1 } from '../src/mod1/index.ts';
import { isPayload2 } from '../src/mod2/index.ts';
import { isPayload3 } from '../src/mod3/index.ts';
import { isPayload4 } from '../src/mod4/index.ts';
import { isPayload5 } from '../src/mod5/index.ts';
import { isPayload6 } from '../src/mod6/index.ts';
import { isPayload7 } from '../src/mod7/index.ts';

interface DeployPlan {
  stage: string;
}

const isDeployPlan = typia.createIs<DeployPlan>();

const checks = [
  isPayload0({ id: 'a', size: 0 }),
  isPayload1({ id: 'a', size: 1 }),
  isPayload2({ id: 'a', size: 2 }),
  isPayload3({ id: 'a', size: 3 }),
  isPayload4({ id: 'a', size: 4 }),
  isPayload5({ id: 'a', size: 5 }),
  isPayload6({ id: 'a', size: 6 }),
  isPayload7({ id: 'a', size: 7 }),
  isDeployPlan({ stage: 'e2e' }),
];

console.log(JSON.stringify({ all: checks.every(Boolean), count: checks.length }));
