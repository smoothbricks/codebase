/**
 * An entry point the emit program excludes, shaped like a deploy script: it
 * imports a validator the emit program owns and declares one of its own.
 *
 * Both halves matter. An untransformed `typia.createIs` throws at the callsite
 * instead of validating, so printing four discriminated results is proof the
 * transform reached this file and the module it imports.
 */
import typia from 'typia';
import { isDeployConfig } from '../src/public.ts';

interface DeployPlan {
  stage: string;
  dryRun: boolean;
}

const isDeployPlan = typia.createIs<DeployPlan>();

console.log(
  JSON.stringify({
    imported: {
      good: isDeployConfig({ target: 'prod', retries: 2 }),
      bad: isDeployConfig({ target: 'prod', retries: 'two' }),
    },
    own: {
      good: isDeployPlan({ stage: 'e2e', dryRun: false }),
      bad: isDeployPlan({ stage: 'e2e', dryRun: 'no' }),
    },
  }),
);
