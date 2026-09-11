import typia from 'typia';
import { formatValidationErrors } from '../lib/json.js';
import type { StageConfigFields } from './stage.js';

/**
 * A Wrangler configuration with no `env` blocks: the shape build tools emit
 * once they have resolved an environment (the Cloudflare Vite and Astro
 * adapters write one under the build output). Unknown keys are carried
 * through verbatim so a deploy never loses a binding this module does not
 * model.
 */
export interface FlatWranglerConfig extends StageConfigFields {
  env?: unknown;
}

const parseFlatWranglerConfigText = typia.json.createValidateParse<FlatWranglerConfig>();

/** Reads a build-generated Wrangler JSON, refusing anything that is not already resolved for one stage. */
export function parseFlatWranglerConfig(json: string): FlatWranglerConfig {
  const result = parseFlatWranglerConfigText(json);
  if (!result.success) {
    throw new Error(`Flat Wrangler configuration is malformed at ${formatValidationErrors(result.errors)}.`);
  }
  if (result.data.env !== undefined) {
    throw new Error('A flat Wrangler configuration must not declare env blocks; it is already resolved for one stage.');
  }
  return result.data;
}
