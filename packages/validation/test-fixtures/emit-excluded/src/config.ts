import typia from 'typia';

/** A validator the emit program owns, imported by the excluded entry point. */
export interface DeployConfig {
  target: string;
  retries: number;
}

export const isDeployConfig = typia.createIs<DeployConfig>();
