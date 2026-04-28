import type { AfterAllProjectsVersioned, VersionActions } from 'nx/release';

declare const versionActions: typeof VersionActions & {
  afterAllProjectsVersioned: AfterAllProjectsVersioned;
};

export = versionActions;
