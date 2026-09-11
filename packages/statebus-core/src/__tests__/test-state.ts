import type { ByID } from '../index.js';

declare module '@smoothbricks/statebus-core' {
  interface States {
    records: ByID<string>;
  }
}

export function initialTestState() {
  return { counter: 0, counter1: 0, counter2: 0, records: () => undefined };
}
