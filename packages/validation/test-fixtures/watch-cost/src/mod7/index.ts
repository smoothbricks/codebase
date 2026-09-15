import typia from 'typia';

export interface Payload7 {
  id: string;
  size: number;
}

export const isPayload7 = typia.createIs<Payload7>();
