import typia from 'typia';

export interface Payload6 {
  id: string;
  size: number;
}

export const isPayload6 = typia.createIs<Payload6>();
