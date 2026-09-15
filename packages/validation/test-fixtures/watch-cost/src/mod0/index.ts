import typia from 'typia';

export interface Payload0 {
  id: string;
  size: number;
}

export const isPayload0 = typia.createIs<Payload0>();
