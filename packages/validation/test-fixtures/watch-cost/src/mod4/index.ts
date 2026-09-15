import typia from 'typia';

export interface Payload4 {
  id: string;
  size: number;
}

export const isPayload4 = typia.createIs<Payload4>();
