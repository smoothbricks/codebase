import typia from 'typia';

export interface Payload1 {
  id: string;
  size: number;
}

export const isPayload1 = typia.createIs<Payload1>();
