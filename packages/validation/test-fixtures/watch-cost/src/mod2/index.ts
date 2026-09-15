import typia from 'typia';

export interface Payload2 {
  id: string;
  size: number;
}

export const isPayload2 = typia.createIs<Payload2>();
