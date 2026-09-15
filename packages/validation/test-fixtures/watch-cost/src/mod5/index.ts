import typia from 'typia';

export interface Payload5 {
  id: string;
  size: number;
}

export const isPayload5 = typia.createIs<Payload5>();
