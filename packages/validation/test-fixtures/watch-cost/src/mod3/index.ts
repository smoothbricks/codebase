import typia from 'typia';

export interface Payload3 {
  id: string;
  size: number;
}

export const isPayload3 = typia.createIs<Payload3>();
