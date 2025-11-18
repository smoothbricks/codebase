import { defineTagAttributes } from '../defineTagAttributes.js';
import { describe, it, expect } from 'bun:test';
import { S } from '../builder.js';

 describe('defineTagAttributes', () => {
   it('defines base attributes', () => {
     const base = defineTagAttributes({
       requestId: S.string(),
       userId: S.optional(S.string().with('hash')),
       timestamp: S.number(),
     });
     
     expect(base).toBeDefined();
   });
   
   it('supports schema extension', () => {
     const base = defineTagAttributes({
       requestId: S.string()
     });

     const extended = base.extend({ duration: S.number() });
     expect(extended).toHaveProperty('requestId');
     expect(extended).toHaveProperty('duration');
   });
   
   it('validates union types', () => {
     const schema = defineTagAttributes({
       operation: S.union(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
     });
     
     expect(schema).toBeDefined();
   });
 });
