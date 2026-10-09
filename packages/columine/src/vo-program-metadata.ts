import type { VoProgramMetadata } from './types.js';

const METADATA_ENCODER = new TextEncoder();

/**
 * Encode the program wrapper around the existing canonical schema-metadata
 * tree. VO frames and definition evidence stay opaque hexadecimal bytes;
 * this function neither encodes values nor verifies schema commitments.
 */
export function encodeVoProgramMetadata(metadata: VoProgramMetadata): Uint8Array {
  return METADATA_ENCODER.encode(
    JSON.stringify({
      fields: metadata.fields.map((link) => ({
        slot: link.slot,
        field_idx: link.fieldIdx,
        schema: {
          root: link.schema.root,
          frame: link.schema.frame,
          rows: link.schema.rows,
          profile: link.schema.profile,
          retained: link.schema.retained,
        },
      })),
      counts: metadata.counts.map((role) => ({
        instruction_offset: role.instructionOffset,
        count_field: role.countField,
        histogram_fields: role.histogramFields,
        mask_fields: role.maskFields,
      })),
    }),
  );
}

/**
 * Refusal of value-object admission metadata by a backend that links no native
 * value-object capability. Typed so a caller can tell an absent capability from
 * an invalid program or invalid metadata.
 */
export class VoProgramAdmissionUnavailableError extends Error {
  readonly code = 'VO_ADMISSION_UNAVAILABLE';

  constructor() {
    super('value-object program admission needs a native capability this backend does not link');
    this.name = 'VoProgramAdmissionUnavailableError';
  }
}
