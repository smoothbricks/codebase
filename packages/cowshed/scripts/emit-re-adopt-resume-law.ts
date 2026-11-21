// The re-adoption resume law's Rust model: bun packages/cowshed/scripts/emit-re-adopt-resume-law.ts [--check]
import { emitLaw } from './emit-law.ts';

await emitLaw({
  label: 're-adoption resume law',
  modelName: 'specs/cowshed/laws/re_adopt_resume.bend',
  kernel: new URL('../crates/cowshed-core/src/storage/recovery/resume/laws/model.rs', import.meta.url),
  functions: ['empty_step', 'rep_step', 'publication', 'missing_choice', 'copy_step'],
  laws: [
    'empty_never_over_a_foreign_image',
    'replacement_never_over_a_foreign_image',
    'empty_no_record_names_nothing',
    'replacement_no_record_names_nothing',
    'empty_replay_converges',
    'replacement_replay_converges',
    'published_replacement_is_kept',
    'a_retained_build_needs_its_claim',
    'a_missing_main_holds_no_claim',
    'a_missing_main_never_publishes_over_a_volume',
  ],
  cap: 400,
  properties: 200,
  propertyLength: 12,
  regenerate: 'bun packages/cowshed/scripts/emit-re-adopt-resume-law.ts',
});
