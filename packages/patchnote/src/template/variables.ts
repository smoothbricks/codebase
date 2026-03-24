/**
 * Build template variables from update data for PR body rendering.
 */

import {
  analyzeChangelogs,
  formatDowngradesSection,
  formatNixUpdatesSection,
  generateUpdateTable,
  renderReleaseNotesSection,
} from '../changelog/analyzer.js';
import type { PatchnoteConfig } from '../config.js';
import { formatDeprecationWarnings } from '../deprecated/formatter.js';
import { formatProvenanceWarnings } from '../provenance/formatter.js';
import type { PackageUpdate } from '../types.js';

export interface TemplateVariables {
  [key: string]: string | number | undefined;
  header: string;
  table: string;
  aiSummary: string;
  releaseNotes: string;
  nixUpdates: string;
  warnings: string;
  provenanceWarnings: string;
  deprecationWarnings: string;
  downgrades: string;
  updateCount: number;
  majorCount: number;
  minorCount: number;
  patchCount: number;
}

/**
 * Build all template variables from update data.
 * Each section is computed independently so the template controls layout.
 */
export async function buildTemplateVariables(opts: {
  updates: PackageUpdate[];
  downgrades: PackageUpdate[];
  changelogs: Map<string, string>;
  config: PatchnoteConfig;
  skipAI: boolean;
  commitTitle: string;
}): Promise<TemplateVariables> {
  const { updates, downgrades, changelogs, config, skipAI, commitTitle } = opts;

  // Get AI summary (raw mode -- no embedded sections)
  let aiSummary = '';
  if (!skipAI && changelogs.size > 0) {
    aiSummary = await analyzeChangelogs(updates, changelogs, config, downgrades, true);
  }

  // Get structured table (no downgrades/release notes embedded)
  // When AI is active and returns content, table is empty (aiSummary replaces it)
  const table = skipAI || changelogs.size === 0 ? generateUpdateTable(updates) : '';

  const provenanceWarnings = formatProvenanceWarnings(updates);
  const deprecationWarnings = formatDeprecationWarnings(updates);
  const nixUpdates = formatNixUpdatesSection(updates);
  const downgradesSection = formatDowngradesSection(downgrades);
  const currentLength = (aiSummary || table).length;
  const releaseNotes = renderReleaseNotesSection(updates, changelogs, 60000 - currentLength);

  return {
    header: commitTitle,
    table,
    aiSummary,
    releaseNotes,
    nixUpdates,
    warnings: [provenanceWarnings, deprecationWarnings].filter(Boolean).join('\n\n'),
    provenanceWarnings,
    deprecationWarnings,
    downgrades: downgradesSection,
    updateCount: updates.length,
    majorCount: updates.filter((u) => u.updateType === 'major').length,
    minorCount: updates.filter((u) => u.updateType === 'minor').length,
    patchCount: updates.filter((u) => u.updateType === 'patch').length,
  };
}
